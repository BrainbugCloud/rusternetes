//===----------------------------------------------------------------------===//
// Pod IPAM.
//
// A pod needs an address before anything can reach it: kubelet probes, CRI
// port-forward, and eventually Services all dial the pod, and `PodConfigWire.interfaces`
// was passed empty until now.
//
// The plumbing beneath is entirely Apple's — `NATInterface` is a value type that
// `VZVirtualMachineInstance` turns into a `VZVirtioNetworkDeviceConfiguration`
// with a `VZNATNetworkDeviceAttachment`. What was missing is only the choice of
// address, so this uses Apple's own `IPv4Address.allocator` rather than a
// hand-rolled one.
//
// ## The collision this cannot rule out
//
// `VZNATNetworkDeviceAttachment` puts guests on macOS's shared vmnet subnet,
// where `bootpd` also hands out addresses over DHCP. Our guests never ask —
// `vminitd` configures the address statically — so nothing we do can clash with
// itself. It *can* clash with another VM on the same subnet that DHCP gave the
// same address: an `apple/container` container, another VZ app, Docker Desktop.
//
// Hence `--pod-subnet`, and a default range high in the subnet where bootpd is
// least likely to have reached. This is a real risk, not a solved problem; the
// honest fix is a dedicated subnet, which needs a vmnet mode Virtualization.framework
// does not expose to us.
//===----------------------------------------------------------------------===//

import ContainerizationExtras
import Foundation

/// Allocates one address per pod from a configured subnet.
final class NetworkService: @unchecked Sendable {
    /// Used only when the vmnet bridge cannot be found. See ``discoverSubnet()``:
    /// guessing the subnet is how pods end up unreachable, so this is a last
    /// resort, not a default.
    static let fallbackSubnet = "192.168.64.0/24"
    /// `.1` is the gateway; bootpd hands out from the low end, so start high.
    static let defaultRangeStart: UInt32 = 200
    static let defaultRangeSize = 50

    /// The subnet macOS has actually put its vmnet bridge on.
    ///
    /// `VZNATNetworkDeviceAttachment` attaches guests to the shared vmnet
    /// network, and **macOS picks that subnet, not us** — 192.168.64.0/24 and
    /// 192.168.65.0/24 are both common, and it varies by machine. Hardcoding one
    /// is not a cosmetic bug: the guest comes up with a statically configured
    /// address on a network the host has no route to, so the container runs
    /// perfectly and nothing can reach it. That is exactly how the port-mapping
    /// and port-forward specs failed here — the host was on 192.168.65.0/24
    /// while pods were being handed 192.168.64.x, with no interface, no route
    /// and no ARP for them.
    ///
    /// The bridge is found from the live interface list rather than from
    /// `com.apple.vmnet.plist`, which is root-only. Candidates are `bridge*`
    /// interfaces that are up, running and carry an IPv4 address; `bridge0` is
    /// the Thunderbolt bridge and is filtered out by having neither.
    static func discoverSubnet() -> String? {
        var head: UnsafeMutablePointer<ifaddrs>?
        guard getifaddrs(&head) == 0, let head else { return nil }
        defer { freeifaddrs(head) }

        var candidates: [(name: String, subnet: String)] = []
        for entry in sequence(first: head, next: { $0.pointee.ifa_next }) {
            let ifa = entry.pointee
            guard let raw = ifa.ifa_name else { continue }
            let name = String(cString: raw)
            guard name.hasPrefix("bridge") else { continue }
            let flags = Int32(ifa.ifa_flags)
            guard flags & IFF_UP != 0, flags & IFF_RUNNING != 0 else { continue }
            guard let addr = ifa.ifa_addr, addr.pointee.sa_family == sa_family_t(AF_INET),
                let mask = ifa.ifa_netmask
            else { continue }

            let ip = addr.withMemoryRebound(to: sockaddr_in.self, capacity: 1) {
                UInt32(bigEndian: $0.pointee.sin_addr.s_addr)
            }
            let netmask = mask.withMemoryRebound(to: sockaddr_in.self, capacity: 1) {
                UInt32(bigEndian: $0.pointee.sin_addr.s_addr)
            }
            guard netmask != 0 else { continue }
            let network = ip & netmask
            let prefix = netmask.nonzeroBitCount
            let octets = [network >> 24, (network >> 16) & 0xff, (network >> 8) & 0xff, network & 0xff]
            candidates.append((name, "\(octets.map(String.init).joined(separator: "."))/\(prefix)"))
        }
        // Lowest-numbered bridge wins, so the choice is stable across runs when a
        // machine has more than one VM network up.
        return candidates.sorted { $0.name < $1.name }.first?.subnet
    }

    private let prefixLength: UInt8
    private let gateway: String
    private let allocator: any AddressAllocator<IPv4Address>
    private let lock = NSLock()
    private var assigned: [String: IPv4Address] = [:]

    /// - Parameters:
    ///   - subnet: CIDR the pods live on, e.g. `192.168.64.0/24`.
    ///   - rangeStart: host number the allocatable range begins at, so the low
    ///     part of the subnet can be left to whatever else shares it.
    ///   - rangeSize: how many addresses to hand out.
    init(subnet: String, rangeStart: UInt32, rangeSize: Int) throws {
        let cidr = try CIDRv4(subnet)
        self.prefixLength = cidr.prefix.length
        self.gateway = cidr.gateway.description

        let lower = cidr.lower.value + rangeStart
        guard lower >= cidr.lower.value, lower < cidr.upper.value else {
            throw BrokerError.badRequest(
                "pod range start \(rangeStart) is outside \(subnet)")
        }
        // Never hand out the broadcast address.
        let available = Int(cidr.upper.value - lower)
        guard available > 0 else {
            throw BrokerError.badRequest("no addresses left in \(subnet) above \(rangeStart)")
        }
        self.allocator = try IPv4Address.allocator(lower: lower, size: min(rangeSize, available))
    }

    /// Allocate this pod's interface. Idempotent: asking twice for the same pod
    /// returns what it already has rather than burning a second address.
    func allocate(podID: String) throws -> InterfaceWire {
        // Check and insert under one lock: connections are served concurrently,
        // so splitting them would let two creates of the same pod both miss and
        // burn two addresses, leaking one for the process lifetime.
        let address = try lock.withLock { () throws -> IPv4Address in
            if let existing = assigned[podID] { return existing }
            let fresh = try allocator.allocate()
            assigned[podID] = fresh
            return fresh
        }
        return interface(address)
    }

    /// Return a pod's address to the pool. Safe for a pod that never had one.
    func release(podID: String) {
        guard let address = lock.withLock({ assigned.removeValue(forKey: podID) }) else { return }
        try? allocator.release(address)
    }

    /// The address currently assigned to `podID`, if any.
    func address(podID: String) -> String? {
        lock.withLock { assigned[podID] }?.description
    }

    private func interface(_ address: IPv4Address) -> InterfaceWire {
        InterfaceWire(
            address: "\(address.description)/\(prefixLength)",
            gateway: gateway,
            mtu: nil,
            macAddress: nil
        )
    }
}
