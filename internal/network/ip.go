package network

import (
	"net"
	"strings"
)

// GetLocalIPs returns all non-loopback IPv4 addresses found on local network interfaces
func GetLocalIPs() []string {
	var ips []string
	interfaces, err := net.Interfaces()
	if err != nil {
		return ips
	}

	for _, iface := range interfaces {
		// Ignore down or loopback interfaces
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}

			// We prefer IPv4 for LAN QR codes
			ip4 := ip.To4()
			if ip4 != nil {
				// Filter out link-local (169.254.x.x)
				if !ip4.IsLinkLocalUnicast() {
					ips = append(ips, ip4.String())
				}
			}
		}
	}
	return ips
}

// GetPreferredOutboundIP tries to determine the primary outbound local IP
// by establishing a dummy UDP connection (no actual traffic sent)
func GetPreferredOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err == nil {
		defer conn.Close()
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		if localAddr.IP != nil && !localAddr.IP.IsLoopback() {
			return localAddr.IP.String()
		}
	}

	// Fallback to searching interfaces
	ips := GetLocalIPs()
	for _, ip := range ips {
		// Prefer 192.168.x.x or 10.x.x.x or 172.x.x.x
		if strings.HasPrefix(ip, "192.168.") || strings.HasPrefix(ip, "10.") || strings.HasPrefix(ip, "172.") {
			return ip
		}
	}

	if len(ips) > 0 {
		return ips[0]
	}

	return "127.0.0.1"
}
