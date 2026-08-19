package network

import (
	"net"
	"testing"
)

func TestGetLocalIPs(t *testing.T) {
	ips := GetLocalIPs()
	// In most test environments, at least one IP or empty slice is returned
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			t.Errorf("invalid IP string returned: %s", ipStr)
		}
		if ip.IsLoopback() {
			t.Errorf("loopback IP returned in GetLocalIPs: %s", ipStr)
		}
	}
}

func TestGetPreferredOutboundIP(t *testing.T) {
	ipStr := GetPreferredOutboundIP()
	if ipStr == "" {
		t.Errorf("GetPreferredOutboundIP returned empty string")
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		t.Errorf("invalid IP returned: %s", ipStr)
	}
}
