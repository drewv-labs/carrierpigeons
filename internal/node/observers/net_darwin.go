//go:build darwin

package observers

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"net"
	"strings"
	"syscall"
	"unsafe"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
	"golang.org/x/net/route"
	"golang.org/x/sys/unix"
)

func observeNetPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Println("[Net Observer] macOS networking detected (zero-fork route RIB observer).")
	observeNetDarwin(ctx, pub, nodeID)
}

type darwinNetCounters struct {
	rxBytes   uint64
	txBytes   uint64
	rxPackets uint64
	txPackets uint64
}

// collectDarwinNetCounters fetches 64-bit interface byte/packet counters directly from the kernel
// routing information base (NET_RT_IFLIST2) without spawning `netstat`.
func collectDarwinNetCounters() (map[string]darwinNetCounters, error) {
	rib, err := route.FetchRIB(syscall.AF_UNSPEC, route.RIBType(syscall.NET_RT_IFLIST2), 0)
	if err != nil {
		return nil, err
	}

	hdrSize := int(unsafe.Sizeof(unix.IfMsghdr2{}))
	seen := make(map[string]darwinNetCounters)

	for offset := 0; offset+4 <= len(rib); {
		msgLen := int(binary.LittleEndian.Uint16(rib[offset : offset+2]))
		if msgLen <= 0 || offset+msgLen > len(rib) {
			break
		}
		msgType := rib[offset+3]
		if msgType == unix.RTM_IFINFO2 && msgLen >= hdrSize {
			var hdr unix.IfMsghdr2
			if err := binary.Read(bytes.NewReader(rib[offset:offset+hdrSize]), binary.LittleEndian, &hdr); err == nil {
				iface := parseDarwinSockaddrDLName(rib[offset+hdrSize : offset+msgLen])
				if iface == "" && hdr.Index > 0 {
					if netIf, ifErr := net.InterfaceByIndex(int(hdr.Index)); ifErr == nil {
						iface = netIf.Name
					}
				}
				if (strings.HasPrefix(iface, "en") || strings.HasPrefix(iface, "utun")) &&
					(hdr.Data.Ibytes > 0 || hdr.Data.Obytes > 0) {
					seen[iface] = darwinNetCounters{
						rxBytes:   hdr.Data.Ibytes,
						txBytes:   hdr.Data.Obytes,
						rxPackets: hdr.Data.Ipackets,
						txPackets: hdr.Data.Opackets,
					}
				}
			}
		}
		offset += msgLen
	}

	return seen, nil
}

// parseDarwinSockaddrDLName extracts the interface name from the sockaddr_dl following IfMsghdr2.
func parseDarwinSockaddrDLName(b []byte) string {
	// struct sockaddr_dl: sdl_len(0), sdl_family(1), sdl_index(2..3), sdl_type(4), sdl_nlen(5), sdl_alen(6), sdl_slen(7), sdl_data(8..)
	if len(b) < 8 || b[1] != syscall.AF_LINK {
		return ""
	}
	nlen := int(b[5])
	if nlen <= 0 || 8+nlen > len(b) {
		return ""
	}
	return strings.TrimRight(string(b[8:8+nlen]), "\x00")
}

func emitDarwinNetSamples(pub core.TelemetryPublisher, nodeID string) {
	seen, err := collectDarwinNetCounters()
	if err != nil {
		return
	}

	for iface, c := range seen {
		ev := core.NewCTDPayload(nodeID, "net-"+iface, "active").
			AddMetric("rx_bytes", float64(c.rxBytes)).
			AddMetric("tx_bytes", float64(c.txBytes)).
			AddMetric("rx_packets", float64(c.rxPackets)).
			AddMetric("tx_packets", float64(c.txPackets))
		if err := pub.Publish(ev); err != nil {
			log.Printf("[Net Observer] Publish failed for net-%s: %v", iface, err)
		}
	}
}

func observeNetDarwin(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	runPeriodicObserver(ctx, true, func() {
		emitDarwinNetSamples(pub, nodeID)
	})
}
