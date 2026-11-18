//go:build windows

package observers

import (
	"context"
	"log"
	"strconv"
	"syscall"
	"time"
	"unsafe"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

var (
	modIphlpapi    = syscall.NewLazyDLL("iphlpapi.dll")
	procGetIfTable = modIphlpapi.NewProc("GetIfTable")
)

const (
	mibIfTypeLoopback = 24
	ifOperStatusUp    = 5
	ifOperStatusConn  = 4
	errorInsuffBuffer = 122
	maxInterfaceName  = 256
	maxPhysAddressLen = 8
	maxDescrLen       = 256
)

type mibIfRow struct {
	Name            [maxInterfaceName]uint16
	Index           uint32
	Type            uint32
	Mtu             uint32
	Speed           uint32
	PhysAddrLen     uint32
	PhysAddr        [maxPhysAddressLen]byte
	AdminStatus     uint32
	OperStatus      uint32
	LastChange      uint32
	InOctets        uint32
	InUcastPkts     uint32
	InNUcastPkts    uint32
	InDiscards      uint32
	InErrors        uint32
	InUnknownProtos uint32
	OutOctets       uint32
	OutUcastPkts    uint32
	OutNUcastPkts   uint32
	OutDiscards     uint32
	OutErrors       uint32
	OutQLen         uint32
	DescrLen        uint32
	Descr           [maxDescrLen]byte
}

func observeNetPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Println("[Net Observer] Windows networking detected (zero-fork Win32 GetIfTable observer).")
	go observeNetWindows(ctx, pub, nodeID)
}

func emitWindowsNetSamples(pub core.TelemetryPublisher, nodeID string) {
	var size uint32
	r, _, _ := procGetIfTable.Call(0, uintptr(unsafe.Pointer(&size)), 0)
	if r != errorInsuffBuffer || size == 0 || size > 4*1024*1024 {
		return
	}

	buf := make([]byte, size)
	r, _, _ = procGetIfTable.Call(uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(&size)), 0)
	if r != 0 || len(buf) < 4 {
		return
	}

	numEntries := *(*uint32)(unsafe.Pointer(&buf[0]))
	rowSize := uintptr(unsafe.Sizeof(mibIfRow{}))
	baseOffset := unsafe.Sizeof(uint32(0))

	for i := range numEntries {
		offset := baseOffset + uintptr(i)*rowSize
		if offset+rowSize > uintptr(len(buf)) {
			break
		}
		row := (*mibIfRow)(unsafe.Pointer(&buf[offset]))
		if row.Type == mibIfTypeLoopback {
			continue
		}
		if row.OperStatus != ifOperStatusUp && row.OperStatus != ifOperStatusConn {
			continue
		}
		if row.InOctets == 0 && row.OutOctets == 0 {
			continue
		}

		comp := "net-eth" + strconv.FormatUint(uint64(row.Index), 10)
		ev := core.NewCTDPayload(nodeID, comp, "active").
			AddMetric("rx_bytes", float64(row.InOctets)).
			AddMetric("tx_bytes", float64(row.OutOctets))
		if row.DescrLen > 0 && int(row.DescrLen) <= len(row.Descr) {
			ev.AddParam("description", string(row.Descr[:row.DescrLen]))
		}
		if err := pub.Publish(ev); err != nil {
			log.Printf("[Windows Net Observer] Publish failed for %s: %v", comp, err)
		}
	}
}

func observeNetWindows(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	emitWindowsNetSamples(pub, nodeID)

	ticker := time.NewTicker(pollInterval(ctx))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			emitWindowsNetSamples(pub, nodeID)
		}
	}
}
