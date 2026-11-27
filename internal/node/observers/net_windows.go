//go:build windows

package observers

import (
	"bytes"
	"context"
	"log"
	"strconv"
	"syscall"
	"unsafe"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

var (
	modIphlpapi      = syscall.NewLazyDLL("iphlpapi.dll")
	procGetIfTable2  = modIphlpapi.NewProc("GetIfTable2")
	procFreeMibTable = modIphlpapi.NewProc("FreeMibTable")
)

const (
	mibIfTypeLoopback      = winMibIfTypeLoopback
	ifOperStatusUp         = winIfOperStatusUp
	ifMaxStringSize        = 256
	ifMaxPhysAddressLength = 32
)

type winGUID struct {
	Data1 uint32
	Data2 uint16
	Data3 uint16
	Data4 [8]byte
}

// mibIfRow2 matches Windows netioapi.h MIB_IF_ROW2 (64-bit byte/packet counters).
type mibIfRow2 struct {
	InterfaceLuid               uint64
	InterfaceIndex              uint32
	InterfaceGuid               winGUID
	Alias                       [ifMaxStringSize + 1]uint16
	Description                 [ifMaxStringSize + 1]uint16
	PhysicalAddressLength       uint32
	PhysicalAddress             [ifMaxPhysAddressLength]byte
	PermanentPhysicalAddress    [ifMaxPhysAddressLength]byte
	Mtu                         uint32
	Type                        uint32
	TunnelType                  uint32
	MediaType                   uint32
	PhysicalMediumType          uint32
	AccessType                  uint32
	DirectionType               uint32
	InterfaceAndOperStatusFlags byte
	_                           [3]byte
	OperStatus                  uint32
	AdminStatus                 uint32
	MediaConnectState           uint32
	NetworkGuid                 winGUID
	ConnectionType              uint32
	_                           uint32
	TransmitLinkSpeed           uint64
	ReceiveLinkSpeed            uint64
	InOctets                    uint64
	InUcastPkts                 uint64
	InNUcastPkts                uint64
	InDiscards                  uint64
	InErrors                    uint64
	InUnknownProtos             uint64
	InUcastOctets               uint64
	InMulticastOctets           uint64
	InBroadcastOctets           uint64
	OutOctets                   uint64
	OutUcastPkts                uint64
	OutNUcastPkts               uint64
	OutDiscards                 uint64
	OutErrors                   uint64
	OutUcastOctets              uint64
	OutMulticastOctets          uint64
	OutBroadcastOctets          uint64
	OutQLen                     uint64
}

type mibIfTable2 struct {
	NumEntries uint32
	_          uint32
	Table      [1]mibIfRow2
}

func observeNetPlatform(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	log.Println("[Net Observer] Windows networking detected (zero-fork Win32 GetIfTable2 64-bit observer).")
	observeNetWindows(ctx, pub, nodeID)
}

func emitWindowsNetSamples(pub core.TelemetryPublisher, nodeID string) {
	var table *mibIfTable2
	r, _, _ := procGetIfTable2.Call(uintptr(unsafe.Pointer(&table)))
	if r != 0 || table == nil {
		return
	}

	numEntries := int(table.NumEntries)
	if numEntries <= 0 || numEntries > 4096 {
		procFreeMibTable.Call(uintptr(unsafe.Pointer(table)))
		return
	}

	// Copy into a Go-allocated, properly aligned []mibIfRow2 slice before freeing the Win32 table.
	rows := make([]mibIfRow2, numEntries)
	copy(rows, unsafe.Slice(&table.Table[0], numEntries))
	procFreeMibTable.Call(uintptr(unsafe.Pointer(table)))

	for i := range rows {
		row := &rows[i]
		if row.InterfaceAndOperStatusFlags&0x02 != 0 {
			continue // Skip NDIS lightweight filter rows (FilterInterface bit)
		}
		if !isEligibleWindowsNetRow(row.Type, row.OperStatus, row.InterfaceAndOperStatusFlags, row.InOctets, row.OutOctets) {
			continue
		}

		comp := "net-eth" + strconv.FormatUint(uint64(row.InterfaceIndex), 10)
		ev := core.NewCTDPayload(nodeID, comp, "active").
			AddMetric("rx_bytes", float64(row.InOctets)).
			AddMetric("tx_bytes", float64(row.OutOctets)).
			AddMetric("rx_packets", float64(row.InUcastPkts+row.InNUcastPkts)).
			AddMetric("tx_packets", float64(row.OutUcastPkts+row.OutNUcastPkts))

		desc := string(bytes.TrimRight([]byte(syscall.UTF16ToString(row.Description[:])), "\x00"))
		if desc != "" {
			ev.AddParam("description", desc)
		}
		if err := pub.Publish(ev); err != nil {
			log.Printf("[Windows Net Observer] Publish failed for %s: %v", comp, err)
		}
	}
}

func observeNetWindows(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	runPeriodicObserver(ctx, true, func() {
		emitWindowsNetSamples(pub, nodeID)
	})
}
