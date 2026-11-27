package observers

import (
	"context"
	"strings"

	"github.com/drewv-labs/carrierpigeons/pkg/core"
)

const (
	winMibIfTypeLoopback   uint32 = 24
	winIfOperStatusUp      uint32 = 1    // RFC 2863 IF_OPER_STATUS IfOperStatusUp in MIB_IF_ROW2
	winMibIfFlagFilterNode byte   = 0x02 // MIB_IF_ROW2.InterfaceAndOperStatusFlags FilterInterface bit
)

// observeNet dispatches network interface telemetry observation for the target platform.
func observeNet(ctx context.Context, pub core.TelemetryPublisher, nodeID string) {
	observeNetPlatform(ctx, pub, nodeID)
}

func parseProcNetDevLine(line string) (iface string, fields []string, ok bool) {
	colon := strings.LastIndexByte(line, ':')
	if colon < 0 {
		return "", nil, false
	}
	iface = strings.TrimSpace(line[:colon])
	if iface == "" {
		return "", nil, false
	}
	fields = strings.Fields(line[colon+1:])
	if len(fields) < 10 {
		return "", nil, false
	}
	return iface, fields, true
}

func isEligibleWindowsNetRow(ifType, operStatus uint32, interfaceAndOperStatusFlags byte, inOctets, outOctets uint64) bool {
	if interfaceAndOperStatusFlags&winMibIfFlagFilterNode != 0 {
		return false
	}
	if ifType == winMibIfTypeLoopback {
		return false
	}
	if operStatus != winIfOperStatusUp {
		return false
	}
	if inOctets == 0 && outOctets == 0 {
		return false
	}
	return true
}
