package source

import (
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
)

type OfflineSourceConfig struct {
	FilePath string
	Bpf      string
}

func NewOfflineSource(c *OfflineSourceConfig) (*gopacket.PacketSource, error) {
	if handle, err := pcap.OpenOffline(c.FilePath); err != nil {
		return nil, err
	} else if err := handle.SetBPFFilter(c.Bpf); err != nil {
		return nil, err
	} else {
		pktSource := gopacket.NewPacketSource(handle, handle.LinkType())
		return pktSource, nil
	}
}
