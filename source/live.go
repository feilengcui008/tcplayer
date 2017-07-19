package source

import (
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
)

type LiveSourceConfig struct {
	Dev     string
	Caplen  int32
	Promisc bool
	Bpf     string
}

func NewLiveSource(c *LiveSourceConfig) (*gopacket.PacketSource, error) {
	if handle, err := pcap.OpenLive(c.Dev, int32(c.Caplen), c.Promisc, pcap.BlockForever); err != nil {
		return nil, err
	} else if err := handle.SetBPFFilter(c.Bpf); err != nil {
		return nil, err
	} else {
		pktSource := gopacket.NewPacketSource(handle, handle.LinkType())
		return pktSource, nil
	}
}
