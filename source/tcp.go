package source

import (
	"context"
	log "github.com/Sirupsen/logrus"
	"github.com/google/gopacket"
	"github.com/google/gopacket/pcapgo"
	"net"
)

type TcpSourceConfig struct {
	Address string
	MaxConn int
}

func NewTcpSource(ctx context.Context, c *TcpSourceConfig) (chan *gopacket.PacketSource, error) {
	ch := make(chan *gopacket.PacketSource)
	go func(ch chan *gopacket.PacketSource) {
		l, err := net.Listen("tcp", c.Address)
		if err != nil {
			log.Errorf("Listening on %s failed %s", c.Address, err)
			return
		}
		defer l.Close()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if conn, err := l.Accept(); err != nil {
					log.Errorf("Accept error %s", err)
				} else if handle, err := pcapgo.NewReader(conn); err != nil {
					log.Errorf("Pcapgo new reader failed %s", err)
				} else {
					pktSource := gopacket.NewPacketSource(handle, handle.LinkType())
					ch <- pktSource
				}
			}
		}
	}(ch)

	return ch, nil
}
