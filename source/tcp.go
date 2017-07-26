// Copyright © 2017 feilengcui008 <feilengcui008@gmail.com>.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
