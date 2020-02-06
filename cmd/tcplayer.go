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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/feilengcui008/tcplayer/deliver"
	"github.com/feilengcui008/tcplayer/factory"
	"github.com/feilengcui008/tcplayer/source"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/tcpassembly"
	log "github.com/sirupsen/logrus"
)

func init() {
	if level := os.Getenv("TCPLAYER_DEBUG"); level != "" {
		log.SetLevel(log.DebugLevel)
	}
}

var (
	dev         = flag.String("dev", "eth0", "device to capture")
	bpf         = flag.String("bpf", "", "bpf filter expr")
	caplen      = flag.Int("caplen", 65535, "caplen")
	promisc     = flag.Bool("promisc", true, "turn on promisc mode")
	file        = flag.String("file", "", "offline pcap file to read packetes")
	lport       = flag.String("lport", "", "local listening port to get traffic stream")
	proto       = flag.Int("proto", 0, "proto type, 0 for VideoPacket, 1 for HTTP, 2 for GRPC, 3 for THRIFT")
	raddr       = flag.String("raddr", "127.0.0.1:8886", "remote ip address and port")
	clone       = flag.Int("clone", 0, "clone count for each request")
	long        = flag.Bool("long", false, "establish long connections with remote host")
	concurrency = flag.Int("concurrency", 1, "number of concurrent senders(clients)")
	last        = flag.Int("last", 0, "number of ms for capturing and replaying requests")
	mode        = flag.Int("mode", 0, "replay mode, 0 for application layer requests, 1 for raw tcp packets")
	tprotocol   = flag.Int("tprotocol", 0, "thrft protocol type, 0 for TBinaryProtocol, 1 for TCompactProtocol")
)

func handleSource(ctx context.Context, assembler *tcpassembly.Assembler, pktSource *gopacket.PacketSource) {
	var (
		totalCnt int64
		preCnt   int64
		preTime  = time.Now()
	)
	for {
		select {
		case <-ctx.Done():
			log.Infof("stop capturing from source")
			return
		case packet := <-pktSource.Packets():
			if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
				totalCnt++
				now := time.Now()
				if now.After(preTime.Add(time.Second * 1)) {
					log.Infof("total %d packets, %d packets/s", totalCnt, totalCnt-preCnt)
					preCnt = totalCnt
					preTime = now
				}
				tcp, _ := tcpLayer.(*layers.TCP)
				assembler.Assemble(tcp.TransportFlow(), tcp)
			}
		}
	}
}

func main() {
	flag.Parse()
	// HTTP 1.x only supports short connections and does not support ModeRaw
	if factory.ProtoType(*proto) == factory.ProtoHTTP {
		if *long || deliver.ModeType(*mode) == deliver.ModeRaw {
			log.Errorf("ProtoHTTP does not support long connection or ModeRaw ")
			return
		}
	}
	// GRPC only supports long connections and does not support ModeRequest
	if factory.ProtoType(*proto) == factory.ProtoGRPC {
		if !*long || deliver.ModeType(*mode) == deliver.ModeRequest {
			log.Errorf("ProtoGRPC does not support short connection or ModeRequest")
			return
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// create Deliver
	dlc := &deliver.DeliverConfig{
		Clone:        *clone,
		Concurrency:  *concurrency,
		IsLong:       *long,
		RemoteAddr:   *raddr,
		Last:         *last,
		ProtocolType: *tprotocol,
		Mode:         deliver.ModeType(*mode),
	}
	d, err := deliver.NewDeliver(ctx, dlc)
	if err != nil {
		log.Errorf("create deliver failed: %v", err)
		return
	}
	// create StreamFactory
	var f tcpassembly.StreamFactory
	switch ft := factory.ProtoType(*proto); ft {
	case factory.ProtoVideoPacket:
		f = factory.NewVideoPacketStreamFactory(d)
	case factory.ProtoHTTP:
		f = factory.NewHTTPStreamFactory(d)
	case factory.ProtoGRPC:
		f = factory.NewGrpcStreamFactory(d)
	case factory.ProtoThrift:
		f = factory.NewThriftStreamFactory(d)
	default:
		log.Errorf("do not support proto type %v", ft)
		return
	}
	// create Assembler
	var (
		streamPool = tcpassembly.NewStreamPool(f)
		assembler  = tcpassembly.NewAssembler(streamPool)
	)
	assembler.MaxBufferedPagesPerConnection = 6
	// live source using libpcap
	lsc := &source.LiveSourceConfig{
		Dev:     *dev,
		Caplen:  int32(*caplen),
		Bpf:     *bpf,
		Promisc: *promisc,
	}
	if s, err := source.NewLiveSource(lsc); err != nil {
		log.Errorf("create live source failed: %v", err)
		return
	} else {
		go handleSource(ctx, assembler, s)
	}
	// offline source using pcap file
	if *file != "" {
		osc := &source.OfflineSourceConfig{
			FilePath: *file,
			Bpf:      *bpf,
		}
		if s, err := source.NewOfflineSource(osc); err != nil {
			log.Errorf("create OfflineSource failed: %v", err)
			return
		} else {
			go handleSource(ctx, assembler, s)
		}
	}
	// tcp source
	if *lport != "" {
		tsc := &source.TcpSourceConfig{
			Address: fmt.Sprintf("::%s", *lport),
		}
		if sc, err := source.NewTcpSource(ctx, tsc); err != nil {
			log.Errorf("create TcpSource failed: %v", err)
			return
		} else {
			go func(ctx context.Context, ch chan *gopacket.PacketSource) {
				for {
					select {
					case <-ctx.Done():
						return
					case s := <-sc:
						go handleSource(ctx, assembler, s)
					}
				}
			}(ctx, sc)
		}
	}

	var tc <-chan time.Time
	if *last > 0 {
		tc = time.After(time.Second * time.Duration(*last))
	}
	select {
	case <-tc:
		return
	}
}
