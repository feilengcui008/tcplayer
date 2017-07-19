package main

import (
	"context"
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/tcpassembly"
	"os"
	"tcplayer/deliver"
	"tcplayer/factory"
	"tcplayer/source"
	"time"
)

var (
	dev         string
	bpf         string
	caplen      int
	promisc     bool
	file        string
	lport       string
	proto       int
	raddr       string
	clone       int
	long        bool
	concurrency int
	last        int
	mode        int
)

func init() {
	flag.StringVar(&dev, "dev", "eth0", "device to capture")
	flag.StringVar(&bpf, "bpf", "", "bpf filter")
	flag.IntVar(&caplen, "caplen", 65535, "caplen")
	flag.BoolVar(&promisc, "promisc", true, "turn on promisc mode")
	flag.StringVar(&file, "file", "", "offline pcap file to read packetes")
	flag.StringVar(&lport, "lport", "", "local listening port to get traffic stream")
	flag.IntVar(&proto, "proto", 0, "proto type, 0 for VideoPacket, 1 for HTTP, 2 for GRPC, 3 for THRIFT")
	flag.StringVar(&raddr, "raddr", "127.0.0.1:8886", "remote ip address and port")
	flag.IntVar(&clone, "clone", 0, "clone count for each request")
	flag.BoolVar(&long, "long", false, "establish long connections with remote host")
	flag.IntVar(&concurrency, "concurrency", 1, "number of concurrent senders(clients)")
	flag.IntVar(&last, "last", 0, "number of ms for capturing and replaying requests")
	flag.IntVar(&mode, "mode", 0, "replay mode, 0 for application layer requests, 1 for raw tcp packets")

	if level := os.Getenv("TCPLAYER_DEBUG"); level != "" {
		log.SetLevel(log.DebugLevel)
	}

}
func handleSource(ctx context.Context, assembler *tcpassembly.Assembler, pktSource *gopacket.PacketSource) {
	var totalCnt int64 = 0
	var preCnt int64 = 0
	preTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case packet := <-pktSource.Packets():
			if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
				totalCnt += 1
				// TODO: better stats
				now := time.Now()
				if now.After(preTime.Add(time.Second * 1)) {
					log.Infof("PacketSource total %d packets, %d packets/s", totalCnt, totalCnt-preCnt)
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

	// http 1.x only support short connections and ModeRequest
	if factory.ProtoType(proto) == factory.ProtoHTTP {
		if long {
			log.Errorf("ProtoHTTP only support short connection")
			return
		}
		if deliver.ModeType(mode) == deliver.ModeRaw {
			log.Errorf("ProtoHTTP don't support ModeRaw")
			return
		}
	}

	if factory.ProtoType(proto) == factory.ProtoGRPC {
		if !long {
			log.Errorf("ProtoGRPC only support long connection")
			return
		}
		if deliver.ModeType(mode) == deliver.ModeRequest {
			log.Errorf("ProtoGRPC don't support ModeRequest")
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create Deliver
	dlc := &deliver.DeliverConfig{
		Clone:       clone,
		Concurrency: concurrency,
		IsLong:      long,
		RemoteAddr:  raddr,
		Last:        last,
		Mode:        deliver.ModeType(mode),
	}
	d, err := deliver.NewDeliver(ctx, dlc)
	if err != nil {
		log.Errorf("Create Deliver failed %s", err)
		return
	}

	// create StreamFactory
	var f tcpassembly.StreamFactory
	switch factory.ProtoType(proto) {
	case factory.ProtoVideoPacket:
		f = factory.NewVideoPacketStreamFactory(d)
	case factory.ProtoHTTP:
		f = factory.NewHTTPStreamFactory(d)
	case factory.ProtoGRPC:
		f = factory.NewGrpcStreamFactory(d)
	case factory.ProtoThrift:
		f = factory.NewThriftStreamFactory(d)
	default:
		log.Errorf("Do not support this mode")
		return
	}

	// create Assembler
	streamPool := tcpassembly.NewStreamPool(f)
	assembler := tcpassembly.NewAssembler(streamPool)
	assembler.MaxBufferedPagesPerConnection = 6

	// live source using libpcap
	lsc := &source.LiveSourceConfig{
		Dev:     dev,
		Caplen:  int32(caplen),
		Bpf:     bpf,
		Promisc: promisc,
	}
	if s, err := source.NewLiveSource(lsc); err != nil {
		log.Errorf("Create LiveSource failed %s", err)
	} else {
		go handleSource(ctx, assembler, s)
	}

	// offline source using pcap file
	if file != "" {
		osc := &source.OfflineSourceConfig{
			FilePath: file,
			Bpf:      bpf,
		}
		if s, err := source.NewOfflineSource(osc); err != nil {
			log.Errorf("Create OfflineSource failed %s", err)
		} else {
			go handleSource(ctx, assembler, s)
		}
	}

	// tcp source
	if lport != "" {
		tsc := &source.TcpSourceConfig{
			Address: fmt.Sprintf("::%s", lport),
		}
		if sc, err := source.NewTcpSource(ctx, tsc); err != nil {
			log.Errorf("Create TcpSource failed %s", err)
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

	var tc <-chan time.Time = nil
	if last > 0 {
		tc = time.After(time.Second * time.Duration(last))
	}

	select {
	case <-tc:
		return
	}
}
