package factory

import (
	"context"
	log "github.com/Sirupsen/logrus"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	"io"
	"sync/atomic"
	"tcplayer/deliver"
)

// TCP -> GRPC
var grpcStreamCount uint64 = 0

type GrpcStreamFactory struct {
	d *deliver.Deliver
}

func (f *GrpcStreamFactory) New(l, r gopacket.Flow) tcpassembly.Stream {
	s := tcpreader.NewReaderStream()
	n := atomic.AddUint64(&grpcStreamCount, 1)
	log.Debugf("Stream Count %d", n)
	go f.handleGRPCStream(&s)
	return &s
}

// since grpc is based on http2 which formed by frames,
// the protocol fields for frame are variable, so we
// can't recognize the frame and dig out the requests
// content, so here we just reverse proxy whole tcp
// packets to remote, but this require we capture the
// whole tcp establishing process. Maybe try directly
// recognize grpc binary content later?
func (f *GrpcStreamFactory) handleGRPCStream(r io.Reader) {
	ctx, cancel := context.WithCancel(f.d.Ctx)
	defer cancel()

	sender, err := deliver.NewLongConnSender(ctx, f.d.Config.Clone+1, f.d.Config.RemoteAddr)
	if err != nil {
		log.Errorf("GrpcStreamFactory create serder failed %s", err)
		return
	}

	for {
		buf := make([]byte, 4096)
		if _, err := io.ReadFull(r, buf); err != nil {
			log.Errorf("Grpc read full failed %s", err)
			return
		}
		sender.Data() <- buf
	}

}

func NewGrpcStreamFactory(d *deliver.Deliver) *GrpcStreamFactory {
	return &GrpcStreamFactory{
		d: d,
	}
}
