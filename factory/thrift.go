package factory

import (
	log "github.com/Sirupsen/logrus"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	"io"
	"sync/atomic"
	"tcplayer/deliver"
)

// TCP -> Thrift
var thriftStreamCount uint64 = 0

type ThriftStreamFactory struct {
	d *deliver.Deliver
}

func (f *ThriftStreamFactory) New(l, r gopacket.Flow) tcpassembly.Stream {
	s := tcpreader.NewReaderStream()
	n := atomic.AddUint64(&thriftStreamCount, 1)
	log.Debugf("Stream Count %d", n)
	go f.handleThriftStream(&s)
	return &s
}

// TODO: parse thrift binary protocol
func (f *ThriftStreamFactory) handleThriftStream(r io.Reader) {
	log.Errorf("TODO")
	return
}

func NewThriftStreamFactory(d *deliver.Deliver) *ThriftStreamFactory {
	return &ThriftStreamFactory{
		d: d,
	}
}
