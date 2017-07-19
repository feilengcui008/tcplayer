package factory

import (
	"bufio"
	log "github.com/Sirupsen/logrus"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	"io"
	"net/http"
	"net/http/httputil"
	"sync/atomic"
	"tcplayer/deliver"
)

// TCP -> HTTP 1.x
var httpStreamCount uint64 = 0

type HTTPStreamFactory struct {
	d *deliver.Deliver
}

func (f *HTTPStreamFactory) New(l, r gopacket.Flow) tcpassembly.Stream {
	s := tcpreader.NewReaderStream()
	httpStreamCount += 1
	n := atomic.AddUint64(&httpStreamCount, 1)
	log.Debugf("Stream Count %d", n)
	go f.handleHTTPRequest(&s)
	return &s
}

// Usually for http 1.x, one request consumes one short
// connection, there is no need for the loop of  parsing
// the protocol, if read error happens, we just drop this
// connection.
func (f *HTTPStreamFactory) handleHTTPRequest(r io.Reader) {
	buf := bufio.NewReader(r)
	for {
		if req, err := http.ReadRequest(buf); err == io.EOF {
			return
		} else if err != nil {
			log.Errorf("parsing http request error %s", err)
		} else {
			data, _ := httputil.DumpRequest(req, true)
			f.d.C <- data
		}
	}
}

func NewHTTPStreamFactory(d *deliver.Deliver) *HTTPStreamFactory {
	return &HTTPStreamFactory{
		d: d,
	}
}
