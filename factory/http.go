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

package factory

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httputil"
	"sync/atomic"

	"github.com/feilengcui008/tcplayer/deliver"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	log "github.com/sirupsen/logrus"
)

// TCP -> HTTP 1.x
var httpStreamCount uint64

type HTTPStreamFactory struct {
	d *deliver.Deliver
}

func (f *HTTPStreamFactory) New(l, r gopacket.Flow) tcpassembly.Stream {
	s := tcpreader.NewReaderStream()
	httpStreamCount++
	n := atomic.AddUint64(&httpStreamCount, 1)
	log.Debugf("stream count %d", n)
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
			log.Errorf("parsing http request error: %v", err)
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
