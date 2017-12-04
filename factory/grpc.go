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
	"context"
	"github.com/feilengcui008/tcplayer/deliver"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	log "github.com/sirupsen/logrus"
	"io"
	"sync/atomic"
)

const GrpcMaxBufferSize int = 4096

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
		buf := make([]byte, GrpcMaxBufferSize)
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
