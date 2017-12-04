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

// Sender writes request bytes to remote connections
// it can maintain multiple connections concurrently
// and receive bytes from the chan C.
package deliver

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"time"
)

type Sender interface {
	run()
	destroy()
	Data() chan []byte
}

type LongConnSender struct {
	RemoteAddr string
	ConnNum    int
	Remotes    []net.Conn
	ConnState  []bool
	Ctx        context.Context
	C          chan []byte
	Stat       *Stat
}

func (s *LongConnSender) run() {
	defer s.destroy()
	for {
		select {
		case <-s.Ctx.Done():
			return
		case req := <-s.C:
			s.Stat.TotalRequest += 1
			now := time.Now()
			if now.After(s.Stat.LastStatTime.Add(time.Second * 1)) {
				s.Stat.RequestPerSecond = s.Stat.TotalRequest - s.Stat.LastTotalRequest
				log.Infof("Remote[%s] total reqs %d, %d reqs/s", s.RemoteAddr, s.Stat.TotalRequest, s.Stat.RequestPerSecond)
				s.Stat.LastTotalRequest = s.Stat.TotalRequest
				s.Stat.LastStatTime = now
			}
			for idx, conn := range s.Remotes {
				if !s.ConnState[idx] {
					continue
				}
				if _, err := conn.Write(req); err != nil {
					log.Errorf("Write to remote %s failed %s", s.RemoteAddr, err)
					s.Remotes[idx].Close()
					s.ConnState[idx] = false
				}
			}
		}
	}
}

func (s *LongConnSender) destroy() {
	for idx, conn := range s.Remotes {
		if s.ConnState[idx] && conn != nil {
			conn.Close()
			s.ConnState[idx] = false
		}
	}
}

func (s *LongConnSender) Data() chan []byte {
	return s.C
}

func NewLongConnSender(ctx context.Context, n int, addr string) (Sender, error) {
	s := &LongConnSender{
		RemoteAddr: addr,
		ConnNum:    n,
		ConnState:  []bool{},
		Ctx:        ctx,
		C:          make(chan []byte),
		Stat:       &Stat{},
	}

	// establish several connections, each request
	// bytes buf will be send to all those conns.
	for i := 0; i < n; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			err = fmt.Errorf("Connect to remote %s failed %s", s.RemoteAddr, err)
			s.destroy()
			return nil, err
		}
		s.Remotes = append(s.Remotes, conn)
		s.ConnState = append(s.ConnState, true)
	}

	go s.run()
	return s, nil
}

type ShortConnSender struct {
	RemoteAddr string
	ConnNum    int
	Ctx        context.Context
	C          chan []byte
	Stat       *Stat
}

func (s *ShortConnSender) run() {
	defer s.destroy()
	for {
		select {
		case <-s.Ctx.Done():
			return
		case req := <-s.C:
			s.Stat.TotalRequest += 1
			now := time.Now()
			if now.After(s.Stat.LastStatTime.Add(time.Second * 1)) {
				s.Stat.RequestPerSecond = s.Stat.TotalRequest - s.Stat.LastTotalRequest
				log.Infof("Remote[%s] total reqs %d, %d reqs/s", s.RemoteAddr, s.Stat.TotalRequest, s.Stat.RequestPerSecond)
				s.Stat.LastTotalRequest = s.Stat.TotalRequest
				s.Stat.LastStatTime = now
			}
			for i := 0; i < s.ConnNum; i++ {
				go s.sendOne(req)
			}
		}
	}
}

func (s *ShortConnSender) sendOne(req []byte) {
	conn, err := net.Dial("tcp", s.RemoteAddr)
	if err != nil {
		log.Errorf("Send one to remote %s failed %s", s.RemoteAddr, err)
		return
	}
	defer conn.Close()
	if _, err := conn.Write(req); err != nil {
		log.Errorf("Write one to remote %s failed %s", s.RemoteAddr, err)
	}
	// try to cunsume response for 3 seconds
	tm := time.After(time.Second * time.Duration(3))
	buf := make([]byte, 4096)
	for {
		select {
		case <-tm:
			return
		default:
			if _, err := io.ReadFull(conn, buf); err != nil {
				return
			}
		}
	}
}

func (s *ShortConnSender) destroy() {
}

func (s *ShortConnSender) Data() chan []byte {
	return s.C
}

func NewShortConnSender(ctx context.Context, n int, addr string) (Sender, error) {
	s := &ShortConnSender{
		RemoteAddr: addr,
		ConnNum:    n,
		Ctx:        ctx,
		C:          make(chan []byte),
		Stat:       &Stat{},
	}

	go s.run()
	return s, nil
}
