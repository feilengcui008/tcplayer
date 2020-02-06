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
	"io"
	"sync/atomic"

	"github.com/feilengcui008/tcplayer/deliver"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	log "github.com/sirupsen/logrus"
)

const ThriftMaxBufferSize int = 4096

// TCP -> Thrift
var thriftStreamCount uint64

type ThriftStreamFactory struct {
	d *deliver.Deliver
}

func (f *ThriftStreamFactory) New(l, r gopacket.Flow) tcpassembly.Stream {
	s := tcpreader.NewReaderStream()
	n := atomic.AddUint64(&thriftStreamCount, 1)
	log.Debugf("stream count %d", n)
	go f.handleThriftStream(&s)
	return &s
}

func (f *ThriftStreamFactory) handleThriftStream(r io.Reader) {
	ctx, cancel := context.WithCancel(f.d.Ctx)
	defer cancel()
	sender, err := deliver.NewLongConnSender(ctx, f.d.Config.Clone+1, f.d.Config.RemoteAddr)
	if err != nil {
		log.Errorf("thriftStreamFactory create sender error: %v", err)
		return
	}
	parser := f.parseThriftBinaryMessageHeader
	if f.d.Config.ProtocolType == deliver.TCompactProtocol {
		parser = f.parseThriftCompactMessageHeader
	}
	for {
		// we assume the following packets are valid thrift requests
		header, err := parser(r)
		if err != nil {
			log.Errorf("ThriftStreamFactory parse thrift message header failed: %v", err)
			return
		}
		sender.Data() <- header
		for {
			buf := make([]byte, ThriftMaxBufferSize)
			if n, err := io.ReadFull(r, buf); err != nil {
				log.Errorf("ThriftStreamFactory read full failed: %v", err)
				if n > 0 {
					sender.Data() <- buf[:n]
				}
				break
			}
			sender.Data() <- buf
		}
	}
}

// https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
/*
Compact protocol Message (4+ bytes):
+--------+--------+--------+...+--------+--------+...+--------+--------+...+--------+
|pppppppp|mmmvvvvv| seq id              | name length         | name                |
+--------+--------+--------+...+--------+--------+...+--------+--------+...+--------+
Where:
    * pppppppp is the protocol id, fixed to 1000 0010, 0x82.
    * mmm is the message type, an unsigned 3 bit integer.
    * vvvvv is the version, an unsigned 5 bit integer, fixed to 00001.
    * seq id is the sequence id, a signed 32 bit integer encoded as a var int.
    * name length is the byte length of the name field, a signed 32 bit integer encoded as a var int (must be >= 0).
    * name is the method name to invoke, a UTF-8 encoded string.)
*/
func (f *ThriftStreamFactory) parseThriftCompactMessageHeader(r io.Reader) ([]byte, error) {
	vFirstByte := make([]byte, 1)
	vSecondByte := make([]byte, 1)
	for {
		// loop until find a valid first byte
		for {
			if _, err := io.ReadFull(r, vFirstByte); err != nil || int(vFirstByte[0]) != 0x82 {
				log.Debugf("ThriftStreamFactory read version first byte failed: %v", err)
				if err == io.EOF {
					return nil, err
				}
				continue
			}
			break
		}
		if _, err := io.ReadFull(r, vSecondByte); err != nil || (int(vSecondByte[0])&0x1f) != 1 {
			log.Debugf("ThriftStreamFactory read version second byte failed: %v", err)
			if err == io.EOF {
				return nil, err
			}
			continue
		}
		msgHdr := []byte{vFirstByte[0], vSecondByte[0]}
		log.Debugf("ThriftStreamFactory got a valid request header: %v", msgHdr)
		return msgHdr, nil
	}
}

// https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md
/*
Binary protocol Message, strict encoding, 12+ bytes:
+--------+--------+--------+--------+--------+--------+--------+--------+--------+...+--------+--------+--------+--------+--------+
|1vvvvvvv|vvvvvvvv|unused  |00000mmm| name length                       | name                | seq id                            |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+...+--------+--------+--------+--------+--------+
*/
// Parse thrift message header, we just use the leading
// 29 bits to recognize a valid thrift message.
// 10000000 00000001 00000000 00000xxx
func (f *ThriftStreamFactory) parseThriftBinaryMessageHeader(r io.Reader) ([]byte, error) {
	vFirstByte := make([]byte, 1)
	vSecondByte := make([]byte, 1)
	vThirdByte := make([]byte, 1)
	vFourthByte := make([]byte, 1)
	for {
		// loop until find a valid first byte
		for {
			if _, err := io.ReadFull(r, vFirstByte); err != nil || int(vFirstByte[0]) != 128 {
				log.Debugf("ThriftStreamFactory read version first byte failed: %v", err)
				if err == io.EOF {
					return nil, err
				}
				continue
			}
			break
		}
		if _, err := io.ReadFull(r, vSecondByte); err != nil || int(vSecondByte[0]) != 1 {
			log.Debugf("ThriftStreamFactory read version second byte failed: %v", err)
			if err == io.EOF {
				return nil, err
			}
			continue
		}
		if _, err := io.ReadFull(r, vThirdByte); err != nil || int(vThirdByte[0]) != 0 {
			log.Debugf("ThriftStreamFactory read version third byte failed: %v", err)
			if err == io.EOF {
				return nil, err
			}
			continue
		}

		if _, err := io.ReadFull(r, vFourthByte); err != nil || int(vFourthByte[0]) > 7 || int(vFourthByte[0]) < 0 {
			log.Debugf("ThriftStreamFactory read version fourth byte failed: %v", err)
			if err == io.EOF {
				return nil, err
			}
			continue
		}
		msgHdr := []byte{vFirstByte[0], vSecondByte[0], vThirdByte[0], vFourthByte[0]}
		log.Debugf("ThriftStreamFactory got a valid request header: %v", msgHdr)
		return msgHdr, nil
	}
}

func NewThriftStreamFactory(d *deliver.Deliver) *ThriftStreamFactory {
	return &ThriftStreamFactory{
		d: d,
	}
}
