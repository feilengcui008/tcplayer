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

const ThriftMaxBufferSize int = 4096

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

func (f *ThriftStreamFactory) handleThriftStream(r io.Reader) {
	ctx, cancel := context.WithCancel(f.d.Ctx)
	defer cancel()

	sender, err := deliver.NewLongConnSender(ctx, f.d.Config.Clone+1, f.d.Config.RemoteAddr)
	if err != nil {
		log.Errorf("ThriftStreamFactory create sender error %s", err)
		return
	}

	for {
		// we assume the following packets are valid
		// thrift requests
		header, err := f.parseThriftMessageHeader(r)
		if err != nil {
			log.Errorf("ThriftStreamFactory parse thrift message header failed %s", err)
			return
		}
		sender.Data() <- header

		for {
			buf := make([]byte, ThriftMaxBufferSize)
			if n, err := io.ReadFull(r, buf); err != nil {
				log.Errorf("ThriftStreamFactory read full failed %s", err)
				if n > 0 {
					sender.Data() <- buf[:n]
				}
				break
			}
			sender.Data() <- buf
		}
	}
}

// Parse thrift message header, we just use the leading
// 29 bits to recognize a valid thrift message.
// 10000000 00000001 00000000 00000xxx
func (f *ThriftStreamFactory) parseThriftMessageHeader(r io.Reader) ([]byte, error) {
	vFirstByte := make([]byte, 1)
	vSecondByte := make([]byte, 1)
	vThirdByte := make([]byte, 1)
	vFourthByte := make([]byte, 1)
	for {
		// we loop until find a valid first byte
		for {
			if _, err := io.ReadFull(r, vFirstByte); err != nil || int(vFirstByte[0]) != 128 {
				log.Debugf("ThriftStreamFactory read version first byte failed %s %v", err)
				if err == io.EOF {
					return nil, err
				}
				continue
			}
			break
		}
		if _, err := io.ReadFull(r, vSecondByte); err != nil || int(vSecondByte[0]) != 1 {
			log.Debugf("ThriftStreamFactory read version second byte failed %s", err)
			if err == io.EOF {
				return nil, err
			}
			continue
		}
		if _, err := io.ReadFull(r, vThirdByte); err != nil || int(vThirdByte[0]) != 0 {
			log.Debugf("ThriftStreamFactory read version third byte failed %s", err)
			if err == io.EOF {
				return nil, err
			}
			continue
		}

		if _, err := io.ReadFull(r, vFourthByte); err != nil || int(vFourthByte[0]) > 7 || int(vFourthByte[0]) < 0 {
			log.Debugf("ThriftStreamFactory read version fourth byte failed %s", err)
			if err == io.EOF {
				return nil, err
			}
			continue
		}
		msgHdr := []byte{vFirstByte[0], vSecondByte[0], vThirdByte[0], vFourthByte[0]}
		log.Debugf("ThriftStreamFactory got a valid request header %v", msgHdr)
		return msgHdr, nil
	}
}

func NewThriftStreamFactory(d *deliver.Deliver) *ThriftStreamFactory {
	return &ThriftStreamFactory{
		d: d,
	}
}
