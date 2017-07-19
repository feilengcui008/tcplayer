package factory

import (
	"context"
	"encoding/binary"
	log "github.com/Sirupsen/logrus"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	"io"
	"sync/atomic"
	"tcplayer/deliver"
)

// TCP -> VideoPacket
var videoPacketStreamCount uint64 = 0

type VideoPacketStreamFactory struct {
	d *deliver.Deliver
}

func (f *VideoPacketStreamFactory) New(l, r gopacket.Flow) tcpassembly.Stream {
	s := tcpreader.NewReaderStream()
	n := atomic.AddUint64(&videoPacketStreamCount, 1)
	log.Debugf("Stream Count %d", n)
	if f.d.Config.Mode == deliver.ModeRaw {
		go f.handleVideoPacketRaw(&s)
	} else {
		go f.handleVideoPacketRequest(&s)
	}
	return &s
}

func (f *VideoPacketStreamFactory) handleVideoPacketRequest(r io.Reader) {
	for {
		// must be a valid request or EOF
		req, err := f.parseVideoPacketRequest(r)
		if err != nil {
			log.Errorf("VideoPacketStreamFactory did not find a valid req %s", err)
			return
		}
		f.d.C <- req
	}
}

func (f *VideoPacketStreamFactory) handleVideoPacketRaw(r io.Reader) {
	ctx, cancel := context.WithCancel(f.d.Ctx)
	defer cancel()

	sender, err := deliver.NewLongConnSender(ctx, f.d.Config.Clone+1, f.d.Config.RemoteAddr)
	if err != nil {
		log.Errorf("Create sender failed %s", err)
		return
	}

	for {
		// first we get a valid request, then we can
		// assume the following traffic contains all
		// valid requests until error happens
		req, err := f.parseVideoPacketRequest(r)
		if err != nil {
			log.Errorf("VideoPacketStreamFactory did not find a valid req %s", err)
			return
		}
		sender.Data() <- req

		for {
			// buf must in loop for avoiding race condition
			buf := make([]byte, 4096)
			// when error happens, we go to outer loop
			// and try to refind a valid request
			if n, err := io.ReadFull(r, buf); err != nil {
				log.Errorf("VideoPacketStreamFactory read full failed %s", err)
				if n > 0 {
					sender.Data() <- buf
				}
				break
			}
			sender.Data() <- buf
		}
	}
}

func (f *VideoPacketStreamFactory) parseVideoPacketRequest(r io.Reader) ([]byte, error) {
	for {
		// 1 header byte
		proto := make([]byte, 1)
		for {
			if _, err := r.Read(proto); err != nil {
				log.Debugf("Read header byte for VideoPacket failed %s", err)
				if err == io.EOF {
					return nil, err
				}
			} else {
				// maybe a valid packet
				if int(proto[0]) == 0x26 {
					break
				}
				log.Debugf("Got a valid proto head")
			}
		}
		// 4 length bytes
		length := make([]byte, 4)
		var dataLength uint64 = 0
		if _, err := r.Read(length); err != nil {
			log.Debugf("Read length for VideoPacket failed %s", err)
			if err == io.EOF {
				return nil, err
			}
		} else {
			dataLength = uint64(binary.BigEndian.Uint32(length)) - 17
			if dataLength < 0 {
				log.Debugf("Length %d for VideoPacket not valid", dataLength)
				continue
			}
			log.Debugf("Got a valid data len %d", dataLength)
		}
		// 1 version byte
		version := make([]byte, 1)
		if _, err := r.Read(version); err != nil {
			log.Debugf("Read version for VideoPacket failed %s", err)
			if err == io.EOF {
				return nil, err
			}
		} else {
			if int(version[0]) != 1 {
				log.Debugf("Version %d for VideoPacket not valid", int(version[0]))
				continue
			}
			log.Debugf("Read version %d", int(version[0]))
		}
		// 10 reserved bytes
		reserved := make([]byte, 10)
		if n, err := r.Read(reserved); err != nil {
			log.Debugf("Read reserved for VideoPacket failed %s", err)
			if err == io.EOF {
				return nil, err
			}
		} else {
			log.Debugf("Read reserved len %d", n)
		}
		// read data
		data := []byte{}
		if dataLength > 0 {
			if dataLength > 1024*1024*10 {
				log.Debugf("Data length too long, skip this request")
				continue
			}
			data = make([]byte, dataLength)
			if n, err := r.Read(data); err != nil {
				log.Debugf("Read data for VideoPacket failed %s", err)
				if err == io.EOF {
					return nil, err
				}
			} else {
				log.Debugf("Read data len %d", n)
			}
		}
		// 1 tail byte
		tail := make([]byte, 1)
		if _, err := r.Read(tail); err != nil {
			log.Debugf("Read tail byte for VideoPacket failed %s", err)
			if err == io.EOF {
				return nil, err
			}
		} else {
			if int(tail[0]) != 0x28 {
				log.Debugf("Tail byte is not 0x28")
				continue
			}
		}

		reqData := []byte{}
		reqData = append(reqData, proto[:]...)
		reqData = append(reqData, length[:]...)
		reqData = append(reqData, version[:]...)
		reqData = append(reqData, reserved[:]...)
		reqData = append(reqData, data[:]...)
		reqData = append(reqData, tail[:]...)
		log.Debugf("Got a valid VideoPacket len %d, content %v", len(reqData), reqData)
		return reqData, nil
	}

}

func NewVideoPacketStreamFactory(d *deliver.Deliver) *VideoPacketStreamFactory {
	return &VideoPacketStreamFactory{
		d: d,
	}
}
