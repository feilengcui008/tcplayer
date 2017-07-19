package deliver

import (
	"context"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"math/rand"
	"time"
)

type ModeType int

const (
	ModeRequest ModeType = 0
	ModeRaw     ModeType = 1
)

type DeliverConfig struct {
	IsLong      bool
	Concurrency int
	RemoteAddr  string
	Last        int
	Clone       int
	Mode        ModeType
}

type Deliver struct {
	Config  *DeliverConfig
	Stat    *Stat
	Clients []*Client
	Ctx     context.Context
	C       chan []byte
}

func (d *Deliver) startClient(ch chan struct{}) {
	for i := 0; i < d.Config.Concurrency; i++ {
		clientConfig := &ClientConfig{
			RemoteAddr: d.Config.RemoteAddr,
			Clone:      d.Config.Clone,
			IsLong:     d.Config.IsLong,
		}
		client, err := NewClient(d.Ctx, clientConfig)
		if err != nil {
			log.Errorf("Create client %d failed %s", i, err)
			continue
		}
		d.Clients = append(d.Clients, client)
	}
	ch <- struct{}{}
}

func (d *Deliver) deliverRequest() {
	d.Stat.StartTime = time.Now()
	d.Stat.LastStatTime = time.Now()
	for {
		select {
		case <-d.Ctx.Done():
			return
		case req := <-d.C:
			for i := 0; i < d.Config.Clone+1; i++ {
				d.Stat.TotalRequest += 1
				// TODO: move stat log to another goroutine
				now := time.Now()
				if now.After(d.Stat.LastStatTime.Add(time.Second * 1)) {
					d.Stat.RequestPerSecond = d.Stat.TotalRequest - d.Stat.LastTotalRequest
					d.Stat.LastTotalRequest = d.Stat.TotalRequest
					d.Stat.LastStatTime = now
					log.Infof("Deliver total reqs %d, %d reqs/s", d.Stat.TotalRequest, d.Stat.RequestPerSecond)
				}

				// choose a random client
				idx := rand.Int() % len(d.Clients)
				d.Clients[idx].S.Data() <- req
				log.Debugf("Send packets to %s with connection %d", d.Config.RemoteAddr, idx)
			}
		}
	}
}

func (d *Deliver) Run() error {
	if d.Config == nil {
		err := fmt.Errorf("DeliverConfig not set\n")
		return err
	}

	// we start clients only with ModeRequest
	if d.Config.Mode == ModeRequest {
		ch := make(chan struct{})
		go d.startClient(ch)
		<-ch
		go d.deliverRequest()
	}

	select {
	case <-d.Ctx.Done():
		return fmt.Errorf("Context canceled")
	}
}

func NewDeliver(ctx context.Context, config *DeliverConfig) (*Deliver, error) {
	if len(config.RemoteAddr) == 0 {
		err := fmt.Errorf("DeliverConfig not set RemoteAddrs\n")
		return nil, err
	}
	log.Debugf("DeliverConfig %#v", config)
	d := &Deliver{
		Config:  config,
		C:       make(chan []byte),
		Clients: []*Client{},
		Stat:    &Stat{},
		Ctx:     ctx,
	}
	go d.Run()
	return d, nil
}
