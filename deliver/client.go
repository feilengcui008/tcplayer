// Client stands for a client which send requests to remote
// server, client can clone at request level, since request
// is not related to the underlining tcp packet sequence, so
// more flexible.

package deliver

import (
	"context"
	"fmt"
)

type ClientConfig struct {
	RemoteAddr string
	IsLong     bool
	Clone      int
}

// Client
type Client struct {
	Idx    int
	Config *ClientConfig
	S      Sender
	Ctx    context.Context
	Stat   *Stat
}

func NewClient(ctx context.Context, c *ClientConfig) (*Client, error) {
	client := &Client{Config: c}
	creator := NewLongConnSender
	if !c.IsLong {
		creator = NewShortConnSender
	}
	s, err := creator(ctx, 1, c.RemoteAddr)
	if err != nil {
		return nil, fmt.Errorf("NewClient failed %s", err)
	}
	client.S = s
	return client, nil
}
