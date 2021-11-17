package storage

import (
	"context"
	"strings"
	"time"

	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	nc "github.com/nats-io/nats.go"
)

// Nats is for connecting and inserting data to Nats.
type Nats struct {
	Basic  *nc.Conn
	Client *nc.EncodedConn
	Cfg    *config.NATS
}

var _nats *Nats

// InitNATS initializes Nats connection with configured values.
func InitNATS(cfg *config.NATS) (*Nats, error) {
	if _nats == nil {
		basic, err := nc.Connect(
			strings.Join(cfg.Addresses, ","),
			nc.Name("Cryptogalaxy Publisher"),
			nc.Timeout(time.Duration(cfg.ReqTimeoutSec)*time.Second),
			nc.PingInterval(-1),
			nc.NoReconnect(),
			nc.UserInfo(cfg.Username, cfg.Password),
		)
		if err != nil {
			return nil, err
		}

		// Client for structured data publish.
		client, err := nc.NewEncodedConn(basic, nc.JSON_ENCODER)
		if err != nil {
			return nil, err
		}

		_nats = &Nats{
			Basic:  basic,
			Client: client,
			Cfg:    cfg,
		}
		stores[NATS] = _nats
	}

	return _nats, nil
}

// CommitTickers batch inserts input ticker data to Nats.
func (n *Nats) CommitTickers(_ context.Context, data []*Ticker) error {
	for i := range data {
		ticker := data[i]

		err := n.Client.Publish(n.Cfg.SubjectBaseName+".ticker", &ticker)
		if err != nil {
			return err
		}
	}

	// Maybe data is pushed to Nats server before the flush also.
	// Because client library of Nats automatically buffers and pushes the data to server.
	// This is just to confirm.
	err := n.Client.Flush()
	if err != nil {
		return err
	}
	return nil
}

// CommitTrades batch inserts input trade data to Nats.
func (n *Nats) CommitTrades(_ context.Context, data []*Trade) error {
	for i := range data {
		trade := data[i]

		err := n.Client.Publish(n.Cfg.SubjectBaseName+".trade", &trade)
		if err != nil {
			return err
		}
	}

	// Maybe data is pushed to Nats server before the flush also.
	// Because client library of Nats automatically buffers and pushes the data to server.
	// This is just to confirm.
	err := n.Client.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (n *Nats) CommitCandles(_ context.Context, _ []*Candle) error { return nil }
