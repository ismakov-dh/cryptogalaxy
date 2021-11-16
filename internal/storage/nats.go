package storage

import (
	"context"
	"strings"
	"time"

	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	nc "github.com/nats-io/nats.go"
)

// nats is for connecting and inserting data to nats.
type nats struct {
	Basic  *nc.Conn
	Client *nc.EncodedConn
	Cfg    *config.NATS
}

// InitNATS initializes nats connection with configured values.
func InitNATS(cfg *config.NATS) (Store, error) {
	if _, ok := stores[NATS]; !ok {
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

		stores[NATS] = &nats{
			Basic:  basic,
			Client: client,
			Cfg:    cfg,
		}
	}

	return stores[NATS], nil
}

// CommitTickers batch inserts input ticker data to nats.
func (n *nats) CommitTickers(_ context.Context, data []*Ticker) error {
	for i := range data {
		ticker := data[i]

		err := n.Client.Publish(n.Cfg.SubjectBaseName+".ticker", &ticker)
		if err != nil {
			return err
		}
	}

	// Maybe data is pushed to nats server before the flush also.
	// Because client library of nats automatically buffers and pushes the data to server.
	// This is just to confirm.
	err := n.Client.Flush()
	if err != nil {
		return err
	}
	return nil
}

// CommitTrades batch inserts input trade data to nats.
func (n *nats) CommitTrades(_ context.Context, data []*Trade) error {
	for i := range data {
		trade := data[i]

		err := n.Client.Publish(n.Cfg.SubjectBaseName+".trade", &trade)
		if err != nil {
			return err
		}
	}

	// Maybe data is pushed to nats server before the flush also.
	// Because client library of nats automatically buffers and pushes the data to server.
	// This is just to confirm.
	err := n.Client.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (n *nats) CommitCandles(_ context.Context, _ []*Candle) error { return nil }
