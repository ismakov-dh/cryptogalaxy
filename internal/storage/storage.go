package storage

import (
	"context"
	"time"
)

type Store interface {
	CommitTrades(context.Context, []Trade) error
	CommitTickers(context.Context, []Ticker) error
}

// Ticker represents final form of market ticker info received from exchange
// ready to store.
type Ticker struct {
	Exchange      string
	MktID         string
	MktCommitName string
	Price         float64
	Timestamp     time.Time
	InfluxVal     int64 `json:",omitempty"`
}

// Trade represents final form of market trade info received from exchange
// ready to store.
type Trade struct {
	Exchange      string
	MktID         string
	MktCommitName string
	TradeID       string
	Side          string
	Size          float64
	Price         float64
	Timestamp     time.Time
	InfluxVal     int64 `json:",omitempty"`
}
