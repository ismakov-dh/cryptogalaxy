package exchange

import (
	"context"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type Storage struct {
	ctx           context.Context
	store         storage.Store
	tickers       []storage.Ticker
	tickersCount  int
	tickersBuffer int
	tickersStream chan []storage.Ticker
	trades        []storage.Trade
	tradesCount   int
	tradesBuffer  int
	tradesStream  chan []storage.Trade
	candles       []storage.Candle
	candlesCount  int
	candlesBuffer int
	candlesStream chan []storage.Candle
}

func NewStorage(ctx context.Context, store storage.Store, tickersBuffer int, tradesBuffer int, candlesBuffer int) *Storage {
	return &Storage{
		ctx:           ctx,
		store:         store,
		tickersBuffer: tickersBuffer,
		tickersStream: make(chan []storage.Ticker, 1),
		tradesBuffer:  tradesBuffer,
		tradesStream:  make(chan []storage.Trade, 1),
		candlesBuffer: candlesBuffer,
		candlesStream: make(chan []storage.Candle, 1),
	}
}

func (s *Storage) Start(errGroup *errgroup.Group) {
	errGroup.Go(func() error { return s.tradesToStore() })
	errGroup.Go(func() error { return s.tickersToStore() })
	errGroup.Go(func() error { return s.candlesToStore() })
}

func (s *Storage) AppendTicker(ticker storage.Ticker) {
	s.tickersCount++
	s.tickers = append(s.tickers, ticker)
	if s.tickersCount == s.tickersBuffer {
		tickers := s.tickers
		s.tickersStream <- tickers

		s.tickersCount = 0
		s.tickers = nil
	}

	return
}

func (s *Storage) AppendTrade(trade storage.Trade) {
	s.tradesCount++
	s.trades = append(s.trades, trade)
	if s.tradesCount == s.tradesBuffer {
		trades := s.trades
		s.tradesStream <- trades

		s.tradesCount = 0
		s.trades = nil
	}

	return
}

func (s *Storage) AppendCandle(candle storage.Candle) {
	s.candlesCount++
	s.candles = append(s.candles, candle)
	if s.tradesCount == s.tradesBuffer {
		candles := s.candles
		s.candlesStream <- candles

		s.tradesCount = 0
		s.trades = nil
	}

	return
}

func (s *Storage) tradesToStore() error {
	for {
		select {
		case data := <-s.tradesStream:
			err := s.store.CommitTrades(s.ctx, data)
			if err != nil {
				if !errors.Is(err, s.ctx.Err()) {
					log.Error().Stack().Err(errors.WithStack(err)).Msg("")
				}
				return err
			}
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *Storage) tickersToStore() error {
	for {
		select {
		case data := <-s.tickersStream:
			err := s.store.CommitTickers(s.ctx, data)
			if err != nil {
				if !errors.Is(err, s.ctx.Err()) {
					log.Error().Stack().Err(errors.WithStack(err)).Msg("")
				}
				return err
			}
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *Storage) candlesToStore() error {
	for {
		select {
		case data := <-s.candlesStream:
			err := s.store.CommitCandles(s.ctx, data)
			if err != nil {
				if !errors.Is(err, s.ctx.Err()) {
					log.Error().Stack().Err(errors.WithStack(err)).Msg("")
				}
				return err
			}
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}