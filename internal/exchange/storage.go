package exchange

import (
	"context"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"time"
)

type Storage struct {
	ctx                       context.Context
	store                     storage.Store
	tickers                   []storage.Ticker
	trades                    []storage.Trade
	candles                   map[storage.CandleKey]storage.Candle
	tickersStream             chan []storage.Ticker
	tradesStream              chan []storage.Trade
	candlesStream             chan map[storage.CandleKey]storage.Candle
	tickersCount              int
	tickersBuffer             int
	tradesCount               int
	tradesBuffer              int
	candlesCount              int
	candlesBuffer             int
	candlesPreBuffer          map[storage.CandleKey]storage.Candle
	candlesPreBufferedCount   int
	candlesPreBufferThreshold int
}

func NewStorage(ctx context.Context, store storage.Store, tickersBuf int, tradesBuf int, candlesBuf int) *Storage {
	return &Storage{
		ctx:                       ctx,
		store:                     store,
		tickers:                   make([]storage.Ticker, 0, tickersBuf),
		trades:                    make([]storage.Trade, 0, tradesBuf),
		candles:                   make(map[storage.CandleKey]storage.Candle, candlesBuf),
		tickersStream:             make(chan []storage.Ticker, 1),
		tradesStream:              make(chan []storage.Trade, 1),
		candlesStream:             make(chan map[storage.CandleKey]storage.Candle, 1),
		tickersBuffer:             tickersBuf,
		tradesBuffer:              tradesBuf,
		candlesBuffer:             candlesBuf,
		candlesPreBuffer:          make(map[storage.CandleKey]storage.Candle, 2),
		candlesPreBufferThreshold: 2,
	}
}

func (s *Storage) Start(errGroup *errgroup.Group) {
	errGroup.Go(s.tickersToStore)
	errGroup.Go(s.tradesToStore)
	errGroup.Go(s.candlesToStore)
}

func (s *Storage) AppendTicker(ticker storage.Ticker) {
	s.tickersCount++
	s.tickers = append(s.tickers, ticker)
	if s.tickersCount == s.tickersBuffer {
		tickers := s.tickers
		s.tickersStream <- tickers

		s.tickersCount = 0
		s.tickers = make([]storage.Ticker, 0, s.tickersBuffer)
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
		s.trades = make([]storage.Trade, 0, s.tradesBuffer)
	}

	return
}

func (s *Storage) AppendCandle(candle storage.Candle) {
	candle.Timestamp = candle.Timestamp.Truncate(time.Minute)
	key := storage.CandleKey{Market: candle.MktID, Timestamp: candle.Timestamp}

	_, ok := s.candles[key]
	if ok {
		s.candles[key] = candle
	} else {
		_, ok := s.candlesPreBuffer[key]
		if !ok {
			s.candlesPreBufferedCount++

			if s.candlesPreBufferedCount > s.candlesPreBufferThreshold {
				key := storage.CandleKey{
					Market:    candle.MktID,
					Timestamp: candle.Timestamp.Add(time.Duration(s.candlesPreBufferThreshold) * -time.Minute),
				}

				s.candlesCount++
				s.candles[key] = s.candlesPreBuffer[key]

				s.candlesPreBufferedCount--
				delete(s.candlesPreBuffer, key)
			}
		}

		s.candlesPreBuffer[key] = candle
	}

	if s.candlesCount == s.candlesBuffer {
		candles := s.candles
		s.candlesStream <- candles

		s.candlesCount = 0
		s.candles = make(map[storage.CandleKey]storage.Candle, s.candlesBuffer)
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
