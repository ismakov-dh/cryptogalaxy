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
	candles                   map[string]*candlesBuffer
	tickersStream             chan []storage.Ticker
	tradesStream              chan []storage.Trade
	candlesStream             chan []storage.Candle
	tickersCount              int
	tickersBuffer             int
	tradesCount               int
	tradesBuffer              int
	candlesBuffer             int
	candlesPreBufferThreshold int
}

type candlesBuffer struct {
	buffer           []storage.Candle
	count            int
	preBuffered      []storage.Candle
	preBufferedIdxs  map[time.Time]int
	preBufferedCount int
}

func (s *Storage) getCandleBuffer(market string) *candlesBuffer {
	buf, ok := s.candles[market]
	if !ok {
		buf = s.newCandleBuffer()
		s.candles[market] = buf
	}

	return buf
}

func (s *Storage) newCandleBuffer() *candlesBuffer {
	return &candlesBuffer{
		buffer:          make([]storage.Candle, 0, s.candlesBuffer),
		preBufferedIdxs: make(map[time.Time]int),
	}
}

func NewStorage(ctx context.Context, store storage.Store, tickersBuf int, tradesBuf int, candlesBuf int) *Storage {
	return &Storage{
		ctx:                       ctx,
		store:                     store,
		tickers:                   make([]storage.Ticker, 0, tickersBuf),
		trades:                    make([]storage.Trade, 0, tradesBuf),
		candles:                   make(map[string]*candlesBuffer),
		tickersStream:             make(chan []storage.Ticker, 1),
		tradesStream:              make(chan []storage.Trade, 1),
		candlesStream:             make(chan []storage.Candle, 1),
		tickersBuffer:             tickersBuf,
		tradesBuffer:              tradesBuf,
		candlesBuffer:             candlesBuf,
		candlesPreBufferThreshold: 2,
	}
}

func (s *Storage) Start(errGroup *errgroup.Group) {
	errGroup.Go(s.tickersToStore)
	errGroup.Go(s.tradesToStore)
	errGroup.Go(s.candlesToStore)
}

func (s *Storage) AppendTicker(ticker storage.Ticker) {
	s.tickers[s.tickersCount] = ticker
	s.tickersCount++
	if s.tickersCount == s.tickersBuffer {
		tickers := s.tickers
		s.tickersStream <- tickers

		s.tickersCount = 0
		s.tickers = make([]storage.Ticker, 0, s.tickersBuffer)
	}

	return
}

func (s *Storage) AppendTrade(trade storage.Trade) {
	s.trades[s.tradesCount] = trade
	s.tradesCount++
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
	market, timestamp := candle.MktID, candle.Timestamp

	buf := s.getCandleBuffer(market)

	idx, ok := buf.preBufferedIdxs[timestamp]
	if !ok {
		if buf.preBufferedCount != 0 {
			lastCandle := buf.preBuffered[buf.preBufferedCount-1]
			gap := candle.Timestamp.Sub(lastCandle.Timestamp)

			for i := 1; i < int(gap/time.Minute); i++ {
				ts := lastCandle.Timestamp.Add(time.Duration(i) * time.Minute)

				buf.preBuffered = append(
					buf.preBuffered,
					storage.Candle{
						Exchange:      lastCandle.Exchange,
						MktID:         lastCandle.MktID,
						MktCommitName: lastCandle.MktCommitName,
						Open:          lastCandle.Close,
						High:          lastCandle.Close,
						Low:           lastCandle.Close,
						Close:         lastCandle.Close,
						Volume:        0,
						Timestamp:     ts,
					},
				)
				buf.preBufferedIdxs[ts] = buf.preBufferedCount
				buf.preBufferedCount++
			}
		}

		buf.preBuffered = append(buf.preBuffered, candle)
		buf.preBufferedIdxs[timestamp] = buf.preBufferedCount
		buf.preBufferedCount++
	} else {
		buf.preBuffered[idx] = candle
	}

	if buf.preBufferedCount > 2 {
		count := buf.preBufferedCount - 2
		candles := buf.preBuffered[:count]
		buf.preBuffered = buf.preBuffered[count:]

		buf.preBufferedCount = 2
		buf.count += count

		for _, c := range candles {
			buf.buffer = append(buf.buffer, c)
			delete(buf.preBufferedIdxs, c.Timestamp)
		}

		for key := range buf.preBufferedIdxs {
			buf.preBufferedIdxs[key] -= count
		}
	}

	if buf.count >= s.candlesBuffer {
		candles := buf.buffer
		s.candlesStream <- candles

		buf.count = 0
		buf.buffer = make([]storage.Candle, 0, s.candlesBuffer)
	}
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
			log.Info().
				Str("exchange", data[0].Exchange).
				Str("market", data[0].MktID).
				Timestamp().
				Msg("committed candles")
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}
