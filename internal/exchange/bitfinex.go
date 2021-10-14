package exchange

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type bitfinex struct {
	wrapper *Wrapper
}

type respBitfinex []interface{}

type wsEventRespBitfinex struct {
	Event     string `json:"event"`
	Channel   string `json:"channel"`
	ChannelID int    `json:"chanId"`
	Symbol    string `json:"symbol"`
	Code      int    `json:"code"`
	Msg       string `json:"msg"`
	Version   int    `json:"version"`
	Platform  struct {
		Status int `json:"status"`
	} `json:"platform"`
}

func NewBitfinex(wrapper *Wrapper) *bitfinex {
	return &bitfinex{wrapper: wrapper}
}

func (e *bitfinex) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "trade" {
		channel = "trades"
	}
	market = "t" + strings.ToUpper(market)

	frame, err = jsoniter.Marshal(map[string]string{
		"event":   "subscribe",
		"channel": channel,
		"symbol":  market,
	})
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *bitfinex) postConnectWs() error { return nil }

func (e *bitfinex) pingWs(_ context.Context) error { return nil }

func (e *bitfinex) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *bitfinex) processWs(frame []byte) (err error) {
	var market, channel string
	var wri respBitfinex

	// Need to differentiate event and data responses.
	temp := bytes.TrimLeftFunc(frame, unicode.IsSpace)
	if bytes.HasPrefix(temp, []byte("{")) {
		wr := wsEventRespBitfinex{}
		err = jsoniter.Unmarshal(frame, &wr)
		if err != nil {
			logErrStack(err)
			return
		}

		switch wr.Event {
		case "hb":
		case "subscribed":
			market = wr.Symbol[1:]
			if wr.Channel == "trades" {
				channel = "trade"
			}

			e.wrapper.channelIds[wr.ChannelID] = [2]string{market, channel}

			log.Debug().Str("exchange", "bitfinex").Str("func", "readWs").Str("market", market).Str("channel", channel).Msg("channel subscribed")
		case "error":
			log.Error().Str("exchange", "bitfinex").Str("func", "readWs").Int("code", wr.Code).Str("msg", wr.Msg).Msg("")
			err = errors.New("bitfinex websocket error")
			return
		case "info":
			if wr.Code != 0 {
				log.Info().Str("exchange", "bitfinex").Str("func", "readWs").Int("code", wr.Code).Str("msg", wr.Msg).Msg("info received")
			} else if wr.Version != 0 {
				log.Info().Str("exchange", "bitfinex").Str("func", "readWs").Int("version", wr.Version).Int("platform-status", wr.Platform.Status).Msg("info received")
			}
		}
	} else if bytes.HasPrefix(temp, []byte("[")) {
		wr := respBitfinex{}
		err = jsoniter.Unmarshal(frame, &wr)
		if err != nil {
			logErrStack(err)
			return
		}

		if chanID, ok := wr[0].(float64); ok {
			ch := e.wrapper.channelIds[int(chanID)]

			market, channel = ch[0], ch[1]

			switch data := wr[1].(type) {
			case string:
				if data != "te" {
					return
				}
				if wsData, ok := wr[2].([]interface{}); ok {
					wri = wsData
				} else {
					log.Error().Str("exchange", "bitfinex").Str("func", "readWs").Interface("data", wr[2]).Msg("")
					err = errors.New("cannot convert frame data to []interface{}")
					return
				}
			case []interface{}:
				if channel != "ticker" {
					return
				}
				wri = data
			}
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "readWs").Interface("channel id", wr[0]).Msg("")
			err = errors.New("cannot convert frame data field channel id to float")
			return
		}

		cfg, _, updateRequired := e.wrapper.getCfgMap(market, channel)
		if !updateRequired {
			return
		}

		switch channel {
		case "ticker":
			ticker := storage.Ticker{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				Timestamp:     time.Now().UTC(),
			}

			if price, ok := wri[6].(float64); ok {
				ticker.Price = price
			} else {
				log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("price", wri[6]).Msg("")
				err = errors.New("cannot convert ticker data field price to float")
				return
			}

			if cfg.influxStr {
				ticker.InfluxVal = e.wrapper.getTickerInfluxTime(cfg.mktCommitName)
			}

			err = e.wrapper.appendTicker(ticker, cfg)
		case "trade":
			trade := storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
			}

			if tradeID, ok := wri[0].(float64); ok {
				trade.TradeID = strconv.FormatFloat(tradeID, 'f', 0, 64)
			} else {
				log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("trade id", wri[0]).Msg("")
				err = errors.New("cannot convert trade data field trade id to float")
				return
			}

			if size, ok := wri[2].(float64); ok {
				if size > 0 {
					trade.Side = "buy"
				} else {
					trade.Side = "sell"
				}
				trade.Size = math.Abs(size)
			} else {
				log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("size", wri[2]).Msg("")
				err = errors.New("cannot convert trade data field size to float")
				return
			}

			if price, ok := wri[3].(float64); ok {
				trade.Price = price
			} else {
				log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("price", wri[3]).Msg("")
				err = errors.New("cannot convert trade data field price to float")
				return
			}

			if timestamp, ok := wri[1].(float64); ok {
				trade.Timestamp = time.Unix(0, int64(timestamp)*int64(time.Millisecond)).UTC()
			} else {
				log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("timestamp", wri[1]).Msg("")
				err = errors.New("cannot convert trade data field timestamp to float")
				return
			}

			if cfg.influxStr {
				trade.InfluxVal = e.wrapper.getTradeInfluxTime(cfg.mktCommitName)
			}

			err = e.wrapper.appendTrade(trade, cfg)
		}
	}
	return
}

func (e *bitfinex) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"ticker/t"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"trades/t"+mktID+"/hist")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("limit", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *bitfinex) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := respBitfinex{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	var ok bool
	price, ok = rr[6].(float64)
	if !ok {
		log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("price", rr[6]).Msg("")
		err = errors.New("cannot convert ticker data field price to float")
	}

	return
}

func (e *bitfinex) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	var rr []respBitfinex

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr {
		r := rr[i]

		tradeID, ok := r[0].(float64)
		if !ok {
			log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("trade id", r[0]).Msg("")
			continue
		}

		size, ok := r[2].(float64)
		if !ok {
			log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("size", r[2]).Msg("")
			continue
		}
		var side string
		if size > 0 {
			side = "buy"
		} else {
			side = "sell"
		}
		size = math.Abs(size)

		price, ok := r[3].(float64)
		if !ok {
			log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("price", r[3]).Msg("")
			continue
		}

		timestamp, ok := r[1].(float64)
		if !ok {
			log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("timestamp", r[1]).Msg("")
			continue
		}

		trade := storage.Trade{
			TradeID:       strconv.FormatFloat(tradeID, 'f', 0, 64),
			Side:          side,
			Size:          size,
			Price:         price,
			Timestamp:     time.Unix(0, int64(timestamp)*int64(time.Millisecond)).UTC(),
		}

		trades = append(trades, trade)
	}

	return
}
