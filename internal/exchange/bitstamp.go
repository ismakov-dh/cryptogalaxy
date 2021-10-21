package exchange

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type bitstamp struct {
	wrapper *Wrapper
}

type wsRespBitstamp struct {
	Event         string             `json:"event"`
	Channel       string             `json:"channel"`
	Data          wsRespDataBitstamp `json:"data"`
	mktID         string
	mktCommitName string
}

type wsRespDataBitstamp struct {
	TradeID   uint64  `json:"id"`
	Type      int     `json:"type"`
	Amount    float64 `json:"amount"`
	Price     float64 `json:"price"`
	Timestamp string  `json:"microtimestamp"`
	Channel   string  `json:"channel"`
}

type restRespBitstamp struct {
	TradeID     string `json:"tid"`
	Type        string `json:"type"`
	Amount      string `json:"amount"`
	TickerPrice string `json:"last"`
	TradePrice  string `json:"price"`
	Timestamp   string `json:"date"`
}

func NewBitstamp(wrapper *Wrapper) *bitstamp {
	return &bitstamp{wrapper: wrapper}
}

func (e *bitstamp) postConnectWs() error { return nil }

func (e *bitstamp) pingWs(_ context.Context) error { return nil }

func (e *bitstamp) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *bitstamp) getWsSubscribeMessage(market string, _ string, _ int) (frame []byte, err error) {
	sub := wsRespBitstamp{
		Event: "bts:subscribe",
	}
	sub.Data.Channel = "live_trades_" + market
	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *bitstamp) processWs(frame []byte) (err error) {
	var market string

	wr := wsRespBitstamp{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	s := strings.Split(wr.Channel, "_")
	market = s[2]

	var timestamp int64
	timestamp, err = strconv.ParseInt(wr.Data.Timestamp, 10, 64)
	if err != nil {
		logErrStack(err)
		return
	}
	ts := time.Unix(0, timestamp*int64(time.Microsecond)).UTC()

	cfg, ok, updateRequired := e.wrapper.getCfgMap(market, "ticker")

	if ok && cfg.connector == "websocket" {
		if wr.Event == "bts:subscription_succeeded" {
			log.Debug().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Str("market", market).
				Str("channel", "ticker").
				Msg("channel subscribed")
			return
		}

		if !updateRequired {
			return
		}

		ticker := storage.Ticker{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
			Price:         wr.Data.Price,
			Timestamp:     ts,
		}

		if cfg.influxStr {
			ticker.InfluxVal = e.wrapper.getTickerInfluxTime(cfg.mktCommitName)
		}

		if err = e.wrapper.appendTicker(ticker, cfg); err != nil {
			logErrStack(err)
		}

	}

	cfg, ok, updateRequired = e.wrapper.getCfgMap(market, "trade")

	if ok && cfg.connector == "websocket" {
		if wr.Event == "bts:subscription_succeeded" {
			log.Debug().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Str("market", market).
				Str("channel", "trade").
				Msg("channel subscribed")
			return
		}

		if !updateRequired {
			return
		}

		trade := storage.Trade{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
			TradeID:       strconv.FormatUint(wr.Data.TradeID, 10),
			Size:          wr.Data.Amount,
			Price:         wr.Data.Price,
			Timestamp:     ts,
		}

		if wr.Data.Type == 0 {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}

		if cfg.influxStr {
			trade.InfluxVal = e.wrapper.getTradeInfluxTime(cfg.mktCommitName)
		}

		if err = e.wrapper.appendTrade(trade, cfg); err != nil {
			logErrStack(err)
		}
	}

	return
}

func (e *bitstamp) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"ticker/"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"transactions/"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("time", "minute")
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *bitstamp) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespBitstamp{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price, err = strconv.ParseFloat(rr.TickerPrice, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *bitstamp) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	var rr []restRespBitstamp

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr {
		var err error
		r := rr[i]

		trade := storage.Trade{
			TradeID: r.TradeID,
		}

		if r.Type == "0" {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}

		trade.Size, err = strconv.ParseFloat(r.Amount, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		trade.Price, err = strconv.ParseFloat(r.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		var timestamp int64
		timestamp, err = strconv.ParseInt(r.Timestamp, 10, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		trade.Timestamp = time.Unix(timestamp, 0).UTC()

		trades = append(trades, trade)
	}

	return
}
