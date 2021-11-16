package exchange

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type probit struct {
	wrapper *Wrapper
}

type wsSubProbit struct {
	Channel  string    `json:"channel"`
	Filter   [1]string `json:"args"`
	Interval int       `json:"interval"`
	MarketID string    `json:"market_id"`
	Type     string    `json:"type"`
}

type wsRespProbit struct {
	Channel    string           `json:"channel"`
	MarketID   string           `json:"market_id"`
	Status     string           `json:"status"`
	TickerData respDataProbit   `json:"ticker"`
	TradeData  []respDataProbit `json:"recent_trades"`
}

type restRespProbit struct {
	Data []respDataProbit `json:"data"`
}

type respDataProbit struct {
	Side        string    `json:"side"`
	Quantity    string    `json:"quantity"`
	TickerPrice string    `json:"last"`
	TradePrice  string    `json:"price"`
	Time        time.Time `json:"time"`
}

func NewProbit(wrapper *Wrapper) *probit {
	return &probit{wrapper: wrapper}
}

func (e *probit) postConnectWs() error { return nil }

func (e *probit) pingWs(_ context.Context) error { return nil }

func (e *probit) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *probit) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "trade" {
		channel = "recent_trades"
	}
	sub := wsSubProbit{
		Channel:  "marketdata",
		Filter:   [1]string{channel},
		Interval: 100,
		MarketID: market,
		Type:     "subscribe",
	}

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *probit) processWs(frame []byte) (err error) {
	var market, channel string

	log.Debug().Str("exchange", "probit").
		Str("func", "processWs").
		Msg("unlike other exchanges probit does not send channel subscribed success message")

	wr := wsRespProbit{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.Status != "ok" {
		log.Error().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("msg", wr.Status).
			Msg("")
		return errors.New("probit websocket error")
	}

	if wr.TickerData.TickerPrice != "" {
		channel = "ticker"
	} else if len(wr.TradeData) > 0 {
		channel = "trade"
	}
	market = wr.MarketID

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
			Timestamp:     wr.TickerData.Time,
		}

		ticker.Price, err = strconv.ParseFloat(wr.TickerData.TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		var err error
		for _, data := range wr.TradeData {
			trade := storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				Side:          data.Side,
				Timestamp:     data.Time,
			}

			trade.Size, err = strconv.ParseFloat(data.Quantity, 64)
			if err != nil {
				logErrStack(err)
				continue
			}

			trade.Price, err = strconv.ParseFloat(data.TradePrice, 64)
			if err != nil {
				logErrStack(err)
				continue
			}

			if err = e.wrapper.appendTrade(trade, cfg); err != nil {
				logErrStack(err)
			}
		}
	}

	return
}

func (e *probit) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restUrl = e.wrapper.exchangeCfg().RestUrl

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("market_ids", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"trade")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("market_id", mktID)
		q.Add("limit", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *probit) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespProbit{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	r := rr.Data[0]

	price, err = strconv.ParseFloat(r.TickerPrice, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *probit) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	rr := restRespProbit{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr.Data {
		var err error
		r := rr.Data[i]

		trade := storage.Trade{
			Side:      r.Side,
			Timestamp: r.Time,
		}

		trade.Size, err = strconv.ParseFloat(r.Quantity, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		trade.Price, err = strconv.ParseFloat(r.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		trades = append(trades, trade)
	}

	return
}
