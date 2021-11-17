package exchange

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type HBTC struct {
	wrapper *Wrapper
}

type wsSubHbtc struct {
	Topic  string          `json:"topic"`
	Event  string          `json:"event"`
	Params wsSubParamsHbtc `json:"params"`
}

type wsSubParamsHbtc struct {
	Symbol string `json:"symbol"`
}

type wsRespHbtc struct {
	Pong   int64           `json:"pong"`
	Topic  string          `json:"topic"`
	Event  string          `json:"event"`
	Params wsSubParamsHbtc `json:"params"`
	Data   wsRespDataHbtc  `json:"data"`
	Code   string          `json:"code"`
	Msg    string          `json:"msg"`
}

type wsRespDataHbtc struct {
	Qty         string              `json:"q"`
	TickerPrice string              `json:"c"`
	TradePrice  string              `json:"p"`
	Time        int64               `json:"t"`
	Maker       jsoniter.RawMessage `json:"m"`
}

type restRespHbtc struct {
	Maker bool   `json:"isBuyerMaker"`
	Qty   string `json:"qty"`
	Price string `json:"price"`
	Time  int64  `json:"time"`
}

func NewHbtc(wrapper *Wrapper) *HBTC {
	return &HBTC{wrapper: wrapper}
}

func (e *HBTC) postConnectWs() error { return nil }

// pingWs sends ping request to websocket server for every 4 minutes (~10% earlier to required 5 minutes on a safer side).
func (e *HBTC) pingWs(ctx context.Context) error {
	tick := time.NewTicker(4 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			frame, err := jsoniter.Marshal(map[string]int64{"ping": time.Now().Unix()})
			if err != nil {
				logErrStack(err)
				return err
			}
			err = e.wrapper.ws.Write(frame)
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					err = errors.New("context canceled")
				} else {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (e *HBTC) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *HBTC) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "ticker" {
		channel = "realtimes"
	}
	sub := wsSubHbtc{
		Topic: channel,
		Event: "sub",
		Params: wsSubParamsHbtc{
			Symbol: market,
		},
	}

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *HBTC) processWs(frame []byte) (err error) {
	var market, channel string

	wr := wsRespHbtc{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return err
	}

	if wr.Pong > 0 {
		return
	}

	if wr.Topic == "realtimes" {
		channel = "ticker"
	}
	market = wr.Params.Symbol

	if wr.Msg == "Success" && wr.Event == "sub" {
		log.Debug().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("market", wr.Params.Symbol).
			Str("channel", wr.Topic).
			Msg("channel subscribed")
		return
	}

	cfg, _, updateRequired := e.wrapper.getCfgMap(market, channel)
	if !updateRequired {
		return
	}

	switch channel {
	case "ticker":
		ticker := &storage.Ticker{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
			Timestamp:     time.Unix(0, wr.Data.Time*int64(time.Millisecond)).UTC(),
		}

		ticker.Price, err = strconv.ParseFloat(wr.Data.TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		trade := &storage.Trade{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
			Timestamp:     time.Unix(0, wr.Data.Time*int64(time.Millisecond)).UTC(),
		}

		var maker bool
		maker, err = strconv.ParseBool(string(wr.Data.Maker))
		if err != nil {
			logErrStack(err)
			return
		}
		if maker {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}

		trade.Size, err = strconv.ParseFloat(wr.Data.Qty, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		trade.Price, err = strconv.ParseFloat(wr.Data.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		e.wrapper.appendTrade(trade, cfg)
	}

	return err
}

func (e *HBTC) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restURL = e.wrapper.exchangeCfg().RestURL

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", restURL+"openapi/quote/v1/ticker/price")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", restURL+"openapi/quote/v1/trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
		q.Add("limit", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return req, err
}

func (e *HBTC) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespHbtc{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price, err = strconv.ParseFloat(rr.Price, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *HBTC) processRestTrade(body io.ReadCloser) (trades []*storage.Trade, err error) {
	var rr []restRespHbtc

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr {
		var err error
		r := rr[i]

		trade := &storage.Trade{
			Timestamp: time.Unix(0, r.Time*int64(time.Millisecond)).UTC(),
		}

		if r.Maker {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}

		trade.Size, err = strconv.ParseFloat(r.Qty, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		trade.Price, err = strconv.ParseFloat(r.Price, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		trades = append(trades, trade)
	}

	return trades, err
}
