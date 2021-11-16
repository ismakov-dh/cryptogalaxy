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

type Bitmart struct {
	wrapper *Wrapper
}

type wsSubBitmart struct {
	Op   string    `json:"op"`
	Args [1]string `json:"args"`
}

type wsRespBitmart struct {
	Table   string              `json:"table"`
	Data    []wsRespDataBitmart `json:"data"`
	ErrMsg  string              `json:"errorMessage"`
	ErrCode string              `json:"errorCode"`
}

type wsRespDataBitmart struct {
	Symbol      string `json:"symbol"`
	Side        string `json:"side"`
	Size        string `json:"size"`
	TickerPrice string `json:"last_price"`
	TradePrice  string `json:"price"`
	Timestamp   int64  `json:"s_t"`
}

type restRespBitmart struct {
	Data restRespDataBitmart `json:"data"`
}

type restRespDataBitmart struct {
	Tickers []restRespDataDetailBitmart `json:"tickers"`
	Trades  []restRespDataDetailBitmart `json:"trades"`
}

type restRespDataDetailBitmart struct {
	Type        string `json:"type"`
	Count       string `json:"count"`
	TickerPrice string `json:"last_price"`
	TradePrice  string `json:"price"`
	OrderTime   int64  `json:"order_time"`
}

func NewBitmart(wrapper *Wrapper) *Bitmart {
	return &Bitmart{wrapper: wrapper}
}

func (e *Bitmart) postConnectWs() (err error) { return nil }

func (e *Bitmart) pingWs(ctx context.Context) (err error) {
	tick := time.NewTicker(18 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			err = e.wrapper.ws.Write([]byte(`ping`))
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					err = errors.New("context canceled")
				} else {
					logErrStack(err)
				}
				return
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (e *Bitmart) readWs() (frame []byte, err error) {
	return e.wrapper.ws.ReadTextOrFlateBinary()
}

func (e *Bitmart) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "ticker" {
		channel = "spot/ticker:" + market
	} else {
		channel = "spot/trade:" + market
	}
	sub := wsSubBitmart{
		Op:   "subscribe",
		Args: [1]string{channel},
	}
	frame, err = jsoniter.Marshal(sub)

	return
}

func (e *Bitmart) processWs(frame []byte) (err error) {
	var market, channel string

	wr := wsRespBitmart{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.ErrCode != "" {
		log.Error().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("code", wr.ErrCode).
			Str("msg", wr.ErrMsg).
			Msg("")
		err = errors.New("bitmart websocket error")
		return
	}

	if wr.Table == "spot/ticker" {
		channel = "ticker"
	} else {
		channel = "trade"
	}

	data := wr.Data[0]

	market = data.Symbol

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
			Timestamp:     time.Unix(data.Timestamp, 0).UTC(),
		}

		ticker.Price, err = strconv.ParseFloat(data.TickerPrice, 64)
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
			Side:          data.Side,
			Timestamp:     time.Unix(data.Timestamp, 0).UTC(),
		}

		trade.Size, err = strconv.ParseFloat(data.Size, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		trade.Price, err = strconv.ParseFloat(data.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		e.wrapper.appendTrade(trade, cfg)
	}

	return err
}

func (e *Bitmart) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestURL+"ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestURL+"symbols/trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *Bitmart) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespBitmart{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	r := rr.Data.Tickers[0]

	price, err = strconv.ParseFloat(r.TickerPrice, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *Bitmart) processRestTrade(body io.ReadCloser) (trades []*storage.Trade, err error) {
	rr := restRespBitmart{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr.Data.Trades {
		var err error
		r := rr.Data.Trades[i]

		trade := &storage.Trade{
			Side:      r.Type,
			Timestamp: time.Unix(0, r.OrderTime*int64(time.Millisecond)).UTC(),
		}

		trade.Size, err = strconv.ParseFloat(r.Count, 64)
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

	return trades, err
}
