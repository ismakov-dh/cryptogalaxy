package exchange

import (
	"context"
	"fmt"
	"io"
	"math"
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

type Ftx struct {
	wrapper *Wrapper
}

type wsRespFtx struct {
	Channel string              `json:"channel"`
	Market  string              `json:"market"`
	Type    string              `json:"type"`
	Code    int                 `json:"code"`
	Msg     string              `json:"msg"`
	Data    jsoniter.RawMessage `json:"data"`
}

type wsTickerRespDataFtx struct {
	Last float64 `json:"last"`
	Time float64 `json:"time"`
}

type wsTradeRespDataFtx struct {
	Side  string  `json:"side"`
	Size  float64 `json:"size"`
	Price float64 `json:"price"`
	Time  string  `json:"time"`
}

type restTickerRespFtx struct {
	Success bool              `json:"success"`
	Result  restRespResultFtx `json:"result"`
}

type restTradeRespFtx struct {
	Success bool                `json:"success"`
	Result  []restRespResultFtx `json:"result"`
}

type restRespResultFtx struct {
	Last  float64 `json:"last"`
	Side  string  `json:"side"`
	Size  float64 `json:"size"`
	Price float64 `json:"price"`
	Time  string  `json:"time"`
}

func NewFtx(wrapper *Wrapper) *Ftx {
	return &Ftx{wrapper: wrapper}
}

func (e *Ftx) postConnectWs() error { return nil }

func (e *Ftx) pingWs(ctx context.Context) error {
	tick := time.NewTicker(13 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			err := e.wrapper.ws.Write([]byte(`{"op":"ping"}`))
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

func (e *Ftx) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *Ftx) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "trade" {
		channel = "trades"
	}
	frame, err = jsoniter.Marshal(map[string]string{
		"op":      "subscribe",
		"market":  market,
		"channel": channel,
	})
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *Ftx) processWs(frame []byte) (err error) {
	var market, channel string
	wr := wsRespFtx{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return err
	}

	if wr.Channel == "trades" {
		channel = "trade"
	}
	market = wr.Market

	switch wr.Type {
	case "pong":
	case "error":
		log.Error().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Int("code", wr.Code).
			Str("msg", wr.Msg).
			Msg("")
		return fmt.Errorf("%s websocket error", e.wrapper.name)
	case "subscribed":
		log.Debug().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("market", wr.Market).
			Str("channel", channel).
			Msg("channel subscribed")
		return
	case "info":
		log.Info().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Int("code", wr.Code).
			Str("msg", wr.Msg).
			Msg("info received")
		return
	}

	cfg, _, updateRequired := e.wrapper.getCfgMap(market, channel)
	if !updateRequired {
		return
	}

	switch channel {
	case "ticket":
		data := wsTickerRespDataFtx{}
		if err = jsoniter.Unmarshal(wr.Data, &data); err != nil {
			logErrStack(err)
			return
		}

		ticker := &storage.Ticker{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
			Price:         data.Last,
		}

		intPart, fracPart := math.Modf(data.Time)
		ticker.Timestamp = time.Unix(int64(intPart), int64(fracPart*1e9)).UTC()

		e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		var dataResp []wsTradeRespDataFtx

		if err = jsoniter.Unmarshal(wr.Data, &dataResp); err != nil {
			logErrStack(err)
			return
		}

		var err error
		for _, data := range dataResp {
			trade := &storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				Side:          data.Side,
				Size:          data.Size,
				Price:         data.Price,
			}

			trade.Timestamp, err = time.Parse(time.RFC3339Nano, data.Time)
			if err != nil {
				logErrStack(err)
				continue
			}

			e.wrapper.appendTrade(trade, cfg)
		}
	}

	return err
}

func (e *Ftx) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restURL = e.wrapper.exchangeCfg().RestURL

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", restURL+"markets/"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", restURL+"markets/"+mktID+"/trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()

		// Querying for 100 trades, which is a max allowed for a request by the exchange.
		// If the configured interval gap is big, then maybe it will not return all the trades.
		// Better to use websocket.
		q.Add("limit", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return req, err
}

func (e *Ftx) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restTickerRespFtx{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price = rr.Result.Price

	return
}

func (e *Ftx) processRestTrade(body io.ReadCloser) (trades []*storage.Trade, err error) {
	rr := restTradeRespFtx{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr.Result {
		var err error
		r := rr.Result[i]

		trade := &storage.Trade{
			Side:  r.Side,
			Size:  r.Size,
			Price: r.Price,
		}

		trade.Timestamp, err = time.Parse(time.RFC3339Nano, r.Time)
		if err != nil {
			logErrStack(err)
			continue
		}

		trades = append(trades, trade)
	}

	return
}
