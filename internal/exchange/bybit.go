package exchange

import (
	"context"
	"io"
	"net"
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

type Bybit struct {
	wrapper *Wrapper
}

type wsSubBybit struct {
	Op   string    `json:"op"`
	Args [1]string `json:"args"`
}

type wsRespBybit struct {
	Success bool                `json:"success"`
	RetMsg  string              `json:"ret_msg"`
	Request wsSubBybit          `json:"request"`
	Topic   string              `json:"topic"`
	Data    jsoniter.RawMessage `json:"data"`
}

type wsRespUpdateBybit struct {
	Update []wsRespDataBybit `json:"update"`
}

type wsRespDataBybit struct {
	TradeID     string  `json:"trade_id"`
	Side        string  `json:"side"`
	Size        float64 `json:"size"`
	TickerPrice string  `json:"index_price_e4"`
	TradePrice  string  `json:"price"`
	Time        string  `json:"trade_time_ms"`
}

type restRespBybit struct {
	Result []restRespDataBybit `json:"result"`
}

type restRespDataBybit struct {
	Side        string    `json:"side"`
	Size        float64   `json:"qty"`
	TickerPrice string    `json:"last_price"`
	TradePrice  float64   `json:"price"`
	Time        time.Time `json:"time"`
}

func NewBybit(wrapper *Wrapper) *Bybit {
	return &Bybit{wrapper: wrapper}
}

func (e *Bybit) postConnectWs() error { return nil }

func (e *Bybit) pingWs(ctx context.Context) error {
	tick := time.NewTicker(54 * time.Second)
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

func (e *Bybit) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *Bybit) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "ticker" {
		channel = "instrument_info.100ms." + market
	} else {
		channel = "trade." + market
	}
	sub := wsSubBybit{
		Op:   "subscribe",
		Args: [1]string{channel},
	}

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *Bybit) processWs(frame []byte) (err error) {
	var market, channel string
	wr := wsRespBybit{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.Success {
		if wr.RetMsg == "pong" {
		} else {
			s := strings.Split(wr.Request.Args[0], ".")
			if s[0] == "instrument_info" {
				channel = "ticker"
				market = s[2]
			} else {
				channel = "trade"
				market = s[1]
			}
			log.Debug().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Str("market", market).
				Str("channel", channel).
				Msg("channel subscribed")
		}
		return
	}

	if wr.RetMsg != "" {
		log.Error().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("msg", wr.RetMsg).
			Msg("")
		return errors.New("bybit websocket error")
	}

	s := strings.Split(wr.Topic, ".")
	if s[0] == "instrument_info" {
		channel = "ticker"
		market = s[2]
	} else {
		channel = "trade"
		market = s[1]
	}

	cfg, _, updateRequired := e.wrapper.getCfgMap(market, channel)
	if !updateRequired {
		return
	}

	switch channel {
	case "ticker":
		data := wsRespUpdateBybit{}
		if err = jsoniter.Unmarshal(wr.Data, &data); err != nil {
			logErrStack(err)
			return
		}

		ticker := &storage.Ticker{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
			Timestamp:     time.Now().UTC(),
		}

		if len(data.Update) < 1 || data.Update[0].TickerPrice == "" {
			return nil
		}

		var price float64
		price, err = strconv.ParseFloat(data.Update[0].TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return
		}
		ticker.Price = price / 10000

		e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		var dataResp []wsRespDataBybit

		if err = jsoniter.Unmarshal(wr.Data, &dataResp); err != nil {
			logErrStack(err)
			return err
		}

		var err error
		for _, data := range dataResp {
			trade := &storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				TradeID:       data.TradeID,
				Size:          data.Size,
			}

			if data.Side == "Buy" {
				trade.Side = "buy"
			} else {
				trade.Side = "sell"
			}

			trade.Price, err = strconv.ParseFloat(data.TradePrice, 64)
			if err != nil {
				logErrStack(err)
				continue
			}

			var timestamp int64
			timestamp, err = strconv.ParseInt(data.Time, 10, 64)
			if err != nil {
				logErrStack(err)
				continue
			}
			trade.Timestamp = time.Unix(0, timestamp*int64(time.Millisecond)).UTC()

			e.wrapper.appendTrade(trade, cfg)
		}
	}

	return err
}

func (e *Bybit) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restUrl = e.wrapper.exchangeCfg().RestURL

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"v2/public/tickers")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"public/linear/recent-trading-records")
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

func (e *Bybit) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespBybit{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	r := rr.Result[0]

	price, err = strconv.ParseFloat(r.TickerPrice, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *Bybit) processRestTrade(body io.ReadCloser) (trades []*storage.Trade, err error) {
	rr := restRespBybit{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr.Result {
		r := rr.Result[i]

		trade := &storage.Trade{
			Size:      r.Size,
			Price:     r.TradePrice,
			Timestamp: r.Time,
		}
		if r.Side == "Buy" {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}

		trades = append(trades, trade)
	}

	return
}
