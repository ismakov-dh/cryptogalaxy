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

type Okex struct {
	wrapper *Wrapper
}

type wsSubOkex struct {
	Op   string          `json:"op"`
	Args []wsSubChanOkex `json:"args"`
}

type wsSubChanOkex struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

type respOkex struct {
	Event string              `json:"event"`
	Arg   wsSubChanOkex       `json:"arg"`
	Data  jsoniter.RawMessage `json:"data"`
	Code  string              `json:"code"`
	Msg   string              `json:"msg"`
}

type respDataOkex struct {
	TradeID     string `json:"tradeId"`
	Side        string `json:"side"`
	Size        string `json:"sz"`
	TickerPrice string `json:"last"`
	TradePrice  string `json:"px"`
	Timestamp   string `json:"ts"`
}

func NewOKEx(wrapper *Wrapper) *Okex {
	return &Okex{wrapper: wrapper}
}

func (e *Okex) postConnectWs() error { return nil }

func (e *Okex) pingWs(ctx context.Context) error {
	tick := time.NewTicker(27 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			err := e.wrapper.ws.Write([]byte(`ping`))
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

func (e *Okex) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (e *Okex) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	switch channel {
	case "ticker":
		channel = "tickers"
	case "trade":
		channel = "trades"
	case "candle":
		channel = "candle1m"
	}
	channels := make([]wsSubChanOkex, 1)
	channels[0].Channel = channel
	channels[0].InstID = market
	sub := wsSubOkex{
		Op:   "subscribe",
		Args: channels,
	}

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *Okex) processWs(frame []byte) (err error) {
	var market, channel string

	wr := respOkex{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		if string(frame) == "pong" {
			return nil
		}
		logErrStack(err)
		return
	}

	market = wr.Arg.InstID
	switch wr.Arg.Channel {
	case "tickers":
		channel = "ticker"
	case "candle1m":
		channel = "candle"
	default:
		channel = "trade"
	}

	switch wr.Event {
	case "error":
		log.Error().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("code", wr.Code).
			Str("msg", wr.Msg).
			Msg("")
		return errors.New("okex websocket error")
	case "subscribe":
		log.Debug().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("market", market).
			Str("channel", channel).
			Msg("channel subscribed")
		return
	}

	if len(wr.Data) == 0 {
		return
	}

	cfg, _, updateRequired := e.wrapper.getCfgMap(market, channel)
	if !updateRequired {
		return
	}

	switch channel {
	case "ticker":
		var data []respDataOkex

		if err := jsoniter.Unmarshal(wr.Data, &data); err != nil {
			return err
		}

		ticker := &storage.Ticker{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
		}

		ticker.Price, err = strconv.ParseFloat(data[0].TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}

		t, err := strconv.ParseInt(data[0].Timestamp, 10, 0)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Timestamp = time.Unix(0, t*int64(time.Millisecond)).UTC()

		e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		var data []respDataOkex

		if err := jsoniter.Unmarshal(wr.Data, &data); err != nil {
			return err
		}

		var err error
		for _, t := range data {
			trade := &storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				TradeID:       t.TradeID,
				Side:          t.Side,
			}

			trade.Size, err = strconv.ParseFloat(t.Size, 64)
			if err != nil {
				logErrStack(err)
				continue
			}

			trade.Price, err = strconv.ParseFloat(t.TradePrice, 64)
			if err != nil {
				logErrStack(err)
				continue
			}

			t, err := strconv.ParseInt(t.Timestamp, 10, 0)
			if err != nil {
				logErrStack(err)
				continue
			}
			trade.Timestamp = time.Unix(0, t*int64(time.Millisecond)).UTC()

			e.wrapper.appendTrade(trade, cfg)
		}
	case "candle":
		var candles [][]string

		if err := jsoniter.Unmarshal(wr.Data, &candles); err != nil {
			return err
		}

		candle := &storage.Candle{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
		}

		candle.Open, err = strconv.ParseFloat(candles[0][1], 64)
		if err != nil {
			logErrStack(err)
			return
		}

		candle.High, err = strconv.ParseFloat(candles[0][2], 64)
		if err != nil {
			logErrStack(err)
			return
		}

		candle.Low, err = strconv.ParseFloat(candles[0][3], 64)
		if err != nil {
			logErrStack(err)
			return
		}

		candle.Close, err = strconv.ParseFloat(candles[0][4], 64)
		if err != nil {
			logErrStack(err)
			return
		}

		candle.Volume, err = strconv.ParseFloat(candles[0][5], 64)
		if err != nil {
			logErrStack(err)
			return
		}

		var t int64
		t, err = strconv.ParseInt(candles[0][0], 10, 64)
		if err != nil {
			logErrStack(err)
			return
		}
		candle.Timestamp = time.Unix(0, t*int64(time.Millisecond)).UTC()

		e.wrapper.appendCandle(candle, cfg)
	}

	return
}

func (e *Okex) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restUrl = e.wrapper.exchangeCfg().RestURL

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"market/ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("instId", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"market/trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("instId", mktID)
		q.Add("limit", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *Okex) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := respOkex{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	var data []respDataOkex

	if err = jsoniter.Unmarshal(rr.Data, data); err != nil {
		return
	}

	price, err = strconv.ParseFloat(data[0].TickerPrice, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *Okex) processRestTrade(body io.ReadCloser) (trades []*storage.Trade, err error) {
	rr := respOkex{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	var data []respDataOkex

	if err = jsoniter.Unmarshal(rr.Data, data); err != nil {
		return
	}

	for i := range data {
		var err error
		r := data[i]

		trade := &storage.Trade{
			TradeID: r.TradeID,
			Side:    r.Side,
		}

		trade.Size, err = strconv.ParseFloat(r.Size, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		trade.Price, err = strconv.ParseFloat(r.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		t, err := strconv.ParseInt(r.Timestamp, 10, 0)
		if err != nil {
			logErrStack(err)
			continue
		}
		trade.Timestamp = time.Unix(0, t*int64(time.Millisecond)).UTC()

		trades = append(trades, trade)
	}

	return trades, err
}
