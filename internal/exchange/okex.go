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

type okex struct {
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
	Event string         `json:"event"`
	Arg   wsSubChanOkex  `json:"arg"`
	Data  []respDataOkex `json:"data"`
	Code  string         `json:"code"`
	Msg   string         `json:"msg"`
}

type respDataOkex struct {
	TradeID     string `json:"tradeId"`
	Side        string `json:"side"`
	Size        string `json:"sz"`
	TickerPrice string `json:"last"`
	TradePrice  string `json:"px"`
	Timestamp   string `json:"ts"`
}

func NewOKEx(wrapper *Wrapper) *okex {
	return &okex{wrapper: wrapper}
}

func (e *okex) postConnectWs() error { return nil }

func (e *okex) pingWs(ctx context.Context) error {
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

func (e *okex) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (e *okex) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	switch channel {
	case "ticker":
		channel = "tickers"
	case "trade":
		channel = "trades"
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

func (e *okex) processWs(frame []byte) (err error) {
	var market, channel string

	wr := respOkex{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		if string(frame) == "pong" {
			return
		}
		logErrStack(err)
		return
	}

	switch wr.Event {
	case "error":
		log.Error().
			Str("exchange", "okex").
			Str("func", "readWs").
			Str("code", wr.Code).
			Str("msg", wr.Msg).
			Msg("")
		return errors.New("okex websocket error")
	case "subscribe":
		if wr.Arg.Channel == "tickers" {
			log.Debug().
				Str("exchange", "okex").
				Str("func", "readWs").
				Str("market", wr.Arg.InstID).
				Str("channel", "ticker").
				Msg("channel subscribed")
		} else {
			log.Debug().
				Str("exchange", "okex").
				Str("func", "readWs").
				Str("market", wr.Arg.InstID).
				Str("channel", "trade").
				Msg("channel subscribed")
		}
		return
	}

	if len(wr.Data) == 0 {
		return
	}

	if wr.Arg.Channel == "tickers" {
		channel = "ticker"
	} else {
		channel = "trade"
	}

	cfg, _, updateRequired := e.wrapper.getCfgMap(market, channel)
	if !updateRequired {
		return
	}

	switch wr.Arg.Channel {
	case "ticker":
		ticker := storage.Ticker{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
		}

		ticker.Price, err = strconv.ParseFloat(wr.Data[0].TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}

		t, err := strconv.ParseInt(wr.Data[0].Timestamp, 10, 0)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Timestamp = time.Unix(0, t*int64(time.Millisecond)).UTC()

		if cfg.influxStr {
			ticker.InfluxVal = e.wrapper.getTickerInfluxTime(cfg.mktCommitName)
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		var err error
		for _, data := range wr.Data {
			trade := storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				TradeID: data.TradeID,
				Side: data.Side,
			}

			trade.Size, err = strconv.ParseFloat(data.Size, 64)
			if err != nil {
				logErrStack(err)
				continue
			}

			trade.Price, err = strconv.ParseFloat(data.TradePrice, 64)
			if err != nil {
				logErrStack(err)
				continue
			}

			t, err := strconv.ParseInt(data.Timestamp, 10, 0)
			if err != nil {
				logErrStack(err)
				continue
			}
			trade.Timestamp = time.Unix(0, t*int64(time.Millisecond)).UTC()

			if cfg.influxStr {
				trade.InfluxVal = e.wrapper.getTradeInfluxTime(cfg.mktCommitName)
			}

			if err = e.wrapper.appendTrade(trade, cfg); err != nil {
				logErrStack(err)
			}
		}
	}

	return
}

func (e *okex) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restUrl = e.wrapper.config.RestUrl

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

func (e *okex) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := respOkex{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price, err = strconv.ParseFloat(rr.Data[0].TickerPrice, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *okex) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	rr := respOkex{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr.Data {
		var err error
		r := rr.Data[i]

		trade := storage.Trade{
			TradeID:       r.TradeID,
			Side:          r.Side,
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

	return
}
