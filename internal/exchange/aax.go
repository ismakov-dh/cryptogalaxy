package exchange

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
)

type aax struct {
	wrapper *Wrapper
}

type respAAX struct {
	Event       string `json:"e"`
	TradeID     string `json:"i"`
	Size        string `json:"q"`
	TickerPrice string `json:"c"`
	TradePrice  string `json:"p"`
	Timestamp   int64  `json:"t"`
}

type restTradeRespAAX struct {
	Event  string    `json:"e"`
	Trades []respAAX `json:"trades"`
}

func NewAAX(wrapper *Wrapper) *aax {
	return &aax{wrapper: wrapper}
}

func (e *aax) postConnectWs() error { return nil }

func (e *aax) pingWs(_ context.Context) error { return nil }

func (e *aax) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *aax) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "ticker" {
		channel = "1m_candles"
	}
	frame, err = jsoniter.Marshal(map[string]string{
		"e":      "subscribe",
		"stream": market + "@" + channel,
	})
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *aax) processWs(frame []byte) (err error) {
	var market, channel string

	wr := respAAX{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	switch wr.Event {
	case "empty", "system", "error", "reply":
		return
	default:
		s := strings.Split(wr.Event, "@")
		market = s[0]
		if s[1] == "1m_candles" {
			channel = "ticker"
		} else {
			channel = "trade"
		}
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

		ticker.Price, err = strconv.ParseFloat(wr.TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		trade := storage.Trade{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
			TradeID:       wr.TradeID,
			Timestamp:     time.Unix(0, wr.Timestamp*int64(time.Millisecond)).UTC(),
		}

		trade.Size, err = strconv.ParseFloat(wr.Size, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		trade.Price, err = strconv.ParseFloat(wr.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		if trade.Price >= 0 {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}
		trade.Price = math.Abs(trade.Price)

		err = e.wrapper.appendTrade(trade, cfg)
	}

	return
}

func (e *aax) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestUrl+"market/candles")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
		q.Add("timeFrame", "1m")
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestUrl+"market/trades")
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

	return
}

func (e *aax) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := respAAX{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	if rr.Event == "empty" || rr.Event == "system" || rr.Event == "error" {
		return price, fmt.Errorf("received aax event %s", rr.Event)
	}

	price, err = strconv.ParseFloat(rr.TickerPrice, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *aax) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	rr := restTradeRespAAX{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	if rr.Event == "empty" || rr.Event == "system" || rr.Event == "error" {
		return trades, fmt.Errorf("received aax event %s", rr.Event)
	}

	for i := range rr.Trades {
		var err error
		r := rr.Trades[i]

		trade := storage.Trade{
			TradeID:   r.TradeID,
			Timestamp: time.Unix(0, r.Timestamp*int64(time.Millisecond)).UTC(),
		}

		trade.Size, err = strconv.ParseFloat(r.Size, 64)
		if err != nil {
			logErrStack(err)
			err = nil
			continue
		}

		trade.Price, err = strconv.ParseFloat(r.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			err = nil
			continue
		}

		if trade.Price >= 0 {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}
		trade.Price = math.Abs(trade.Price)

		trades = append(trades, trade)
	}

	return
}
