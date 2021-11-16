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

type btse struct {
	wrapper *Wrapper
}

type wsSubBTSE struct {
	Op   string    `json:"op"`
	Args [1]string `json:"args"`
}

type wsRespBTSE struct {
	Event   string         `json:"event"`
	Channel [1]string      `json:"channel"`
	Topic   string         `json:"topic"`
	Data    []respDataBTSE `json:"data"`
}

type respDataBTSE struct {
	RESTTradeID uint64  `json:"serialId"`
	WSTradeID   uint64  `json:"tradeId"`
	Side        string  `json:"side"`
	Size        float64 `json:"size"`
	TickerPrice float64 `json:"lastPrice"`
	TradePrice  float64 `json:"price"`
	Timestamp   int64   `json:"timestamp"`
}

func NewBtse(wrapper *Wrapper) *btse {
	return &btse{wrapper: wrapper}
}

func (e *btse) postConnectWs() error { return nil }

func (e *btse) pingWs(_ context.Context) error { return nil }

func (e *btse) readWs() (frame []byte, err error) {
	return e.wrapper.ws.Read()
}

func (e *btse) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	channel = "tradeHistoryApi:" + market
	sub := wsSubBTSE{
		Op:   "subscribe",
		Args: [1]string{channel},
	}

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *btse) processWs(frame []byte) (err error) {
	var market string

	wr := wsRespBTSE{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.Event == "subscribe" {
		s := strings.Split(wr.Channel[0], ":")
		market = s[1]

		// There is only one channel provided for both ticker and trade data.
		cfg, ok, _ := e.wrapper.getCfgMap(market, "ticker")
		if ok && cfg.connector == "websocket" {
			log.Debug().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Str("market", market).
				Str("channel", "ticker").
				Msg("channel subscribed")
		}

		cfg, ok, _ = e.wrapper.getCfgMap(market, "trade")
		if ok && cfg.connector == "websocket" {
			log.Debug().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Str("market", market).
				Str("channel", "trade").
				Msg("channel subscribed")
		}

		return
	}

	// API sends duplicate data, so filtering based on the lengte.
	if len(wr.Data) > 1 {
		return
	}

	// There is only one channel provided for both ticker and trade data,
	// so need to duplicate it manually if there is a subscription to both the channels by user.
	s := strings.Split(wr.Topic, ":")
	market = s[1]

	// Ticker.
	cfg, ok, updateRequired := e.wrapper.getCfgMap(market, "ticker")
	if ok && cfg.connector == "websocket" && updateRequired {
		ticker := storage.Ticker{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
			Price:         wr.Data[0].TradePrice,
			Timestamp:     time.Unix(0, wr.Data[0].Timestamp*int64(time.Millisecond)).UTC(),
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	}

	cfg, ok, updateRequired = e.wrapper.getCfgMap(market, "trade")
	if ok && cfg.connector == "websocket" && updateRequired {
		var err error
		for _, data := range wr.Data {
			trade := storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				TradeID:       strconv.FormatUint(data.WSTradeID, 10),
				Size:          data.Size,
				Price:         data.TradePrice,
				Timestamp:     time.Unix(0, wr.Data[0].Timestamp*int64(time.Millisecond)).UTC(),
			}

			if data.Side == "BUY" {
				trade.Side = "buy"
			} else {
				trade.Side = "sell"
			}

			err = e.wrapper.appendTrade(trade, cfg)
			if err != nil {
				logErrStack(err)
			}
		}
	}

	return
}

func (e *btse) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestUrl+"price")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestUrl+"trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
		q.Add("count", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *btse) processRestTicker(body io.ReadCloser) (price float64, err error) {
	var rr []respDataBTSE

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price = rr[0].TickerPrice

	return
}

func (e *btse) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	var rr []respDataBTSE

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr {
		r := rr[i]

		trade := storage.Trade{
			TradeID:   strconv.FormatUint(r.RESTTradeID, 10),
			Size:      r.Size,
			Price:     r.TradePrice,
			Timestamp: time.Unix(0, r.Timestamp*int64(time.Millisecond)).UTC(),
		}

		if r.Side == "BUY" {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}

		trades = append(trades, trade)
	}

	return
}
