package exchange

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type gateio struct {
	wrapper *Wrapper
}

type wsSubGateio struct {
	Time    int64     `json:"time"`
	ID      int       `json:"id"`
	Channel string    `json:"channel"`
	Event   string    `json:"event"`
	Payload [1]string `json:"payload"`
}

type wsSubErrGateio struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type wsRespGateio struct {
	Channel    string      `json:"channel"`
	Result     respGateio  `json:"result"`
	TickerTime int64       `json:"time"`
	ID         int         `json:"id"`
	Error      interface{} `json:"error"`
}

type respGateio struct {
	CurrencyPair string      `json:"currency_pair"`
	TradeID      interface{} `json:"id"`
	Side         string      `json:"side"`
	Amount       string      `json:"amount"`
	TickerPrice  string      `json:"last"`
	TradePrice   string      `json:"price"`
	CreateTimeMs string      `json:"create_time_ms"`
	Status       string      `json:"status"`
}

func NewGateio(wrapper *Wrapper) *gateio {
	return &gateio{wrapper: wrapper}
}

func (e *gateio) postConnectWs() error { return nil }

func (e *gateio) pingWs(_ context.Context) error { return nil }

func (e *gateio) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *gateio) getWsSubscribeMessage(market string, channel string, id int) (frame []byte, err error) {
	switch channel {
	case "ticker":
		channel = "spot.tickers"
	case "trade":
		channel = "spot.trades"
	}
	sub := wsSubGateio{
		Time:    time.Now().UTC().Unix(),
		ID:      id,
		Channel: channel,
		Event:   "subscribe",
		Payload: [1]string{market},
	}

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *gateio) processWs(frame []byte) (err error) {
	var market, channel string

	wr := wsRespGateio{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.ID != 0 {
		if wr.Result.Status == "success" {
			log.Debug().Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Str("market", e.wrapper.channelIds[wr.ID][0]).
				Str("channel", e.wrapper.channelIds[wr.ID][1]).
				Msg("channel subscribed")
			return
		} else {
			wsSubErr, ok := wr.Error.(wsSubErrGateio)
			if !ok {
				log.Error().Str("exchange", e.wrapper.name).
					Str("func", "processWs").
					Interface("error", wr.Error).
					Msg("")
				return errors.New("cannot convert ws resp error to error field")
			}
			log.Error().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Int("code", wsSubErr.Code).
				Str("msg", wsSubErr.Message).
				Msg("")
			return errors.New("gateio websocket error")
		}
	}

	if wr.Channel == "spot.tickers" {
		channel = "ticker"
	} else {
		channel = "trade"
	}
	market = wr.Result.CurrencyPair

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
			Timestamp:     time.Unix(wr.TickerTime, 0).UTC(),
		}

		ticker.Price, err = strconv.ParseFloat(wr.Result.TickerPrice, 64)
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
			Side:          wr.Result.Side,
		}

		// Trade ID sent is in int format for websocket, string format for REST.
		if tradeIDFloat, ok := wr.Result.TradeID.(float64); ok {
			trade.TradeID = strconv.FormatFloat(tradeIDFloat, 'f', 0, 64)
		} else {
			log.Error().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Interface("trade id", wr.Result.TradeID).
				Msg("")
			return errors.New("cannot convert trade data field trade id to float")
		}

		trade.Size, err = strconv.ParseFloat(wr.Result.Amount, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		trade.Price, err = strconv.ParseFloat(wr.Result.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		var timeFloat float64
		timeFloat, err = strconv.ParseFloat(wr.Result.CreateTimeMs, 64)
		if err != nil {
			logErrStack(err)
			return
		}
		intPart, _ := math.Modf(timeFloat)
		trade.Timestamp = time.Unix(0, int64(intPart)*int64(time.Millisecond)).UTC()

		err = e.wrapper.appendTrade(trade, cfg)
	}

	return
}

func (e *gateio) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restUrl = e.wrapper.exchangeCfg().RestUrl

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"spot/tickers")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("currency_pair", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"spot/trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("currency_pair", mktID)
		q.Add("limit", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *gateio) processRestTicker(body io.ReadCloser) (price float64, err error) {
	var r []respGateio

	if err = jsoniter.NewDecoder(body).Decode(&r); err != nil {
		logErrStack(err)
		return
	}

	rr := r[0]

	price, err = strconv.ParseFloat(rr.TickerPrice, 64)
	if err != nil {
		logErrStack(err)
		return
	}

	return
}

func (e *gateio) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	var rr []respGateio

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr {
		var err error
		r := rr[i]

		trade := storage.Trade{
			Side: r.Side,
		}

		// Trade ID sent is in int format for websocket, string format for REST.
		tradeID, ok := r.TradeID.(string)
		if !ok {
			log.Error().
				Str("exchange", e.wrapper.name).
				Str("func", "processREST").
				Interface("trade id", r.TradeID).
				Msg("cannot convert trade data field trade id to string")
			continue
		}
		trade.TradeID = tradeID

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

		var timeFloat float64
		timeFloat, err = strconv.ParseFloat(r.CreateTimeMs, 64)
		if err != nil {
			logErrStack(err)
			continue
		}
		intPart, _ := math.Modf(timeFloat)
		trade.Timestamp = time.Unix(0, int64(intPart)*int64(time.Millisecond)).UTC()

		trades = append(trades, trade)
	}

	return
}
