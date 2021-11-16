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

type mexo struct {
	wrapper *Wrapper
}

type wsSubMexo struct {
	Symbol string          `json:"symbol"`
	Topic  string          `json:"topic"`
	Event  string          `json:"event"`
	Params wsSubParamsMexo `json:"params"`
}

type wsSubParamsMexo struct {
	Binary bool `json:"binary"`
}

type wsRespMexo struct {
	Symbol string           `json:"symbol"`
	Topic  string           `json:"topic"`
	First  bool             `json:"f"`
	Data   []wsRespDataMexo `json:"data"`
	Code   string           `json:"code"`
	Desc   string           `json:"desc"`
}

type wsRespDataMexo struct {
	TradeID     string      `json:"v"`
	Maker       interface{} `json:"m"`
	Size        string      `json:"q"`
	TickerPrice string      `json:"c"`
	TradePrice  string      `json:"p"`
	Timestamp   interface{} `json:"t"`
}

type restRespMexo struct {
	Maker bool   `json:"isBuyerMaker"`
	Qty   string `json:"qty"`
	Price string `json:"price"`
	Time  int64  `json:"time"`
}

func NewMexo(wrapper *Wrapper) *mexo {
	return &mexo{wrapper: wrapper}
}

func (e *mexo) postConnectWs() error { return nil }

func (e *mexo) pingWs(ctx context.Context) error {
	tick := time.NewTicker(1 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			ping := make(map[string]int64, 1)
			ping["ping"] = time.Now().UTC().Unix()
			frame, err := jsoniter.Marshal(ping)
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

func (e *mexo) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *mexo) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "ticker" {
		channel = "realtimes"
	}

	sub := wsSubMexo{
		Symbol: market,
		Topic:  channel,
		Event:  "sub",
	}
	sub.Params.Binary = false
	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *mexo) processWs(frame []byte) (err error) {
	var market, channel string
	log.Debug().
		Str("exchange", e.wrapper.name).
		Str("func", "processWs").
		Msg("unlike other exchanges mexo does not send channel subscribed success message")

	wr := wsRespMexo{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.Code != "" {
		log.Error().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("code", wr.Code).
			Str("msg", wr.Desc).
			Msg("")
		return errors.New("mexo websocket error")
	}

	if wr.Topic == "realtimes" {
		channel = "ticker"
	} else {
		channel = "trade"
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

		ticker.Price, err = strconv.ParseFloat(wr.Data[0].TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		var err error
		for _, data := range wr.Data {
			trade := storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				TradeID:       data.TradeID,
			}

			if maker, ok := data.Maker.(bool); ok {
				if maker {
					trade.Side = "buy"
				} else {
					trade.Side = "sell"
				}
			} else {
				log.Error().
					Str("exchange", e.wrapper.name).
					Str("func", "processWs").
					Interface("maker", data.Maker).
					Msg("cannot convert trade data field maker to bool")
				continue
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

			if timestamp, ok := data.Timestamp.(float64); ok {
				trade.Timestamp = time.Unix(0, int64(timestamp)*int64(time.Millisecond)).UTC()
			} else {
				log.Error().
					Str("exchange", e.wrapper.name).
					Str("func", "processWs").
					Interface("timestamp", data.Timestamp).
					Msg("cannot convert trade data field timestamp to float")
				continue
			}

			if err = e.wrapper.appendTrade(trade, cfg); err != nil {
				logErrStack(err)
			}
		}
	}

	return
}

func (e *mexo) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restUrl = e.wrapper.exchangeCfg().RestUrl

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"quote/v1/ticker/price")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"quote/v1/trades")
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

func (e *mexo) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespMexo{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
	}

	price, err = strconv.ParseFloat(rr.Price, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *mexo) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	var rr []restRespMexo

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr {
		var err error
		r := rr[i]

		trade := storage.Trade{
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

	return
}
