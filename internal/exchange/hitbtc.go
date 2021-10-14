package exchange

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type hitBTC struct {
	wrapper *Wrapper
}

type wsSubHitBTC struct {
	Method  string            `json:"method"`
	Channel string            `json:"ch"`
	Params  wsSubParamsHitBTC `json:"params"`
	ID      int               `json:"id"`
}

type wsSubParamsHitBTC struct {
	Symbols [1]string `json:"symbols"`
}

type wsRespHitBTC struct {
	Result  wsRespSubHitBTC                 `json:"result"`
	Channel string                          `json:"ch"`
	Data    map[string]wsRespDetailHitBTC   `json:"data"`
	Update  map[string][]wsRespDetailHitBTC `json:"update"`
}

type wsRespSubHitBTC struct {
	Channel string   `json:"ch"`
	Subs    []string `json:"subscriptions"`
}

type wsRespDetailHitBTC struct {
	TradeID     uint64 `json:"i"`
	Side        string `json:"s"`
	Qty         string `json:"q"`
	TickerPrice string `json:"c"`
	TradePrice  string `json:"p"`
	Timestamp   int64  `json:"t"`
}

type restRespHitBTC struct {
	TradeID     uint64    `json:"id"`
	Side        string    `json:"side"`
	Qty         string    `json:"qty"`
	TickerPrice string    `json:"last"`
	TradePrice  string    `json:"price"`
	Timestamp   time.Time `json:"timestamp"`
}

func NewHitBTC(wrapper *Wrapper) *hitBTC {
	return &hitBTC{wrapper: wrapper}
}

func (e *hitBTC) postConnectWs() error { return nil }

func (e *hitBTC) pingWs(_ context.Context) error { return nil }

func (e *hitBTC) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *hitBTC) getWsSubscribeMessage(market string, channel string, id int) (frame []byte, err error) {
	switch channel {
	case "ticker":
		channel = "ticker/price/1s"
	case "trade":
		channel = "trades"
	}
	sub := wsSubHitBTC{
		Method:  "subscribe",
		Channel: channel,
		ID:      id,
	}
	sub.Params.Symbols = [1]string{market}
	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *hitBTC) processWs(frame []byte) (err error) {
	var market, channel string

	wr := wsRespHitBTC{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return err
	}

	if wr.Result.Channel != "" {
		switch wr.Result.Channel {
		case "ticker/price/1s":
			wr.Result.Channel = "ticker"
		case "trades":
			wr.Result.Channel = "trade"
		}
		for _, sub := range wr.Result.Subs {
			log.Debug().
				Str("exchange", e.wrapper.name).
				Str("func", "readWs").
				Str("market", sub).
				Str("channel", wr.Result.Channel).
				Msg("channel subscribed (this message may be duplicate as server sends list of all subscriptions on each channel subscribe)")
		}
		return
	}

	switch wr.Channel {
	case "ticker/price/1s":
		channel = "ticker"
		for k := range wr.Data {
			market = k
			break
		}
	case "trades":
		channel = "trade"
		for k := range wr.Update {
			market = k
			break
		}
	}

	cfg, _, updateRequired := e.wrapper.getCfgMap(market, channel)
	if !updateRequired {
		return
	}

	switch wr.Channel {
	case "ticker":
		ticker := storage.Ticker{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
		}

		for _, value := range wr.Data {
			ticker.Price, err = strconv.ParseFloat(value.TickerPrice, 64)
			if err != nil {
				logErrStack(err)
				return
			}

			ticker.Timestamp = time.Unix(0, value.Timestamp*int64(time.Millisecond)).UTC()
			break
		}

		if cfg.influxStr {
			ticker.InfluxVal = e.wrapper.getTickerInfluxTime(cfg.mktCommitName)
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		var err error
		for _, data := range wr.Update {
			for _, value := range data {
				trade := storage.Trade{
					Exchange:      e.wrapper.name,
					MktID:         market,
					MktCommitName: cfg.mktCommitName,
					TradeID:       strconv.FormatUint(value.TradeID, 10),
					Side:          value.Side,
					Timestamp:     time.Unix(0, value.Timestamp*int64(time.Millisecond)).UTC(),
				}

				trade.Size, err = strconv.ParseFloat(value.Qty, 64)
				if err != nil {
					logErrStack(err)
					continue
				}

				trade.Price, err = strconv.ParseFloat(value.TradePrice, 64)
				if err != nil {
					logErrStack(err)
					continue
				}

				if cfg.influxStr {
					trade.InfluxVal = e.wrapper.getTradeInfluxTime(cfg.mktCommitName)
				}

				if err = e.wrapper.appendTrade(trade, cfg); err != nil {
					logErrStack(err)
				}
			}
		}
	}

	return
}

func (e *hitBTC) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restUrl = e.wrapper.config.RestUrl

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbols", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbols", mktID)
		q.Add("limit", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *hitBTC) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := make(map[string]restRespHitBTC)
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for _, data := range rr {
		price, err = strconv.ParseFloat(data.TickerPrice, 64)
		if err != nil {
			logErrStack(err)
		}
		return
	}

	return
}

func (e *hitBTC) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	rr := make(map[string][]restRespHitBTC)

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	var data []restRespHitBTC
	for _, d := range rr {
		data = d
		break
	}

	for i := range data {
		var err error
		r := data[i]

		trade := storage.Trade{
			TradeID:       strconv.FormatUint(r.TradeID, 10),
			Side:          r.Side,
			Timestamp:     r.Timestamp,
		}

		trade.Size, err = strconv.ParseFloat(r.Qty, 64)
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

	return
}
