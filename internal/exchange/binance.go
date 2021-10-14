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

type binance struct {
	wrapper *Wrapper
}

type wsSubBinance struct {
	Method string    `json:"method"`
	Params [1]string `json:"params"`
	ID     int       `json:"id"`
}

type wsRespBinance struct {
	Event         string `json:"e"`
	Symbol        string `json:"s"`
	TradeID       uint64 `json:"t"`
	Maker         bool   `json:"m"`
	Qty           string `json:"q"`
	TickerPrice   string `json:"c"`
	TradePrice    string `json:"p"`
	TickerTime    int64  `json:"E"`
	TradeTime     int64  `json:"T"`
	Code          int    `json:"code"`
	Msg           string `json:"msg"`
	ID            int    `json:"id"`

	// This field value is not used but still need to present
	// because otherwise json decoder does case-insensitive match with "m" and "M".
	IsBestMatch bool `json:"M"`
}

type restRespBinance struct {
	TradeID uint64 `json:"id"`
	Maker   bool   `json:"isBuyerMaker"`
	Qty     string `json:"qty"`
	Price   string `json:"price"`
	Time    int64  `json:"time"`
}

func NewBinance(wrapper *Wrapper) *binance {
	return &binance{wrapper: wrapper}
}

func (e *binance) postConnectWs() error { return nil }

func (e *binance) pingWs(_ context.Context) error { return nil }

func (e *binance) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *binance) getWsSubscribeMessage(market string, channel string, id int) (frame []byte, err error) {
	if channel == "ticker" {
		channel = "miniTicker"
	}
	channel = strings.ToLower(market) + "@" + channel
	sub := wsSubBinance{
		Method: "SUBSCRIBE",
		Params: [1]string{channel},
		ID:     id,
	}
	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *binance) processWs(frame []byte) (err error) {
	var market, channel string

	wr := wsRespBinance{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	ch := e.wrapper.channelIds[wr.ID]
	market, channel = ch[0], ch[1]

	if wr.ID != 0 {
		log.Debug().Str("exchange", "binance").Str("func", "readWs").Str("market", ch[0]).Str("channel", ch[1]).Msg("channel subscribed")
		return
	}
	if wr.Msg != "" {
		log.Error().Str("exchange", "binance").Str("func", "readWs").Int("code", wr.Code).Str("msg", wr.Msg).Msg("")
		return errors.New("binance websocket error")
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
			Timestamp:     time.Unix(0, wr.TickerTime*int64(time.Millisecond)).UTC(),
		}

		ticker.Price, err = strconv.ParseFloat(wr.TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		if cfg.influxStr {
			ticker.InfluxVal = e.wrapper.getTickerInfluxTime(cfg.mktCommitName)
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		trade := storage.Trade{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
			TradeID:       strconv.FormatUint(wr.TradeID, 10),
			Timestamp:     time.Unix(0, wr.TradeTime*int64(time.Millisecond)).UTC(),
		}

		if wr.Maker {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}

		trade.Size, err = strconv.ParseFloat(wr.Qty, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		trade.Price, err = strconv.ParseFloat(wr.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		if cfg.influxStr {
			trade.InfluxVal = e.wrapper.getTradeInfluxTime(cfg.mktCommitName)
		}

		err = e.wrapper.appendTrade(trade, cfg)
	}

	return
}

func (e *binance) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"ticker/price")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"trades")
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

func (e *binance) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespBinance{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price, err = strconv.ParseFloat(rr.Price, 64)
	if err != nil {
		logErrStack(err)
		return
	}

	return
}

func (e *binance) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	var rr []restRespBinance

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr {
		var err error
		r := rr[i]

		trade := storage.Trade{
			TradeID:   strconv.FormatUint(r.TradeID, 10),
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
