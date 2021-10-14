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

type bequant struct {
	wrapper *Wrapper
}

type wsSubBequant struct {
	Method  string             `json:"method"`
	Channel string             `json:"ch"`
	Params  wsSubParamsBequant `json:"params"`
	ID      int                `json:"id"`
}

type wsSubParamsBequant struct {
	Symbols [1]string `json:"symbols"`
}

type wsRespBequant struct {
	Result  wsSubResBequant                `json:"result"`
	Channel string                         `json:"ch"`
	Tickers map[string]wsRespDataBequant   `json:"data"`
	Trades  map[string][]wsRespDataBequant `json:"update"`
	ID      int                            `json:"id"`
}

type wsSubResBequant struct {
	Channel string `json:"ch"`
}

type wsRespDataBequant struct {
	TradeID     uint64 `json:"i"`
	Side        string `json:"s"`
	Size        string `json:"q"`
	TickerPrice string `json:"c"`
	TradePrice  string `json:"p"`
	Timestamp   int64  `json:"t"`
}

type restRespDataBequant struct {
	TradeID     uint64    `json:"id"`
	Side        string    `json:"side"`
	Size        string    `json:"qty"`
	TickerPrice string    `json:"last"`
	TradePrice  string    `json:"price"`
	Timestamp   time.Time `json:"timestamp"`
}

func NewBequant(wrapper *Wrapper) *bequant {
	return &bequant{wrapper: wrapper}
}

func (e *bequant) postConnectWs() error { return nil }

func (e *bequant) pingWs(_ context.Context) error { return nil }

func (e *bequant) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *bequant) getWsSubscribeMessage(market string, channel string, id int) (frame []byte, err error) {
	if channel == "ticker" {
		channel = "ticker/price/1s"
	} else {
		channel = "trades"
	}

	sub := wsSubBequant{
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

func (e *bequant) processWs(frame []byte) (err error) {
	var market, channel string

	wr := wsRespBequant{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	ch := e.wrapper.channelIds[wr.ID]
	market, channel = ch[0], ch[1]

	if wr.Result.Channel != "" {
		if wr.Result.Channel == "ticker/price/1s" {
			log.Debug().Str("exchange", "bequant").Str("func", "readWs").Str("market", ch[0]).Str("channel", "ticker").Msg("channel subscribed")
		} else {
			log.Debug().Str("exchange", "bequant").Str("func", "readWs").Str("market", ch[0]).Str("channel", "trade").Msg("channel subscribed")
		}
		return
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
			Timestamp:     time.Unix(0, wr.Tickers[market].Timestamp*int64(time.Millisecond)).UTC(),
		}

		ticker.Price, err = strconv.ParseFloat(wr.Tickers[market].TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		if cfg.influxStr {
			ticker.InfluxVal = e.wrapper.getTickerInfluxTime(cfg.mktCommitName)
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		var err error
		for _, data := range wr.Trades[market] {
			trade := storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				TradeID:       strconv.FormatUint(data.TradeID, 10),
				Side:          data.Side,
				Timestamp:     time.Unix(0, data.Timestamp*int64(time.Millisecond)).UTC(),
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

			if cfg.influxStr {
				trade.InfluxVal = e.wrapper.getTradeInfluxTime(cfg.mktCommitName)
			}
		}
	}

	return
}

func (e *bequant) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbols", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"trades")
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

func (e *bequant) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := make(map[string]restRespDataBequant, 1)
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for _, data := range rr {
		price, err = strconv.ParseFloat(data.TickerPrice, 64)
		break
	}
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *bequant) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	rr := make(map[string][]restRespDataBequant)

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	var data []restRespDataBequant
	for _, d := range rr {
		data = d
		break
	}

	for i := range data {
		var err error
		r := data[i]

		trade := storage.Trade{
			TradeID:   strconv.FormatUint(r.TradeID, 10),
			Side:      r.Side,
			Timestamp: r.Timestamp,
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

		trades = append(trades, trade)
	}

	return
}
