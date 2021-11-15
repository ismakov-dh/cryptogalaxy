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

type huobi struct {
	wrapper *Wrapper
}

func NewHuobi(wrapper *Wrapper) *huobi {
	return &huobi{wrapper: wrapper}
}

type respHuobi struct {
	Channel       string          `json:"ch"`
	Time          int64           `json:"ts"`
	Tick          respTickHuobi   `json:"tick"`
	RESTTradeData []respTickHuobi `json:"data"`
	Status        string          `json:"status"`
	SubChannel    string          `json:"subbed"`
	PingID        uint64          `json:"ping"`
}

type respTickHuobi struct {
	Open      float64         `json:"open"`
	High      float64         `json:"high"`
	Low       float64         `json:"low"`
	Close     float64         `json:"close"`
	Volume    float64         `json:"amount"`
	TradeData []respDataHuobi `json:"data"`
}

type respDataHuobi struct {
	WsTradeID   uint64  `json:"tradeId"`
	RESTTradeID uint64  `json:"trade-id"`
	Direction   string  `json:"direction"`
	Amount      float64 `json:"amount"`
	Price       float64 `json:"price"`
	Time        int64   `json:"ts"`
}

func (e *huobi) pongWs(pingID uint64) error {
	frame, err := jsoniter.Marshal(map[string]uint64{"pong": pingID})
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
	return nil
}

func (e *huobi) postConnectWs() (err error) { return }

func (e *huobi) pingWs(_ context.Context) (err error) { return }

func (e *huobi) readWs() ([]byte, error) {
	return e.wrapper.ws.ReadTextOrGzipBinary()
}

func (e *huobi) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	switch channel {
	case "ticker":
		channel = "market." + market + ".detail"
	case "trade":
		channel = "market." + market + ".trade.detail"
	case "candle":
		channel = "market." + market + ".kline.1min"
	}
	frame, err = jsoniter.Marshal(map[string]string{
		"sub": channel,
		"id":  channel,
	})
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *huobi) processWs(frame []byte) (err error) {
	var market, channel string

	wr := respHuobi{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.PingID != 0 {
		err = e.pongWs(wr.PingID)
		return
	}

	if wr.Status == "ok" && wr.SubChannel != "" {
		s := strings.Split(wr.SubChannel, ".")
		c := "ticker"
		switch s[2] {
		case "trade":
			channel = "trade"
		case "kline":
			channel = "candle"
		default:
			channel = "ticker"
		}
		log.Debug().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("market", s[1]).
			Str("channel", c).
			Msg("channel subscribed")
		return
	}

	if wr.Channel != "" {
		s := strings.Split(wr.Channel, ".")
		market = s[1]
		switch s[2] {
		case "trade":
			channel = "trade"
		case "kline":
			channel = "candle"
		default:
			channel = "ticker"
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
			Price:         wr.Tick.Close,
			Timestamp:     time.Unix(0, wr.Time*int64(time.Millisecond)).UTC(),
		}

		if cfg.influxStr {
			ticker.InfluxVal = e.wrapper.getTickerInfluxTime(cfg.mktCommitName)
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		var err error
		for _, data := range wr.Tick.TradeData {
			trade := storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				TradeID:       strconv.FormatUint(data.WsTradeID, 10),
				Side:          data.Direction,
				Size:          data.Amount,
				Price:         data.Price,
				Timestamp:     time.Unix(0, data.Time*int64(time.Millisecond)).UTC(),
			}

			if cfg.influxStr {
				trade.InfluxVal = e.wrapper.getTradeInfluxTime(cfg.mktCommitName)
			}

			err = e.wrapper.appendTrade(trade, cfg)
			if err != nil {
				logErrStack(err)
				continue
			}
		}
	case "candle":
		candle := storage.Candle{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
			Open:          wr.Tick.Open,
			High:          wr.Tick.High,
			Low:           wr.Tick.Low,
			Close:         wr.Tick.Close,
			Volume:        wr.Tick.Volume,
			Timestamp:     time.Unix(0, wr.Time*int64(time.Millisecond)).UTC(),
		}

		if cfg.influxStr {
			candle.InfluxVal = e.wrapper.getCandleInfluxTime(cfg.mktCommitName)
		}

		err = e.wrapper.appendCandle(candle, cfg)
	}

	return
}

func (e *huobi) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"market/detail/merged")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}

		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"market/history/trade")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
		q.Add("size", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *huobi) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := respHuobi{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price = rr.Tick.Close

	return
}

func (e *huobi) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	rr := respHuobi{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr.RESTTradeData {
		d := rr.RESTTradeData[i]
		for j := range d.TradeData {
			r := d.TradeData[j]

			trade := storage.Trade{
				TradeID:   strconv.FormatUint(r.RESTTradeID, 10),
				Side:      r.Direction,
				Size:      r.Amount,
				Price:     r.Price,
				Timestamp: time.Unix(0, r.Time*int64(time.Millisecond)).UTC(),
			}

			trades = append(trades, trade)
		}
	}

	return
}
