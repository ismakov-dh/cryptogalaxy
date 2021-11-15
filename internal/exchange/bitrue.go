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

type bitrue struct {
	wrapper *Wrapper
}

type wsSubBitrue struct {
	Event  string            `json:"event"`
	Params wsSubParamsBitrue `json:"params"`
}

type wsSubParamsBitrue struct {
	Channel string `json:"channel"`
	CbID    string `json:"cb_id"`
}

type wsRespBitrue struct {
	Channel  string           `json:"channel"`
	EventRep string           `json:"event_rep"`
	Status   string           `json:"status"`
	Tick     wsRespDataBitrue `json:"tick"`
	PingID   uint64           `json:"ping"`
}

type wsRespDataBitrue struct {
	TickerPrice float64                 `json:"close"`
	Trade       []wsRespTradeDataBitrue `json:"data"`
}

type wsRespTradeDataBitrue struct {
	TradeID    uint64  `json:"id"`
	Side       string  `json:"side"`
	Size       float64 `json:"vol"`
	TradePrice float64 `json:"price"`
	Timestamp  int64   `json:"ts"`
}

type restRespBitrue struct {
	TradeID uint64 `json:"id"`
	Maker   bool   `json:"isBuyerMaker"`
	Qty     string `json:"qty"`
	Price   string `json:"price"`
	Time    int64  `json:"time"`
}

func NewBitrue(wrapper *Wrapper) *bitrue {
	return &bitrue{wrapper: wrapper}
}

func (e *bitrue) pongWs(pingID uint64) error {
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

func (e *bitrue) postConnectWs() error { return nil }

func (e *bitrue) pingWs(_ context.Context) error { return nil }

func (e *bitrue) readWs() ([]byte, error) {
	return e.wrapper.ws.ReadTextOrGzipBinary()
}

func (e *bitrue) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "trade" {
		channel = "trade_ticker"
	}
	sub := wsSubBitrue{
		Event: "sub",
	}
	market = strings.ToLower(market)
	sub.Params.Channel = "market_" + market + "_" + channel
	sub.Params.CbID = market

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *bitrue) processWs(frame []byte) (err error) {
	var market, channel string

	wr := wsRespBitrue{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.PingID != 0 {
		err = e.pongWs(wr.PingID)
		return
	}

	s := strings.Split(wr.Channel, "_")
	market = s[1]
	if s[2] == "ticker" {
		channel = "ticker"
	} else {
		channel = "trade"
	}

	if wr.EventRep == "subed" {
		log.Debug().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("market", market).
			Str("channel", channel).
			Msg("channel subscribed")
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
			Price:         wr.Tick.TickerPrice,
			Timestamp:     time.Now().UTC(),
		}

		if cfg.influxStr {
			ticker.InfluxVal = e.wrapper.getTickerInfluxTime(cfg.mktCommitName)
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		for _, data := range wr.Tick.Trade {
			trade := storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				TradeID:       strconv.FormatUint(data.TradeID, 10),
				Size:          data.Size,
				Price:         data.TradePrice,
				Timestamp:     time.Unix(0, data.Timestamp*int64(time.Millisecond)).UTC(),
			}

			if data.Side == "BUY" {
				trade.Side = "buy"
			} else {
				trade.Side = "sell"
			}

			if cfg.influxStr {
				trade.InfluxVal = e.wrapper.getTradeInfluxTime(cfg.mktCommitName)
			}
		}
	}

	return
}

func (e *bitrue) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
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

func (e *bitrue) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespBitrue{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price, err = strconv.ParseFloat(rr.Price, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *bitrue) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	var rr []restRespBitrue
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
