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

type coinbasePro struct {
	wrapper *Wrapper
}

type wsSubCoinPro struct {
	Type     string             `json:"type"`
	Channels []wsSubChanCoinPro `json:"channels"`
}

type wsSubChanCoinPro struct {
	Name       string    `json:"name"`
	ProductIds [1]string `json:"product_ids"`
}

type respCoinPro struct {
	Type      string             `json:"type"`
	ProductID string             `json:"product_id"`
	TradeID   uint64             `json:"trade_id"`
	Side      string             `json:"side"`
	Size      string             `json:"size"`
	Price     string             `json:"price"`
	Time      string             `json:"time"`
	Message   string             `json:"message"`
	Channels  []wsSubChanCoinPro `json:"channels"`
}

func NewCoinbasePro(wrapper *Wrapper) *coinbasePro {
	return &coinbasePro{wrapper: wrapper}
}

func (e *coinbasePro) postConnectWs() error { return nil }

func (e *coinbasePro) pingWs(_ context.Context) error { return nil }

func (e *coinbasePro) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *coinbasePro) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "trade" {
		channel = "matches"
	}

	channels := make([]wsSubChanCoinPro, 1)
	channels[0].Name = channel
	channels[0].ProductIds = [1]string{market}
	sub := wsSubCoinPro{
		Type:     "subscribe",
		Channels: channels,
	}

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *coinbasePro) processWs(frame []byte) (err error) {
	var market, channel string

	wr := respCoinPro{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.Type == "match" {
		channel = "trade"
	} else {
		channel = "ticker"
	}

	switch wr.Type {
	case "error":
		log.Error().Str("exchange", "coinbase-pro").Str("func", "readWs").Str("msg", wr.Message).Msg("")
		return errors.New("coinbase-pro websocket error")
	case "subscriptions":
		for _, channel := range wr.Channels {
			for _, market := range channel.ProductIds {
				if channel.Name == "matches" {
					log.Debug().Str("exchange", "coinbase-pro").Str("func", "readWs").Str("market", market).Str("channel", "trade").Msg("channel subscribed (this message may be duplicate as server sends list of all subscriptions on each channel subscribe)")
				} else {
					log.Debug().Str("exchange", "coinbase-pro").Str("func", "readWs").Str("market", market).Str("channel", channel.Name).Msg("channel subscribed (this message may be duplicate as server sends list of all subscriptions on each channel subscribe)")
				}
			}
		}
	}

	market = wr.ProductID
	cfg, _, updateRequired := e.wrapper.getCfgMap(market, channel)
	if !updateRequired {
		return
	}

	switch wr.Type {
	case "ticker":
		ticker := storage.Ticker{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
		}

		ticker.Price, err = strconv.ParseFloat(wr.Price, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		// Time sent is in string format.
		ticker.Timestamp, err = time.Parse(time.RFC3339Nano, wr.Time)
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
			Side:          wr.Side,
		}

		trade.Size, err = strconv.ParseFloat(wr.Size, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		trade.Price, err = strconv.ParseFloat(wr.Price, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		trade.Timestamp, err = time.Parse(time.RFC3339Nano, wr.Time)
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

func (e *coinbasePro) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"products/"+mktID+"/ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"products/"+mktID+"/trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("limit", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *coinbasePro) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := respCoinPro{}
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

func (e *coinbasePro) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	var rr []respCoinPro

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr {
		var err error
		r := rr[i]

		trade := storage.Trade{
			TradeID: strconv.FormatUint(r.TradeID, 10),
			Side:    r.Side,
		}

		trade.Size, err = strconv.ParseFloat(r.Size, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		trade.Price, err = strconv.ParseFloat(r.Price, 64)
		if err != nil {
			logErrStack(err)
			continue
		}

		trade.Timestamp, err = time.Parse(time.RFC3339Nano, r.Time)
		if err != nil {
			logErrStack(err)
			continue
		}

		trades = append(trades, trade)
	}

	return
}
