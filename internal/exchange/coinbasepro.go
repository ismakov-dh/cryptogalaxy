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

type CoinbasePro struct {
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

func NewCoinbasePro(wrapper *Wrapper) *CoinbasePro {
	return &CoinbasePro{wrapper: wrapper}
}

func (e *CoinbasePro) postConnectWs() error { return nil }

func (e *CoinbasePro) pingWs(_ context.Context) error { return nil }

func (e *CoinbasePro) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *CoinbasePro) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
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

func (e *CoinbasePro) processWs(frame []byte) (err error) {
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
		log.Error().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Str("msg", wr.Message).
			Msg("")
		return errors.New("coinbase-pro websocket error")
	case "subscriptions":
		for _, channel := range wr.Channels {
			for _, market := range channel.ProductIds {
				if channel.Name == "matches" {
					log.Debug().
						Str("exchange", e.wrapper.name).
						Str("func", "processWs").
						Str("market", market).
						Str("channel", "trade").
						Msg("channel subscribed")
				} else {
					log.Debug().
						Str("exchange", e.wrapper.name).
						Str("func", "processWs").
						Str("market", market).
						Str("channel", channel.Name).
						Msg("channel subscribed")
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
		ticker := &storage.Ticker{
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

		e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		trade := &storage.Trade{
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

		e.wrapper.appendTrade(trade, cfg)
	}

	return err
}

func (e *CoinbasePro) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestURL+"products/"+mktID+"/ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestURL+"products/"+mktID+"/trades")
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

func (e *CoinbasePro) processRestTicker(body io.ReadCloser) (price float64, err error) {
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

func (e *CoinbasePro) processRestTrade(body io.ReadCloser) (trades []*storage.Trade, err error) {
	var rr []respCoinPro

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr {
		var err error
		r := rr[i]

		trade := &storage.Trade{
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

	return trades, err
}
