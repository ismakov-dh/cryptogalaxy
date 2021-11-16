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

type Gemini struct {
	wrapper *Wrapper
}

type wsSubGemini struct {
	Type          string           `json:"type"`
	Subscriptions []wsSubSubGemini `json:"subscriptions"`
}

type wsSubSubGemini struct {
	Name    string    `json:"name"`
	Symbols [1]string `json:"symbols"`
}

type wsRespGemini struct {
	Type          string `json:"type"`
	Symbol        string `json:"symbol"`
	EventID       uint64 `json:"event_id"`
	Side          string `json:"side"`
	Quantity      string `json:"quantity"`
	Price         string `json:"price"`
	Timestamp     int64  `json:"timestamp"`
	mktCommitName string
}

type restRespGemini struct {
	Symbol      string `json:"symbol"`
	TradeID     uint64 `json:"tid"`
	Type        string `json:"type"`
	Amount      string `json:"amount"`
	Price       string `json:"price"`
	Timestamp   int64  `json:"timestampms"`
	TickerPrice string `json:"last"`
}

func NewGemini(wrapper *Wrapper) *Gemini {
	return &Gemini{wrapper: wrapper}
}

func (e *Gemini) postConnectWs() error { return nil }

func (e *Gemini) pingWs(_ context.Context) error { return nil }

func (e *Gemini) readWs() (frame []byte, err error) {
	return e.wrapper.ws.Read()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (e *Gemini) getWsSubscribeMessage(market string, _ string, _ int) (frame []byte, err error) {
	channels := make([]wsSubSubGemini, 1)
	channels[0].Name = "l2"
	channels[0].Symbols = [1]string{market}
	sub := wsSubGemini{
		Type:          "subscribe",
		Subscriptions: channels,
	}

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *Gemini) processWs(frame []byte) (err error) {
	var market string
	log.Debug().
		Str("exchange", e.wrapper.name).
		Str("func", "processWs").
		Msg("unlike other exchanges gemini does not send channel subscribed success message")

	wr := wsRespGemini{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return err
	}

	market = wr.Symbol

	if wr.Type == "trade" {
		cfg, ok, updateRequired := e.wrapper.getCfgMap(market, "ticker")
		if !updateRequired {
			return
		}
		if ok {
			ticker := &storage.Ticker{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				Timestamp:     time.Now().UTC(),
			}
			ticker.Exchange = e.wrapper.name
			ticker.MktID = wr.Symbol
			ticker.MktCommitName = wr.mktCommitName

			ticker.Price, err = strconv.ParseFloat(wr.Price, 64)
			if err != nil {
				logErrStack(err)
				return
			}

			e.wrapper.appendTicker(ticker, cfg)
		}

		cfg, ok, updateRequired = e.wrapper.getCfgMap(market, "trade")
		if !updateRequired {
			return
		}
		if ok {
			trade := &storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				TradeID:       strconv.FormatUint(wr.EventID, 10),
				Side:          wr.Side,
				Timestamp:     time.Unix(0, wr.Timestamp*int64(time.Millisecond)).UTC(),
			}

			trade.Size, err = strconv.ParseFloat(wr.Quantity, 64)
			if err != nil {
				logErrStack(err)
				return
			}

			trade.Price, err = strconv.ParseFloat(wr.Price, 64)
			if err != nil {
				logErrStack(err)
				return
			}

			e.wrapper.appendTrade(trade, cfg)
		}
	}

	return err
}

func (e *Gemini) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restURL = e.wrapper.exchangeCfg().RestURL

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", restURL+"pubticker/"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", restURL+"trades/"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("limit_trades", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *Gemini) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespGemini{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price, err = strconv.ParseFloat(rr.TickerPrice, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *Gemini) processRestTrade(body io.ReadCloser) (trades []*storage.Trade, err error) {
	var rr []restRespGemini

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr {
		var err error
		r := rr[i]

		trade := &storage.Trade{
			TradeID:   strconv.FormatUint(r.TradeID, 10),
			Side:      r.Type,
			Timestamp: time.Unix(0, r.Timestamp*int64(time.Millisecond)).UTC(),
		}

		trade.Size, err = strconv.ParseFloat(r.Amount, 64)
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

	return trades, err
}
