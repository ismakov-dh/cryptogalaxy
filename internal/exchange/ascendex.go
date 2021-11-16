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

type ascendex struct {
	wrapper *Wrapper
}

type wsRespAscendex struct {
	Type         string              `json:"m"`
	Channel      string              `json:"ch"`
	TickerSymbol string              `json:"s"`
	TradeSymbol  string              `json:"symbol"`
	Data         jsoniter.RawMessage `json:"data"`
	Code         int                 `json:"code"`
	Error        string              `json:"err"`
}

type wsTickerRespDataAscendex struct {
	Last      string `json:"l"`
	Timestamp int64  `json:"ts"`
}

type restRespAscendex struct {
	Data restRespDataAscendex `json:"data"`
}

type restRespDataAscendex struct {
	TickerPrice string                  `json:"close"`
	Data        []tradeRespDataAscendex `json:"data"`
}

type tradeRespDataAscendex struct {
	TradeID   uint64 `json:"seqnum"`
	Maker     bool   `json:"bm"`
	Size      string `json:"q"`
	Price     string `json:"p"`
	Timestamp int64  `json:"ts"`
}

func NewAscendex(wrapper *Wrapper) *ascendex {
	return &ascendex{wrapper: wrapper}
}

func (e *ascendex) postConnectWs() error { return nil }

func (e *ascendex) pingWs(ctx context.Context) error {
	tick := time.NewTicker(13 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			err := e.wrapper.ws.Write([]byte(`{"op":"ping"}`))
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

func (e *ascendex) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *ascendex) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	if channel == "ticker" {
		channel = "bar:1:" + market
	} else {
		channel = "trades:" + market
	}
	frame, err = jsoniter.Marshal(map[string]string{
		"op": "sub",
		"ch": channel,
	})
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *ascendex) processWs(frame []byte) (err error) {
	var market, channel string

	wr := wsRespAscendex{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.Error != "" {
		log.Error().Str("exchange", "ascendex").Str("func", "processWs").Int("code", wr.Code).Str("msg", wr.Error).Msg("")
		err = errors.New("ascendex websocket error")
		return
	}

	// Consider frame only in configured interval, otherwise ignore it.
	switch wr.Type {
	case "sub":
		ch := strings.Split(wr.Channel, ":")
		if ch[0] == "bar" {
			channel = "ticker"
		} else {
			channel = "trade"
		}
		log.Debug().Str("exchange", "ascendex").Str("func", "processWs").Str("market", ch[1]).Str("channel", channel).Msg("channel subscribed")
		return
	case "bar":
		channel = "ticker"
		market = wr.TickerSymbol

	case "trades":
		channel = "trade"
		market = wr.TradeSymbol
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

		data := wsTickerRespDataAscendex{}
		if err = jsoniter.Unmarshal(wr.Data, &data); err != nil {
			logErrStack(err)
			return
		}

		ticker.Timestamp = time.Unix(0, data.Timestamp*int64(time.Millisecond)).UTC()

		ticker.Price, err = strconv.ParseFloat(data.Last, 64)
		if err != nil {
			logErrStack(err)
			return
		}

		err = e.wrapper.appendTicker(ticker, cfg)
	case "trade":
		var dataResp []tradeRespDataAscendex
		if err = jsoniter.Unmarshal(wr.Data, &dataResp); err != nil {
			logErrStack(err)
			return
		}

		var err error
		for _, data := range dataResp {
			trade := storage.Trade{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				TradeID:       strconv.FormatUint(data.TradeID, 10),
				Timestamp:     time.Unix(0, data.Timestamp*int64(time.Millisecond)).UTC(),
			}

			if data.Maker {
				trade.Side = "buy"
			} else {
				trade.Side = "sell"
			}

			trade.Size, err = strconv.ParseFloat(data.Size, 64)
			if err != nil {
				logErrStack(err)
				continue
			}

			trade.Price, err = strconv.ParseFloat(data.Price, 64)
			if err != nil {
				logErrStack(err)
				continue
			}

			err = e.wrapper.appendTrade(trade, cfg)
			if err != nil {
				logErrStack(err)
			}
		}
	}

	return
}

func (e *ascendex) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestUrl+"ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestUrl+"trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
		q.Add("n", strconv.Itoa(100))
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *ascendex) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespAscendex{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price, err = strconv.ParseFloat(rr.Data.TickerPrice, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *ascendex) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	rr := restRespAscendex{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr.Data.Data {
		var err error
		r := rr.Data.Data[i]

		trade := storage.Trade{
			TradeID:   strconv.FormatUint(r.TradeID, 10),
			Timestamp: time.Unix(0, r.Timestamp*int64(time.Millisecond)).UTC(),
		}

		if r.Maker {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
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

		trades = append(trades, trade)
	}

	return
}
