package exchange

import (
	"context"
	"fmt"
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

type kucoin struct {
	wrapper      *Wrapper
	wsPingIntSec uint64
}

type wsSubKucoin struct {
	ID             int    `json:"id"`
	Type           string `json:"type"`
	Topic          string `json:"topic"`
	PrivateChannel bool   `json:"privateChannel"`
	Response       bool   `json:"response"`
}

type respKucoin struct {
	ID            string         `json:"id"`
	Topic         string         `json:"topic"`
	Data          respDataKucoin `json:"data"`
	Type          string         `json:"type"`
}

type restRespKucoin struct {
	Data []respDataKucoin `json:"data"`
}

type respDataKucoin struct {
	TradeID string      `json:"tradeId"`
	Side    string      `json:"side"`
	Size    string      `json:"size"`
	Price   string      `json:"price"`
	Time    interface{} `json:"time"`
}

type wsConnectRespKucoin struct {
	Code string `json:"code"`
	Data struct {
		Token           string `json:"token"`
		Instanceservers []struct {
			Endpoint          string `json:"endpoint"`
			Protocol          string `json:"protocol"`
			PingintervalMilli int    `json:"pingInterval"`
		} `json:"instanceServers"`
	} `json:"data"`
}

func NewKucoin(ctx context.Context, wrapper *Wrapper) (e *kucoin, error error) {
	resp, err := http.Post(wrapper.config.RestUrl+"bullet-public", "", nil)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("code : %v, status : %v", resp.StatusCode, resp.Status)
		return
	}

	r := wsConnectRespKucoin{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&r); err != nil {
		logErrStack(err)
		resp.Body.Close()
		return
	}
	resp.Body.Close()
	if r.Code != "200000" || len(r.Data.Instanceservers) < 1 {
		err = errors.New("not able to get websocket server details")
		return
	}

	e = &kucoin{wrapper: wrapper}
	e.wsPingIntSec = uint64(r.Data.Instanceservers[0].PingintervalMilli) / 1000

	wrapper.config.WebsocketUrl = r.Data.Instanceservers[0].Endpoint + "?token=" + r.Data.Token

	return e, err
}

func (e *kucoin) postConnectWs() error {
	frame, err := e.wrapper.ws.Read()
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			err = errors.New("context canceled")
		} else {
			if err == io.EOF {
				err = errors.Wrap(err, "connection close by exchange server")
			}
			logErrStack(err)
		}
		return err
	}
	if len(frame) == 0 {
		return errors.New("not able to connect websocket server")
	}

	wr := respKucoin{}
	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return err
	}

	if wr.Type == "welcome" {

		log.Info().Str("exchange", "kucoin").Msg("websocket connected")
	} else {
		return errors.New("not able to connect websocket server")
	}
	return nil
}

func (e *kucoin) pingWs(ctx context.Context) error {
	interval := e.wsPingIntSec * 90 / 100
	tick := time.NewTicker(time.Duration(interval) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			frame, err := jsoniter.Marshal(map[string]string{
				"id":   strconv.FormatInt(time.Now().Unix(), 10),
				"type": "ping",
			})
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

func (e *kucoin) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *kucoin) getWsSubscribeMessage(market string, channel string, id int) (frame []byte, err error) {
	switch channel {
	case "ticker":
		channel = "/market/ticker:" + market
	case "trade":
		channel = "/market/match:" + market
	}
	sub := wsSubKucoin{
		ID:             id,
		Type:           "subscribe",
		Topic:          channel,
		PrivateChannel: false,
		Response:       true,
	}
	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *kucoin) processWs(frame []byte) (err error) {
	var market, channel string

	wr := respKucoin{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	switch wr.Type {
	case "pong":
	case "ack":
		var id int
		id, err = strconv.Atoi(wr.ID)
		if err != nil {
			logErrStack(err)
			return
		}
		ch := e.wrapper.channelIds[id]
		log.Debug().
			Str("exchange", "kucoin").
			Str("func", "readWs").
			Str("market", ch[0]).
			Str("channel", ch[1]).
			Msg("channel subscribed")
		return
	case "message":
		s := strings.Split(wr.Topic, ":")
		if len(s) < 2 {
			return
		}

		market = s[1]
		if s[0] == "/market/ticker" {
			channel = "ticker"
		} else {
			channel = "trade"
		}

		cfg, _, updateRequired := e.wrapper.getCfgMap(market, channel)
		if !updateRequired {
			return
		}

		switch market {
		case "ticker":
			ticker := storage.Ticker{
				Exchange:      e.wrapper.name,
				MktID:         market,
				MktCommitName: cfg.mktCommitName,
				Timestamp:     time.Now().UTC(),
			}

			ticker.Price, err = strconv.ParseFloat(wr.Data.Price, 64)
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
				TradeID:       wr.Data.TradeID,
				Side:          wr.Data.Side,
			}

			trade.Size, err = strconv.ParseFloat(wr.Data.Size, 64)
			if err != nil {
				logErrStack(err)
				return
			}

			trade.Price, err = strconv.ParseFloat(wr.Data.Price, 64)
			if err != nil {
				logErrStack(err)
				return
			}

			if t, ok := wr.Data.Time.(string); ok {
				var timestamp int64
				timestamp, err = strconv.ParseInt(t, 10, 64)
				if err != nil {
					logErrStack(err)
					return
				}
				trade.Timestamp = time.Unix(0, timestamp*int64(time.Nanosecond)).UTC()
			} else {
				log.Error().Str("exchange", "kucoin").Str("func", "processWs").Interface("time", wr.Data.Time).Msg("")
				err = errors.New("cannot convert trade data field time to string")
				return
			}

			if cfg.influxStr {
				e.wrapper.getTradeInfluxTime(cfg.mktCommitName)
			}

			err = e.wrapper.appendTrade(trade, cfg)
		}
	}

	return
}

func (e *kucoin) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"market/orderbook/level1")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.config.RestUrl+"market/histories")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *kucoin) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := respKucoin{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	price, err = strconv.ParseFloat(rr.Data.Price, 64)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *kucoin) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	rr := restRespKucoin{}

	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr.Data {
		var err error
		r := rr.Data[i]

		trade := storage.Trade{
			Side: r.Side,
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

		t, ok := r.Time.(float64)
		if !ok {
			log.Error().Str("exchange", "kucoin").Str("func", "processREST").Interface("time", r.Time).Msg("error converting time")
			continue
		}
		trade.Timestamp = time.Unix(0, int64(t)*int64(time.Nanosecond)).UTC()

		trades = append(trades, trade)
	}

	return
}
