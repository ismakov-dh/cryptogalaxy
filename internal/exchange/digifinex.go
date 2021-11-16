package exchange

import (
	"context"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type Digifinex struct {
	wrapper *Wrapper
}

type wsSubDigifinex struct {
	ID     int64     `json:"id"`
	Method string    `json:"method"`
	Params [1]string `json:"params"`
}

type wsRespDigifinex struct {
	Error  wsRespErrorDigifinex `json:"error"`
	Result interface{}          `json:"result"`
	ID     int                  `json:"id"`
	Method string               `json:"method"`
	Params jsoniter.RawMessage  `json:"params"`
}

type wsRespErrorDigifinex struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type wsRespDataDetailDigifinex struct {
	Symbol      string `json:"symbol"`
	TickerPrice string `json:"last"`
	TickerTime  int64  `json:"timestamp"`
}

type restRespDigifinex struct {
	Tickers []restRespDataDetailDigifinex `json:"ticker"`
	Trades  []restRespDataDetailDigifinex `json:"data"`
}

type restRespDataDetailDigifinex struct {
	TradeID     uint64  `json:"id"`
	Type        string  `json:"type"`
	Amount      float64 `json:"amount"`
	TickerPrice float64 `json:"last"`
	TradePrice  float64 `json:"price"`
	Date        int64   `json:"date"`
}

func NewDigifinex(wrapper *Wrapper) *Digifinex {
	return &Digifinex{wrapper: wrapper}
}

func (e *Digifinex) postConnectWs() error { return nil }

func (e *Digifinex) pingWs(ctx context.Context) error {
	tick := time.NewTicker(25 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			sub := wsSubDigifinex{
				ID:     time.Now().Unix(),
				Method: "server.ping",
			}
			frame, err := jsoniter.Marshal(sub)
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

func (e *Digifinex) readWs() ([]byte, error) {
	return e.wrapper.ws.ReadTextOrZlibBinary()
}

func (e *Digifinex) getWsSubscribeMessage(market string, channel string, id int) (frame []byte, err error) {
	if channel == "trade" {
		channel = "trades"
	}
	sub := wsSubDigifinex{
		ID:     int64(id),
		Method: channel + ".subscribe",
		Params: [1]string{market},
	}

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

func (e *Digifinex) processWs(frame []byte) (err error) {
	wr := wsRespDigifinex{}

	err = jsoniter.Unmarshal(frame, &wr)
	if err != nil {
		logErrStack(err)
		return
	}

	if wr.Error.Message != "" {
		log.Error().
			Str("exchange", e.wrapper.name).
			Str("func", "processWs").
			Int("code", wr.Error.Code).
			Str("msg", wr.Error.Message).
			Msg("")
		return errors.New("digifinex websocket error")
	}
	if wr.ID != 0 {
		if _, ok := wr.Result.(string); ok {
			// Both pong frame and subscribe success response comes through the same field,
			// so the interface used.
		} else {
			log.Debug().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Str("market", e.wrapper.channelIds[wr.ID][0]).
				Str("channel", e.wrapper.channelIds[wr.ID][1]).
				Msg("channel subscribed")
		}
		return
	}

	if wr.Method == "ticker.update" {
		var dd []wsRespDataDetailDigifinex
		if err = jsoniter.Unmarshal(wr.Params, &dd); err != nil {
			logErrStack(err)
			return
		}

		for i := range dd {
			r := dd[i]

			cfg, _, updateRequired := e.wrapper.getCfgMap(r.Symbol, "ticker")
			if !updateRequired {
				return
			}

			ticker := &storage.Ticker{
				Exchange:      e.wrapper.name,
				MktID:         r.Symbol,
				MktCommitName: cfg.mktCommitName,
				Timestamp:     time.Unix(0, r.TickerTime*int64(time.Millisecond)).UTC(),
			}

			ticker.Price, err = strconv.ParseFloat(r.TickerPrice, 64)
			if err != nil {
				logErrStack(err)
				return
			}

			e.wrapper.appendTicker(ticker, cfg)
		}
	} else {
		wr.Method = "trade"

		// Received data has different data structures for ticker and trades.
		data := make([]interface{}, 3)
		if err = jsoniter.Unmarshal(wr.Params, &data); err != nil {
			logErrStack(err)
			return
		}

		// Ignoring initial snapshot trades.
		if snapshot, ok := data[0].(bool); ok {
			if snapshot {
				return
			}

			if symbol, ok := data[2].(string); ok {
				cfg, _, updateRequired := e.wrapper.getCfgMap(symbol, "trade")
				if !updateRequired {
					return
				}

				switch trades := data[1].(type) {
				case []interface{}:
					for _, data := range trades {
						if detail, ok := data.(map[string]interface{}); ok {
							trade := &storage.Trade{
								Exchange:      e.wrapper.name,
								MktID:         symbol,
								MktCommitName: cfg.mktCommitName,
							}

							if tradeID, ok := detail["id"].(float64); ok {
								trade.TradeID = strconv.FormatFloat(tradeID, 'f', 0, 64)
							} else {
								log.Error().
									Str("exchange", e.wrapper.name).
									Str("func", "processWs").
									Interface("trade id", data).
									Msg("cannot convert frame data field trade id to float64")
								break
							}

							if side, ok := detail["type"].(string); ok {
								trade.Side = side
							} else {
								log.Error().
									Str("exchange", e.wrapper.name).
									Str("func", "processWs").
									Interface("side", data).
									Msg("cannot convert frame data field side to string")
								break
							}

							if size, ok := detail["amount"].(string); ok {
								size, err := strconv.ParseFloat(size, 64)
								if err != nil {
									logErrStack(err)
									return err
								}
								trade.Size = size
							} else {
								log.Error().
									Str("exchange", e.wrapper.name).
									Str("func", "processWs").
									Interface("size", data).
									Msg("cannot convert frame data field size to float64")
								break
							}

							if price, ok := detail["price"].(string); ok {
								price, err := strconv.ParseFloat(price, 64)
								if err != nil {
									logErrStack(err)
									break
								}
								trade.Price = price
							} else {
								log.Error().
									Str("exchange", e.wrapper.name).
									Str("func", "processWs").
									Interface("price", data).
									Msg("cannot convert frame data field price to float64")
								break
							}

							if timestamp, ok := detail["time"].(float64); ok {
								intPart, fracPart := math.Modf(timestamp)
								trade.Timestamp = time.Unix(int64(intPart), int64(fracPart*1e9)).UTC()
							} else {
								log.Error().
									Str("exchange", e.wrapper.name).
									Str("func", "processWs").
									Interface("timestamp", data).
									Msg("cannot convert frame data field timestamp to float64")
								break
							}

							e.wrapper.appendTrade(trade, cfg)
						} else {
							log.Error().
								Str("exchange", e.wrapper.name).
								Str("func", "processWs").
								Interface("trades", data).
								Msg("")
							return errors.New("cannot convert frame data field trade to map[string]interface{}")
						}
					}
				default:
					log.Error().
						Str("exchange", e.wrapper.name).
						Str("func", "processWs").
						Interface("trades", data[1]).
						Msg("")
					return errors.New("cannot convert frame data field trade to array")
				}
			} else {
				log.Error().
					Str("exchange", e.wrapper.name).
					Str("func", "processWs").
					Interface("symbol", data[2]).
					Msg("")
				return errors.New("cannot convert frame data field symbol to string")
			}
		} else {
			log.Error().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Interface("snapshot", data[0]).
				Msg("")
			return errors.New("cannot convert frame data field snapshot to bool")
		}
	}

	return err
}

func (e *Digifinex) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestURL+"ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", e.wrapper.exchangeCfg().RestURL+"trades")
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

func (e *Digifinex) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespDigifinex{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	r := rr.Tickers[0]
	price = r.TickerPrice

	return
}

func (e *Digifinex) processRestTrade(body io.ReadCloser) (trades []*storage.Trade, err error) {
	rr := restRespDigifinex{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for i := range rr.Trades {
		r := rr.Trades[i]

		trade := &storage.Trade{
			Exchange:  e.wrapper.name,
			TradeID:   strconv.FormatUint(r.TradeID, 10),
			Side:      r.Type,
			Size:      r.Amount,
			Price:     r.TradePrice,
			Timestamp: time.Unix(r.Date, 0).UTC(),
		}

		trades = append(trades, trade)
	}

	return
}
