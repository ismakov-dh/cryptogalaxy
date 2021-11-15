package exchange

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"unicode"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type kraken struct {
	wrapper *Wrapper
}

type wsSubKraken struct {
	Event string         `json:"event"`
	Pair  [1]string      `json:"pair"`
	Sub   wsSubSubKraken `json:"subscription"`
}

type wsSubSubKraken struct {
	Name string `json:"name"`
}

type respKraken []interface{}

type wsEventRespKraken struct {
	ChannelName string `json:"channelName"`
	Event       string `json:"event"`
	Pair        string `json:"pair"`
	Status      string `json:"status"`
	ErrMsg      string `json:"errorMessage"`
	Sub         struct {
		Name string `json:"name"`
	} `json:"subscription"`
}

type restRespKraken struct {
	Result map[string]interface{} `json:"result"`
}

func NewKraken(wrapper *Wrapper) *kraken {
	return &kraken{wrapper: wrapper}
}

func (e *kraken) postConnectWs() error { return nil }

func (e *kraken) pingWs(_ context.Context) error { return nil }

func (e *kraken) readWs() ([]byte, error) {
	return e.wrapper.ws.Read()
}

func (e *kraken) getWsSubscribeMessage(market string, channel string, _ int) (frame []byte, err error) {
	sub := wsSubKraken{
		Event: "subscribe",
		Pair:  [1]string{market},
		Sub: wsSubSubKraken{
			Name: channel,
		},
	}

	frame, err = jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
	}

	return
}

// readWs reads ticker / trade data from websocket channels.
func (e *kraken) processWs(frame []byte) (err error) {
	var market, channel string
	var ok bool

	var tickerResp map[string]interface{}
	var tradeResp []interface{}

	temp := bytes.TrimLeftFunc(frame, unicode.IsSpace)

	if bytes.HasPrefix(temp, []byte("{")) {
		wr := wsEventRespKraken{}

		err = jsoniter.Unmarshal(frame, &wr)
		if err != nil {
			logErrStack(err)
			return
		}

		if wr.ErrMsg != "" {
			log.Error().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Str("msg", wr.ErrMsg).
				Msg("")
			return errors.New("kraken websocket error")
		}

		if wr.Status == "subscribed" {
			log.Debug().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Str("market", wr.Pair).
				Str("channel", wr.Sub.Name).
				Msg("channel subscribed")
			return
		}
	} else if bytes.HasPrefix(temp, []byte("[")) {
		wr := respKraken{}

		err = jsoniter.Unmarshal(frame, &wr)
		if err != nil {
			logErrStack(err)
			return
		}

		if channel, ok = wr[2].(string); ok {
			var ok bool
			if market, ok = wr[3].(string); ok {
				switch data := wr[1].(type) {
				case []interface{}:
					tradeResp = data
				default:
					if data, ok := data.(map[string]interface{}); ok {
						tickerResp = data
					} else {
						log.Error().
							Str("exchange", e.wrapper.name).
							Str("func", "processWs").
							Interface("ticker", wr[1]).
							Msg("")
						return errors.New("cannot convert frame ticker data to tickerRespKraken")
					}
				}
			} else {
				log.Error().
					Str("exchange", e.wrapper.name).
					Str("func", "processWs").
					Interface("market", wr[3]).
					Msg("")
				return errors.New("cannot convert frame data field market to string")
			}
		} else {
			log.Error().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Interface("channel", wr[2]).
				Msg("")
			return errors.New("cannot convert frame data field channel to string")
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
			Timestamp:     time.Now().UTC(),
		}

		if data, ok := tickerResp["c"].([]interface{}); ok {
			if str, ok := data[0].(string); ok {
				ticker.Price, err = strconv.ParseFloat(str, 64)
				if err != nil {
					logErrStack(err)
					return
				}

				if cfg.influxStr {
					ticker.InfluxVal = e.wrapper.getTickerInfluxTime(cfg.mktCommitName)
				}

				return e.wrapper.appendTicker(ticker, cfg)
			} else {
				log.Error().Str("exchange", e.wrapper.name).
					Str("func", "processWs").
					Interface("price", data[0]).
					Msg("")
				return errors.New("cannot convert ticker price to string")
			}
		} else {
			log.Error().
				Str("exchange", e.wrapper.name).
				Str("func", "processWs").
				Interface("price", tickerResp["c"]).
				Msg("")
			return errors.New("cannot convert ticker data to []interface")
		}
	case "trade":
		trade := storage.Trade{
			Exchange:      e.wrapper.name,
			MktID:         market,
			MktCommitName: cfg.mktCommitName,
		}

		var err error
		for _, trades := range tradeResp {
			if data, ok := trades.([]interface{}); ok {
				if side, ok := data[3].(string); ok {
					if side == "b" {
						trade.Side = "buy"
					} else {
						trade.Side = "sell"
					}
				} else {
					log.Error().
						Str("exchange", e.wrapper.name).
						Str("func", "processWs").
						Interface("side", data[3]).
						Msg("")
					return errors.New("cannot convert trade data field side to string")
				}

				if str, ok := data[1].(string); ok {
					trade.Size, err = strconv.ParseFloat(str, 64)
					if err != nil {
						logErrStack(err)
						continue
					}
				} else {
					log.Error().
						Str("exchange", e.wrapper.name).
						Str("func", "processWs").
						Interface("size", data[1]).
						Msg("")
					return errors.New("cannot convert trade data field size to string")
				}

				if str, ok := data[0].(string); ok {
					trade.Price, err = strconv.ParseFloat(str, 64)
					if err != nil {
						logErrStack(err)
						continue
					}
				} else {
					log.Error().
						Str("exchange", e.wrapper.name).
						Str("func", "processWs").
						Interface("price", data[0]).
						Msg("")
					return errors.New("cannot convert trade data field price to string")
				}

				// Time sent is in fractional seconds string format.
				if str, ok := data[2].(string); ok {
					timeFloat, err := strconv.ParseFloat(str, 64)
					if err != nil {
						logErrStack(err)
						continue
					}
					intPart, fracPart := math.Modf(timeFloat)
					trade.Timestamp = time.Unix(int64(intPart), int64(fracPart*1e9)).UTC()
				} else {
					log.Error().
						Str("exchange", e.wrapper.name).
						Str("func", "processWs").
						Interface("timestamp", data[2]).
						Msg("")
					return errors.New("cannot convert trade data field timestamp to string")
				}
			} else {
				log.Error().
					Str("exchange", e.wrapper.name).
					Str("func", "processWs").
					Interface("trades", trades).
					Msg("")
				return errors.New("cannot convert trades to []interface")
			}
		}
	}

	return
}

func (e *kraken) buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error) {
	var q url.Values
	var restUrl = e.wrapper.config.RestUrl

	switch channel {
	case "ticker":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"Ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("pair", mktID)
	case "trade":
		req, err = e.wrapper.rest.Request(ctx, "GET", restUrl+"Trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return
		}
		q = req.URL.Query()
		q.Add("pair", mktID)
	}

	req.URL.RawQuery = q.Encode()

	return
}

func (e *kraken) processRestTicker(body io.ReadCloser) (price float64, err error) {
	rr := restRespKraken{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for _, result := range rr.Result {
		if ticker, ok := result.(map[string]interface{}); ok {
			if data, ok := ticker["c"].([]interface{}); ok {
				if str, ok := data[0].(string); ok {
					price, err = strconv.ParseFloat(str, 64)
					if err != nil {
						logErrStack(err)
					}
				} else {
					log.Error().
						Str("exchange", e.wrapper.name).
						Str("func", "processREST").
						Interface("price", data[0]).
						Msg("")
					err = errors.New("cannot convert ticker price to string")
				}
			} else {
				log.Error().
					Str("exchange", e.wrapper.name).
					Str("func", "processREST").
					Interface("price", ticker["c"]).
					Msg("")
				err = errors.New("cannot convert ticker price to []interface")
			}
		} else {
			log.Error().
				Str("exchange", e.wrapper.name).
				Str("func", "processREST").
				Interface("data", rr.Result).
				Msg("")
			err = errors.New("cannot convert ticker result to map[string]interface")
		}
	}

	return
}

func (e *kraken) processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error) {
	rr := restRespKraken{}
	if err = jsoniter.NewDecoder(body).Decode(&rr); err != nil {
		logErrStack(err)
		return
	}

	for key, result := range rr.Result {
		if key == "last" {
			continue
		}
		if v, ok := result.([]interface{}); ok {
			for _, trades := range v {
				var err error
				if data, ok := trades.([]interface{}); ok {
					trade := storage.Trade{}

					if str, ok := data[3].(string); ok {
						if str == "b" {
							trade.Side = "buy"
						} else {
							trade.Side = "sell"
						}
					} else {
						log.Error().
							Str("exchange", e.wrapper.name).
							Str("func", "processREST").
							Interface("side", data[3]).
							Msg("cannot convert trade data field side to string")
						continue
					}

					if str, ok := data[1].(string); ok {
						trade.Size, err = strconv.ParseFloat(str, 64)
						if err != nil {
							logErrStack(err)
							continue
						}
					} else {
						log.Error().
							Str("exchange", e.wrapper.name).
							Str("func", "processREST").
							Interface("size", data[1]).
							Msg("cannot convert trade data field size to string")
						continue
					}

					if str, ok := data[0].(string); ok {
						trade.Price, err = strconv.ParseFloat(str, 64)
						if err != nil {
							logErrStack(err)
							continue
						}
					} else {
						log.Error().
							Str("exchange", e.wrapper.name).
							Str("func", "processREST").
							Interface("price", data[0]).
							Msg("cannot convert trade data field price to string")
						continue
					}

					if timeFloat, ok := data[2].(float64); ok {
						intPart, fracPart := math.Modf(timeFloat)
						trade.Timestamp = time.Unix(int64(intPart), int64(fracPart*1e9)).UTC()
					} else {
						log.Error().
							Str("exchange", e.wrapper.name).
							Str("func", "processREST").
							Interface("timestamp", data[2]).
							Msg("cannot convert trade data field timestamp to float")
						continue
					}
				} else {
					log.Error().
						Str("exchange", e.wrapper.name).
						Str("func", "processREST").
						Interface("trades", trades).
						Msg("cannot convert trades to []interface")
				}
			}
		} else {
			log.Error().
				Str("exchange", e.wrapper.name).
				Str("func", "processREST").
				Interface("trades", result).
				Msg("cannot convert trades result to []interface")
		}
	}

	return
}
