package exchange

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/connector"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// StartDigifinex is for starting digifinex exchange functions.
func StartDigifinex(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newDigifinex(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "digifinex").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect digifinex exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				return fmt.Errorf("not able to connect digifinex exchange even after %v retry. please check the log for details", retry.Number)
			}

			log.Error().Str("exchange", "digifinex").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %v seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "digifinex").Msg("ctx canceled, return from StartDigifinex")
				return appCtx.Err()
			}
		}
	}
}

type digifinex struct {
	ws             connector.Websocket
	rest           *connector.REST
	connCfg        *config.Connection
	cfgMap         map[cfgLookupKey]cfgLookupVal
	channelIds     map[int][2]string
	ter            *storage.Terminal
	es             *storage.ElasticSearch
	mysql          *storage.MySQL
	wsTerTickers   chan []storage.Ticker
	wsTerTrades    chan []storage.Trade
	wsMysqlTickers chan []storage.Ticker
	wsMysqlTrades  chan []storage.Trade
	wsEsTickers    chan []storage.Ticker
	wsEsTrades     chan []storage.Trade
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

type wsRespDataDigifinex struct {
	ticker        wsRespDataDetailDigifinex
	trades        interface{}
	symbol        string
	method        string
	mktCommitName string
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

func newDigifinex(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	digifinexErrGroup, ctx := errgroup.WithContext(appCtx)

	d := digifinex{connCfg: connCfg}

	err := d.cfgLookup(markets)
	if err != nil {
		return err
	}

	var (
		wsCount   int
		restCount int
	)

	for _, market := range markets {
		for _, info := range market.Info {
			switch info.Connector {
			case "websocket":
				if wsCount == 0 {

					err = d.connectWs(ctx)
					if err != nil {
						return err
					}

					digifinexErrGroup.Go(func() error {
						return d.closeWsConnOnError(ctx)
					})

					digifinexErrGroup.Go(func() error {
						return d.pingWs(ctx)
					})

					digifinexErrGroup.Go(func() error {
						return d.readWs(ctx)
					})

					if d.ter != nil {
						digifinexErrGroup.Go(func() error {
							return d.wsTickersToTerminal(ctx)
						})
						digifinexErrGroup.Go(func() error {
							return d.wsTradesToTerminal(ctx)
						})
					}

					if d.mysql != nil {
						digifinexErrGroup.Go(func() error {
							return d.wsTickersToMySQL(ctx)
						})
						digifinexErrGroup.Go(func() error {
							return d.wsTradesToMySQL(ctx)
						})
					}

					if d.es != nil {
						digifinexErrGroup.Go(func() error {
							return d.wsTickersToES(ctx)
						})
						digifinexErrGroup.Go(func() error {
							return d.wsTradesToES(ctx)
						})
					}
				}

				key := cfgLookupKey{market: market.ID, channel: info.Channel}
				val := d.cfgMap[key]
				err = d.subWsChannel(market.ID, info.Channel, val.id)
				if err != nil {
					return err
				}
				wsCount++

			case "rest":
				if restCount == 0 {
					err = d.connectRest()
					if err != nil {
						return err
					}
				}

				var mktCommitName string
				if market.CommitName != "" {
					mktCommitName = market.CommitName
				} else {
					mktCommitName = market.ID
				}
				mktID := market.ID
				channel := info.Channel
				restPingIntSec := info.RESTPingIntSec
				digifinexErrGroup.Go(func() error {
					return d.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = digifinexErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (d *digifinex) cfgLookup(markets []config.Market) error {
	var id int

	// Configurations flat map is prepared for easy lookup later in the app.
	d.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
	d.channelIds = make(map[int][2]string)
	for _, market := range markets {
		var marketCommitName string
		if market.CommitName != "" {
			marketCommitName = market.CommitName
		} else {
			marketCommitName = market.ID
		}
		for _, info := range market.Info {
			key := cfgLookupKey{market: market.ID, channel: info.Channel}
			val := cfgLookupVal{}
			val.wsConsiderIntSec = info.WsConsiderIntSec
			for _, str := range info.Storages {
				switch str {
				case "terminal":
					val.terStr = true
					if d.ter == nil {
						d.ter = storage.GetTerminal()
						d.wsTerTickers = make(chan []storage.Ticker, 1)
						d.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if d.mysql == nil {
						d.mysql = storage.GetMySQL()
						d.wsMysqlTickers = make(chan []storage.Ticker, 1)
						d.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if d.es == nil {
						d.es = storage.GetElasticSearch()
						d.wsEsTickers = make(chan []storage.Ticker, 1)
						d.wsEsTrades = make(chan []storage.Trade, 1)
					}
				}
			}

			// Channel id is used to identify channel in subscribe success message of websocket server.
			id++
			d.channelIds[id] = [2]string{market.ID, info.Channel}
			val.id = id

			val.mktCommitName = marketCommitName
			d.cfgMap[key] = val
		}
	}
	return nil
}

func (d *digifinex) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &d.connCfg.WS, config.DigifinexWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	d.ws = ws
	log.Info().Str("exchange", "digifinex").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (d *digifinex) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := d.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// pingWs sends ping request to websocket server for every 25 seconds.
func (d *digifinex) pingWs(ctx context.Context) error {
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
			err = d.ws.Write(frame)
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

// subWsChannel sends channel subscription requests to the websocket server.
func (d *digifinex) subWsChannel(market string, channel string, id int) error {
	if channel == "trade" {
		channel = "trades"
	}
	sub := wsSubDigifinex{
		ID:     int64(id),
		Method: channel + ".subscribe",
		Params: [1]string{market},
	}
	frame, err := jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = d.ws.Write(frame)
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

// readWs reads ticker / trade data from websocket channels.
func (d *digifinex) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(d.cfgMap))
	for k, v := range d.cfgMap {
		cfgLookup[k] = v
	}

	cd := commitData{
		terTickers:   make([]storage.Ticker, 0, d.connCfg.Terminal.TickerCommitBuf),
		terTrades:    make([]storage.Trade, 0, d.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers: make([]storage.Ticker, 0, d.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:  make([]storage.Trade, 0, d.connCfg.MySQL.TradeCommitBuf),
		esTickers:    make([]storage.Ticker, 0, d.connCfg.ES.TickerCommitBuf),
		esTrades:     make([]storage.Trade, 0, d.connCfg.ES.TradeCommitBuf),
	}

	for {
		select {
		default:
			frame, err := d.ws.ReadTextOrZlibBinary()
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
				continue
			}

			wr := wsRespDigifinex{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.Error.Message != "" {
				log.Error().Str("exchange", "digifinex").Str("func", "readWs").Int("code", wr.Error.Code).Str("msg", wr.Error.Message).Msg("")
				return errors.New("digifinex websocket error")
			}
			if wr.ID != 0 {
				if _, ok := wr.Result.(string); ok {
					// Both pong frame and subscribe success response comes through the same field,
					// so the interface used.
				} else {
					log.Debug().Str("exchange", "digifinex").Str("func", "readWs").Str("market", d.channelIds[wr.ID][0]).Str("channel", d.channelIds[wr.ID][1]).Msg("channel subscribed")
				}
				continue
			}

			if wr.Method == "ticker.update" {
				wr.Method = "ticker"

				// Received data has different data structures for ticker and trades.
				dd := []wsRespDataDetailDigifinex{}
				if err := jsoniter.Unmarshal(wr.Params, &dd); err != nil {
					logErrStack(err)
					return err
				}

				for i := range dd {
					r := dd[i]
					data := wsRespDataDigifinex{
						ticker: r,
						symbol: r.Symbol,
					}

					// Consider frame only in configured interval, otherwise ignore it.
					key := cfgLookupKey{market: r.Symbol, channel: wr.Method}
					val := cfgLookup[key]
					if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
						val.wsLastUpdated = time.Now()
						data.mktCommitName = val.mktCommitName
						data.method = wr.Method
						cfgLookup[key] = val
					} else {
						continue
					}

					err := d.processWs(ctx, &data, &cd)
					if err != nil {
						return err
					}
				}
			} else {
				wr.Method = "trade"

				// Received data has different data structures for ticker and trades.
				data := make([]interface{}, 3)
				if err := jsoniter.Unmarshal(wr.Params, &data); err != nil {
					logErrStack(err)
					return err
				}

				// Ignoring initial snapshot trades.
				if snapshot, ok := data[0].(bool); ok {
					if snapshot {
						continue
					}

					if symbol, ok := data[2].(string); ok {
						data := wsRespDataDigifinex{
							trades: data[1],
							symbol: symbol,
						}

						// Consider frame only in configured interval, otherwise ignore it.
						key := cfgLookupKey{market: symbol, channel: wr.Method}
						val := cfgLookup[key]
						if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
							val.wsLastUpdated = time.Now()
							data.mktCommitName = val.mktCommitName
							data.method = wr.Method
							cfgLookup[key] = val
						} else {
							continue
						}

						err := d.processWs(ctx, &data, &cd)
						if err != nil {
							return err
						}
					} else {
						log.Error().Str("exchange", "digifinex").Str("func", "readWs").Interface("symbol", data[2]).Msg("")
						return errors.New("cannot convert frame data field symbol to string")
					}
				} else {
					log.Error().Str("exchange", "digifinex").Str("func", "readWs").Interface("snapshot", data[0]).Msg("")
					return errors.New("cannot convert frame data field snapshot to bool")
				}
			}

		// Return, if there is any error from another function or exchange.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// processWs receives ticker / trade data,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (d *digifinex) processWs(ctx context.Context, wr *wsRespDataDigifinex, cd *commitData) error {
	switch wr.method {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "digifinex"
		ticker.MktID = wr.symbol
		ticker.MktCommitName = wr.mktCommitName

		price, err := strconv.ParseFloat(wr.ticker.TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Price = price

		// Time sent is in milliseconds.
		ticker.Timestamp = time.Unix(0, wr.ticker.TickerTime*int64(time.Millisecond)).UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := d.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == d.connCfg.Terminal.TickerCommitBuf {
				select {
				case d.wsTerTickers <- cd.terTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.terTickersCount = 0
				cd.terTickers = nil
			}
		}
		if val.mysqlStr {
			cd.mysqlTickersCount++
			cd.mysqlTickers = append(cd.mysqlTickers, ticker)
			if cd.mysqlTickersCount == d.connCfg.MySQL.TickerCommitBuf {
				select {
				case d.wsMysqlTickers <- cd.mysqlTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.mysqlTickersCount = 0
				cd.mysqlTickers = nil
			}
		}
		if val.esStr {
			cd.esTickersCount++
			cd.esTickers = append(cd.esTickers, ticker)
			if cd.esTickersCount == d.connCfg.ES.TickerCommitBuf {
				select {
				case d.wsEsTickers <- cd.esTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.esTickersCount = 0
				cd.esTickers = nil
			}
		}
	case "trade":
		switch trades := wr.trades.(type) {
		case []interface{}:

			for _, data := range trades {
				if detail, ok := data.(map[string]interface{}); ok {
					trade := storage.Trade{}
					trade.Exchange = "digifinex"
					trade.MktID = wr.symbol
					trade.MktCommitName = wr.mktCommitName

					if tradeID, ok := detail["id"].(float64); ok {
						trade.TradeID = strconv.FormatFloat(tradeID, 'f', 0, 64)
					} else {
						log.Error().Str("exchange", "digifinex").Str("func", "readWs").Interface("trade id", data).Msg("")
						return errors.New("cannot convert frame data field trade id to float64")
					}

					if side, ok := detail["type"].(string); ok {
						trade.Side = side
					} else {
						log.Error().Str("exchange", "digifinex").Str("func", "readWs").Interface("side", data).Msg("")
						return errors.New("cannot convert frame data field side to string")
					}

					if size, ok := detail["amount"].(string); ok {
						size, err := strconv.ParseFloat(size, 64)
						if err != nil {
							logErrStack(err)
							return err
						}
						trade.Size = size
					} else {
						log.Error().Str("exchange", "digifinex").Str("func", "readWs").Interface("size", data).Msg("")
						return errors.New("cannot convert frame data field size to float64")
					}

					if price, ok := detail["price"].(string); ok {
						price, err := strconv.ParseFloat(price, 64)
						if err != nil {
							logErrStack(err)
							return err
						}
						trade.Price = price
					} else {
						log.Error().Str("exchange", "digifinex").Str("func", "readWs").Interface("price", data).Msg("")
						return errors.New("cannot convert frame data field price to float64")
					}

					if timestamp, ok := detail["time"].(float64); ok {
						intPart, fracPart := math.Modf(timestamp)
						trade.Timestamp = time.Unix(int64(intPart), int64(fracPart*1e9)).UTC()
					} else {
						log.Error().Str("exchange", "digifinex").Str("func", "readWs").Interface("timestamp", data).Msg("")
						return errors.New("cannot convert frame data field timestamp to float64")
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := d.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == d.connCfg.Terminal.TradeCommitBuf {
							select {
							case d.wsTerTrades <- cd.terTrades:
							case <-ctx.Done():
								return ctx.Err()
							}
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == d.connCfg.MySQL.TradeCommitBuf {
							select {
							case d.wsMysqlTrades <- cd.mysqlTrades:
							case <-ctx.Done():
								return ctx.Err()
							}
							cd.mysqlTradesCount = 0
							cd.mysqlTrades = nil
						}
					}
					if val.esStr {
						cd.esTradesCount++
						cd.esTrades = append(cd.esTrades, trade)
						if cd.esTradesCount == d.connCfg.ES.TradeCommitBuf {
							select {
							case d.wsEsTrades <- cd.esTrades:
							case <-ctx.Done():
								return ctx.Err()
							}
							cd.esTradesCount = 0
							cd.esTrades = nil
						}
					}
				} else {
					log.Error().Str("exchange", "digifinex").Str("func", "readWs").Interface("trades", data).Msg("")
					return errors.New("cannot convert frame data field trade to map[string]interface{}")
				}
			}
		default:
			log.Error().Str("exchange", "digifinex").Str("func", "readWs").Interface("trades", wr.trades).Msg("")
			return errors.New("cannot convert frame data field trade to array")
		}
	}
	return nil
}

func (d *digifinex) wsTickersToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-d.wsTerTickers:
			d.ter.CommitTickers(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *digifinex) wsTradesToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-d.wsTerTrades:
			d.ter.CommitTrades(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *digifinex) wsTickersToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-d.wsMysqlTickers:
			err := d.mysql.CommitTickers(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *digifinex) wsTradesToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-d.wsMysqlTrades:
			err := d.mysql.CommitTrades(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *digifinex) wsTickersToES(ctx context.Context) error {
	for {
		select {
		case data := <-d.wsEsTickers:
			err := d.es.CommitTickers(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *digifinex) wsTradesToES(ctx context.Context) error {
	for {
		select {
		case data := <-d.wsEsTrades:
			err := d.es.CommitTrades(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *digifinex) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	d.rest = rest
	log.Info().Str("exchange", "digifinex").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (d *digifinex) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error
	)

	cd := commitData{
		terTickers:   make([]storage.Ticker, 0, d.connCfg.Terminal.TickerCommitBuf),
		terTrades:    make([]storage.Trade, 0, d.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers: make([]storage.Ticker, 0, d.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:  make([]storage.Trade, 0, d.connCfg.MySQL.TradeCommitBuf),
		esTickers:    make([]storage.Ticker, 0, d.connCfg.ES.TickerCommitBuf),
		esTrades:     make([]storage.Trade, 0, d.connCfg.ES.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = d.rest.Request(ctx, "GET", config.DigifinexRESTBaseURL+"ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = d.rest.Request(ctx, "GET", config.DigifinexRESTBaseURL+"trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)

		// Querying for 100 trades.
		// If the configured interval gap is big, then maybe it will not return all the trades.
		// Better to use websocket.
		q.Add("limit", strconv.Itoa(100))
	}

	tick := time.NewTicker(time.Duration(interval) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:

			switch channel {
			case "ticker":
				req.URL.RawQuery = q.Encode()
				resp, err := d.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restRespDigifinex{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				r := rr.Tickers[0]
				ticker := storage.Ticker{
					Exchange:      "digifinex",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         r.TickerPrice,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := d.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == d.connCfg.Terminal.TickerCommitBuf {
						d.ter.CommitTickers(cd.terTickers)
						cd.terTickersCount = 0
						cd.terTickers = nil
					}
				}
				if val.mysqlStr {
					cd.mysqlTickersCount++
					cd.mysqlTickers = append(cd.mysqlTickers, ticker)
					if cd.mysqlTickersCount == d.connCfg.MySQL.TickerCommitBuf {
						err := d.mysql.CommitTickers(ctx, cd.mysqlTickers)
						if err != nil {
							if !errors.Is(err, ctx.Err()) {
								logErrStack(err)
							}
							return err
						}
						cd.mysqlTickersCount = 0
						cd.mysqlTickers = nil
					}
				}
				if val.esStr {
					cd.esTickersCount++
					cd.esTickers = append(cd.esTickers, ticker)
					if cd.esTickersCount == d.connCfg.ES.TickerCommitBuf {
						err := d.es.CommitTickers(ctx, cd.esTickers)
						if err != nil {
							if !errors.Is(err, ctx.Err()) {
								logErrStack(err)
							}
							return err
						}
						cd.esTickersCount = 0
						cd.esTickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := d.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restRespDigifinex{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr.Trades {
					r := rr.Trades[i]

					// Time sent is in seconds.
					timestamp := time.Unix(r.Date, 0).UTC()

					trade := storage.Trade{
						Exchange:      "digifinex",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       strconv.FormatUint(r.TradeID, 10),
						Side:          r.Type,
						Size:          r.Amount,
						Price:         r.TradePrice,
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := d.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == d.connCfg.Terminal.TradeCommitBuf {
							d.ter.CommitTrades(cd.terTrades)
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == d.connCfg.MySQL.TradeCommitBuf {
							err := d.mysql.CommitTrades(ctx, cd.mysqlTrades)
							if err != nil {
								if !errors.Is(err, ctx.Err()) {
									logErrStack(err)
								}
								return err
							}
							cd.mysqlTradesCount = 0
							cd.mysqlTrades = nil
						}
					}
					if val.esStr {
						cd.esTradesCount++
						cd.esTrades = append(cd.esTrades, trade)
						if cd.esTradesCount == d.connCfg.ES.TradeCommitBuf {
							err := d.es.CommitTrades(ctx, cd.esTrades)
							if err != nil {
								if !errors.Is(err, ctx.Err()) {
									logErrStack(err)
								}
								return err
							}
							cd.esTradesCount = 0
							cd.esTrades = nil
						}
					}
				}
			}

		// Return, if there is any error from another function or exchange.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
