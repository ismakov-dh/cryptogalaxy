package exchange

import (
	"context"
	"fmt"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/connector"
	"golang.org/x/sync/errgroup"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type Wrapper struct {
	name       string
	config     *config.Exchange
	markets    []config.Market
	exchange   Exchange
	ws         *connector.Websocket
	rest       *connector.REST
	connCfg    *config.Connection
	cfgMap     map[cfgLookupKey]*cfgLookupVal
	channelIds map[int][2]string
	itv        *storage.InfluxTimeVal
	ter        *Storage
	mysql      *Storage
	es         *Storage
	influx     *Storage
	nats       *Storage
	clickhouse *Storage
	s3         *Storage
}

type Exchange interface {
	postConnectWs() (err error)
	pingWs(ctx context.Context) (err error)
	readWs() (frame []byte, err error)
	getWsSubscribeMessage(market string, channel string, id int) (frame []byte, err error)
	processWs(frame []byte) (err error)

	buildRestRequest(ctx context.Context, mktID string, channel string) (req *http.Request, err error)
	processRestTicker(body io.ReadCloser) (price float64, err error)
	processRestTrade(body io.ReadCloser) (trades []storage.Trade, err error)
}

func NewWrapper(name string, exchangeConfig *config.Exchange, markets []config.Market, connCfg *config.Connection) *Wrapper {
	return &Wrapper{
		name:    name,
		config:  exchangeConfig,
		markets: markets,
		connCfg: connCfg,
	}
}

func Start(appCtx context.Context, wrapper *Wrapper, exchange Exchange, retry *config.Retry) error {
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := wrapper.start(appCtx, exchange)
		if err != nil {
			log.Error().
				Err(err).
				Str("exchange", wrapper.name).
				Msg("error occurred")
			if retry.Number == 0 {
				return fmt.Errorf("not able to connect %s exchange. please check the log for details", wrapper.name)
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				err = fmt.Errorf("not able to connect to exchange even after %d retry", retry.Number)
				log.Error().
					Err(err).
					Str("exchange", wrapper.name).
					Msg("")
				return err
			}

			log.Error().
				Str("exchange", wrapper.name).
				Int("retry", retryCount).
				Msg(fmt.Sprintf("retrying functions in %d seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			case <-appCtx.Done():
				log.Error().
					Str("exchange", wrapper.name).
					Msg("ctx canceled, return from Start function")
				return appCtx.Err()
			}
		}
	}
}

func (w *Wrapper) start(appCtx context.Context, exchange Exchange) error {
	w.exchange = exchange
	errGroup, ctx := errgroup.WithContext(appCtx)

	err := w.cfgLookup(errGroup, ctx, w.markets)
	if err != nil {
		return err
	}

	var useWs bool
	for _, market := range w.markets {
		for _, info := range market.Info {
			if info.Connector == "websocket" {
				useWs = true
				break
			}
		}
	}

	if useWs {
		errGroup.Go(func() error {
			return w.closeWsOnError(ctx)
		})

		go func() {
		INFINITE:
			for {
				var subsCount int

				ctx, cancel := context.WithCancel(appCtx)
				errGroup, ctx := errgroup.WithContext(ctx)

				select {
				case <-time.After(2 * time.Second):
					log.Info().Str("exchange", w.name).Timestamp().Msg("connecting websocket")
				case <-appCtx.Done():
					cancel()
					return
				}

				err := w.connectWs(ctx, w.config.WebsocketUrl)
				if err != nil {
					logErrStack(err)
					cancel()
					continue
				}

				err = w.exchange.postConnectWs()
				if err != nil {
					logErrStack(err)
					cancel()
					continue
				}

				errGroup.Go(func() error {
					return w.exchange.pingWs(ctx)
				})

				errGroup.Go(func() error {
					return w.listenWs(ctx)
				})

				for _, market := range w.markets {
					for _, info := range market.Info {
						if info.Connector == "websocket" {
							key := cfgLookupKey{market: market.ID, channel: info.Channel}
							val := w.cfgMap[key]
							err = w.subscribeWs(market.ID, info.Channel, val.id)
							if err != nil {
								logErrStack(err)
								cancel()
								continue INFINITE
							}

							subsCount++

							if w.config.WebsocketThreshold != 0 {
								if threshold := subsCount % w.config.WebsocketThreshold; threshold == 0 {
									log.Debug().
										Str("exchange", w.name).
										Int("count", threshold).
										Int("timeout", w.config.WebsocketTimeout).
										Msg("subscribe threshold reached, waiting")
									time.Sleep(time.Duration(w.config.WebsocketTimeout) * time.Second)
								}
							}
						}
					}
				}

				if err := errGroup.Wait(); err != nil {
					logErrStack(err)
					cancel()
				}
			}
		}()
	}

	for _, market := range w.markets {
		for _, info := range market.Info {
			if info.Connector == "rest" {
				if w.rest == nil {
					if err := w.connectRest(); err != nil {
						return err
					}
				}

				errGroup.Go(func() error {
					return w.pollRest(ctx, market.ID, info.Channel, info.RESTPingIntSec)
				})
			}
		}
	}

	return errGroup.Wait()
}

func (w *Wrapper) cfgLookup(errGroup *errgroup.Group, ctx context.Context, markets []config.Market) error {
	var id int

	w.cfgMap = make(map[cfgLookupKey]*cfgLookupVal)
	w.channelIds = make(map[int][2]string)
	for _, market := range markets {
		var mktCommitName string
		if market.CommitName != "" {
			mktCommitName = market.CommitName
		} else {
			mktCommitName = market.ID
		}

		for _, info := range market.Info {
			key := cfgLookupKey{market: market.ID, channel: info.Channel}
			val := &cfgLookupVal{}
			val.connector = info.Connector
			val.wsConsiderIntSec = info.WsConsiderIntSec
			for _, str := range info.Storages {
				switch str {
				case "terminal":
					val.terStr = true
					if w.ter == nil {
						w.ter = NewStorage(
							ctx,
							storage.GetTerminal(),
							w.connCfg.Terminal.TickerCommitBuf,
							w.connCfg.Terminal.TradeCommitBuf,
							w.connCfg.Terminal.CandleCommitBuf,
						)
						w.ter.Start(errGroup)
					}
				case "mysql":
					val.mysqlStr = true
					if w.mysql == nil {
						w.mysql = NewStorage(
							ctx,
							storage.GetMySQL(),
							w.connCfg.MySQL.TickerCommitBuf,
							w.connCfg.MySQL.TradeCommitBuf,
							w.connCfg.MySQL.CandleCommitBuf,
						)
						w.mysql.Start(errGroup)
					}
				case "elastic_search":
					val.esStr = true
					if w.es == nil {
						w.es = NewStorage(
							ctx,
							storage.GetElasticSearch(),
							w.connCfg.ES.TickerCommitBuf,
							w.connCfg.ES.TradeCommitBuf,
							w.connCfg.ES.CandleCommitBuf,
						)
						w.es.Start(errGroup)
					}
				case "influxdb":
					val.influxStr = true
					if w.influx == nil {
						w.influx = NewStorage(
							ctx,
							storage.GetInfluxDB(),
							w.connCfg.InfluxDB.TickerCommitBuf,
							w.connCfg.InfluxDB.TradeCommitBuf,
							w.connCfg.InfluxDB.CandleCommitBuf,
						)
						w.itv = &storage.InfluxTimeVal{
							TickerMap: make(map[string]int64),
							TradeMap:  make(map[string]int64),
						}
						w.influx.Start(errGroup)
					}
				case "nats":
					val.natsStr = true
					if w.nats == nil {
						w.nats = NewStorage(
							ctx,
							storage.GetNATS(),
							w.connCfg.NATS.TickerCommitBuf,
							w.connCfg.NATS.TradeCommitBuf,
							w.connCfg.NATS.CandleCommitBuf,
						)
						w.nats.Start(errGroup)
					}
				case "clickhouse":
					val.clickHouseStr = true
					if w.clickhouse == nil {
						w.clickhouse = NewStorage(
							ctx,
							storage.GetClickHouse(),
							w.connCfg.ClickHouse.TickerCommitBuf,
							w.connCfg.ClickHouse.TradeCommitBuf,
							w.connCfg.ClickHouse.CandleCommitBuf,
						)
						w.clickhouse.Start(errGroup)
					}
				case "s3":
					val.s3Str = true
					if w.s3 == nil {
						w.s3 = NewStorage(
							ctx,
							storage.GetS3(),
							w.connCfg.S3.TickerCommitBuf,
							w.connCfg.S3.TradeCommitBuf,
							w.connCfg.S3.CandleCommitBuf,
						)
						w.s3.Start(errGroup)
					}
				}
			}

			id++
			w.channelIds[id] = [2]string{market.ID, info.Channel}
			val.id = id

			val.mktCommitName = mktCommitName
			w.cfgMap[key] = val
		}
	}
	return nil
}

func (w *Wrapper) connectWs(ctx context.Context, url string) error {
	ws, err := connector.NewWebsocket(ctx, &w.connCfg.WS, url)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	w.ws = &ws
	log.Info().Str("exchange", w.name).Msg("websocket connected")
	return nil
}

func (w *Wrapper) closeWsOnError(ctx context.Context) error {
	<-ctx.Done()
	err := w.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (w *Wrapper) subscribeWs(market string, channel string, id int) (err error) {
	frame, err := w.exchange.getWsSubscribeMessage(market, channel, id)
	if err != nil {
		return
	}
	err = w.ws.Write(frame)
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			err = errors.New("context canceled")
		} else {
			logErrStack(err)
		}
	}

	return
}

func (w *Wrapper) listenWs(ctx context.Context) error {
	for {
		select {
		default:
			frame, err := w.exchange.readWs()
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

			err = w.exchange.processWs(frame)
			if err != nil {
				logErrStack(err)
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *Wrapper) getCfgMap(market, channel string) (cfg *cfgLookupVal, ok bool, updateRequired bool) {
	key := cfgLookupKey{market: market, channel: channel}

	cfg, ok = w.cfgMap[key]
	if ok {
		if cfg.wsConsiderIntSec == 0 || time.Since(cfg.wsLastUpdated).Seconds() >= float64(cfg.wsConsiderIntSec) {
			cfg.wsLastUpdated = time.Now()
			updateRequired = true
		}
	}

	return
}

func (w *Wrapper) connectRest() (err error) {
	client, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return
	}
	w.rest = client
	log.Info().Str("exchange", w.name).Msg("REST connection setup is done")
	return
}

func (w *Wrapper) pollRest(ctx context.Context, market string, channel string, interval int) error {
	var (
		req *http.Request
		err error
	)

	key := cfgLookupKey{market: market, channel: channel}
	cfg := w.cfgMap[key]

	req, err = w.exchange.buildRestRequest(ctx, market, channel)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}

	tick := time.NewTicker(time.Duration(interval) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			resp, err := w.rest.Do(req)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}

			switch channel {
			case "ticker":
				price, err := w.exchange.processRestTicker(resp.Body)
				if err != nil {
					resp.Body.Close()
					logErrStack(err)
					return err
				}

				resp.Body.Close()

				ticker := storage.Ticker{
					Exchange:      w.name,
					MktID:         market,
					MktCommitName: cfg.mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				if cfg.influxStr {
					ticker.InfluxVal = w.getTickerInfluxTime(cfg.mktCommitName)
				}

				if err := w.appendTicker(ticker, cfg); err != nil {
					return err
				}
			case "trade":
				trades, err := w.exchange.processRestTrade(resp.Body)
				if err != nil {
					resp.Body.Close()
					logErrStack(err)
					return err
				}

				resp.Body.Close()

				for _, trade := range trades {
					trade.Exchange = w.name
					trade.MktID = market
					trade.MktCommitName = cfg.mktCommitName

					if cfg.influxStr {
						trade.InfluxVal = w.getTradeInfluxTime(cfg.mktCommitName)
					}

					if err := w.appendTrade(trade, cfg); err != nil {
						return err
					}
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *Wrapper) appendTicker(ticker storage.Ticker, cfg *cfgLookupVal) (err error) {
	if cfg.terStr {
		w.ter.AppendTicker(ticker)
	}
	if cfg.mysqlStr {
		w.mysql.AppendTicker(ticker)
	}
	if cfg.esStr {
		w.es.AppendTicker(ticker)
	}
	if cfg.influxStr {
		w.influx.AppendTicker(ticker)
	}
	if cfg.natsStr {
		w.nats.AppendTicker(ticker)
	}
	if cfg.clickHouseStr {
		w.clickhouse.AppendTicker(ticker)
	}
	if cfg.s3Str {
		w.s3.AppendTicker(ticker)
	}
	return
}

func (w *Wrapper) appendTrade(trade storage.Trade, cfg *cfgLookupVal) (err error) {
	if cfg.terStr {
		w.ter.AppendTrade(trade)
	}
	if cfg.mysqlStr {
		w.mysql.AppendTrade(trade)
	}
	if cfg.esStr {
		w.es.AppendTrade(trade)
	}
	if cfg.influxStr {
		w.influx.AppendTrade(trade)
	}
	if cfg.natsStr {
		w.nats.AppendTrade(trade)
	}
	if cfg.clickHouseStr {
		w.clickhouse.AppendTrade(trade)
	}
	if cfg.s3Str {
		w.s3.AppendTrade(trade)
	}
	return
}

func (w *Wrapper) appendCandle(candle storage.Candle, cfg *cfgLookupVal) (err error) {
	if cfg.terStr {
		w.ter.AppendCandle(candle)
	}
	if cfg.mysqlStr {
		w.mysql.AppendCandle(candle)
	}
	if cfg.esStr {
		w.es.AppendCandle(candle)
	}
	if cfg.influxStr {
		w.influx.AppendCandle(candle)
	}
	if cfg.natsStr {
		w.nats.AppendCandle(candle)
	}
	if cfg.clickHouseStr {
		w.clickhouse.AppendCandle(candle)
	}
	if cfg.s3Str {
		w.s3.AppendCandle(candle)
	}
	return
}

func (w *Wrapper) getTickerInfluxTime(mktCommitName string) (val int64) {
	val = w.itv.TradeMap[mktCommitName]
	if val == 0 || val == 999999 {
		val = 1
	} else {
		val++
	}
	w.itv.TradeMap[mktCommitName] = val

	return
}

func (w *Wrapper) getTradeInfluxTime(mktCommitName string) (val int64) {
	val = w.itv.TickerMap[mktCommitName]
	if val == 0 || val == 999999 {
		val = 1
	} else {
		val++
	}
	w.itv.TickerMap[mktCommitName] = val

	return
}

func (w *Wrapper) getCandleInfluxTime(mktCommitName string) (val int64) {
	val = w.itv.CandleMap[mktCommitName]
	if val == 0 || val == 999999 {
		val = 1
	} else {
		val++
	}
	w.itv.TickerMap[mktCommitName] = val

	return
}

func logErrStack(err error) {
	log.Error().Stack().Err(errors.WithStack(err)).Msg("")
}
