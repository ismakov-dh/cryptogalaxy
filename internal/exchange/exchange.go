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
	config     *config.Config
	exchange   Exchange
	ws         *connector.Websocket
	rest       *connector.REST
	cfgMap     map[cfgLookupKey]*cfgLookupVal
	channelIds map[int][2]string
	storages   map[string]*Storage
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

func NewWrapper(name string, config *config.Config) *Wrapper {
	return &Wrapper{
		name:   name,
		config: config,
	}
}

func Start(appCtx context.Context, wrapper *Wrapper, exchange Exchange) error {
	var retryCount int
	retry := wrapper.exchangeCfg().Retry
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

	err := w.cfgLookup(errGroup, ctx)
	if err != nil {
		return err
	}

	var useWs bool
	for _, market := range w.config.Markets[w.name] {
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

				err := w.connectWs(ctx, w.exchangeCfg().WebsocketUrl)
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

				for _, market := range w.config.Markets[w.name] {
					for _, info := range market.Info {
						if info.Connector != "websocket" {
							continue
						}

						cfg, _, _ := w.getCfgMap(market.ID, info.Channel)
						err = w.subscribeWs(market.ID, info.Channel, cfg.id)
						if err != nil {
							logErrStack(err)
							cancel()
							continue INFINITE
						}

						subsCount++
						if w.exchangeCfg().WebsocketThreshold != 0 {
							if threshold := subsCount % w.exchangeCfg().WebsocketThreshold; threshold == 0 {
								log.Debug().
									Str("exchange", w.name).
									Int("count", threshold).
									Int("timeout", w.exchangeCfg().WebsocketTimeout).
									Msg("subscribe threshold reached, waiting")
								time.Sleep(time.Duration(w.exchangeCfg().WebsocketTimeout) * time.Second)
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

	for _, market := range w.config.Markets[w.name] {
		for _, info := range market.Info {
			if info.Connector != "rest" {
				continue
			}

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

	return errGroup.Wait()
}

func (w *Wrapper) cfgLookup(errGroup *errgroup.Group, ctx context.Context) error {
	var id int

	w.cfgMap = make(map[cfgLookupKey]*cfgLookupVal)
	w.channelIds = make(map[int][2]string)
	for _, market := range w.config.Markets[w.name] {
		var mktCommitName string
		if market.CommitName != "" {
			mktCommitName = market.CommitName
		} else {
			mktCommitName = market.ID
		}

		for _, info := range market.Info {
			id++
			w.channelIds[id] = [2]string{market.ID, info.Channel}

			key := cfgLookupKey{
				market:  market.ID,
				channel: info.Channel,
			}
			val := &cfgLookupVal{
				connector:        info.Connector,
				wsConsiderIntSec: info.WsConsiderIntSec,
				storages:         info.Storages,
				mktCommitName:    mktCommitName,
				id:               id,
			}
			w.cfgMap[key] = val

			for _, name := range info.Storages {
				_, ok := w.storages[name]
				if !ok {
					store, _ := storage.GetStore(name)
					buffers := w.config.CommitBuffers[name]
					s := NewStorage(ctx, store, buffers)
					s.Start(errGroup)
				}
			}
		}
	}
	return nil
}

func (w *Wrapper) connectWs(ctx context.Context, url string) error {
	ws, err := connector.NewWebsocket(ctx, &w.config.Connection.WS, url)
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

	cfg, _, _ := w.getCfgMap(market, channel)

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
	for _, storeName := range cfg.storages {
		w.storages[storeName].AppendTicker(ticker)
	}
	return
}

func (w *Wrapper) appendTrade(trade storage.Trade, cfg *cfgLookupVal) (err error) {
	for _, storeName := range cfg.storages {
		w.storages[storeName].AppendTrade(trade)
	}
	return
}

func (w *Wrapper) appendCandle(candle storage.Candle, cfg *cfgLookupVal) (err error) {
	for _, storeName := range cfg.storages {
		w.storages[storeName].AppendCandle(candle)
	}
	return
}

func (w *Wrapper) exchangeCfg() config.Exchange {
	return w.config.Exchanges[w.name]
}

func logErrStack(err error) {
	log.Error().Stack().Err(errors.WithStack(err)).Msg("")
}
