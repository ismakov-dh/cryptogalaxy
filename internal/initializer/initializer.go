package initializer

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/connector"
	"github.com/milkywaybrain/cryptogalaxy/internal/exchange"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"golang.org/x/sync/errgroup"
)

func Start(mainCtx context.Context, cfg *config.Config) error {
	var (
		logFilePath = cfg.Log.FilePath
		logFile *os.File
		err     error
	)

	if !strings.HasSuffix(logFilePath, ".log") {
		logFilePath = logFilePath + "_" + strconv.Itoa(int(time.Now().Unix())) + ".log"
	}

	logFile, err = os.OpenFile(cfg.Log.FilePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("not able to open or create log file: %s", cfg.Log.FilePath)
	}
	defer logFile.Close()

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	switch cfg.Log.Level {
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	fileLogger := zerolog.New(logFile).With().Timestamp().Logger()
	log.Logger = fileLogger
	log.Info().Msg("logger setup is done")

	for _, markets := range cfg.Markets {
		for _, market := range markets {
			for _, info := range market.Info {
				if info.Connector == "rest" {
					_ = connector.InitREST(&cfg.Connection.REST)
					if info.RESTPingIntSec < 1 {
						err = errors.New("rest_ping_interval_sec should be greater than zero")
						log.Error().Stack().Err(errors.WithStack(err)).Msg("")
						return err
					}
				}

				for _, _type := range info.Storages {
					_, ok := storage.GetStore(_type)
					if ok {
						continue
					}

					switch _type {
					case storage.TERMINAL:
						_ = storage.InitTerminal(os.Stdout)
					case storage.MYSQL:
						_, err = storage.InitMySQL(&cfg.Connection.MySQL)
					case storage.ELASTICSEARCH:
						_, err = storage.InitElasticSearch(&cfg.Connection.ES)
					case storage.INFLUXDB:
						_, err = storage.InitInfluxDB(&cfg.Connection.InfluxDB)
					case storage.NATS:
						_, err = storage.InitNATS(&cfg.Connection.NATS)
					case storage.CLICKHOUSE:
						_, err = storage.InitClickHouse(&cfg.Connection.ClickHouse)
					case storage.S3:
						_, err = storage.InitS3(&cfg.Connection.S3)
					}

					if err != nil {
						err = errors.Wrap(err, fmt.Sprintf("%s connection", _type))
						log.Error().Stack().Err(errors.WithStack(err)).Msg("")
						return err
					}

					log.Info().Str("storage", _type).Msg("connected")
				}
			}
		}
	}

	appErrGroup, appCtx := errgroup.WithContext(mainCtx)

LOOP:
	for exch, _ := range cfg.Markets {
		wrapper := exchange.NewWrapper(exch, cfg)

		var e exchange.Exchange
		switch exch {
		case "aax":
			e = exchange.NewAAX(wrapper)
		case "ascendex":
			e = exchange.NewAscendex(wrapper)
		case "bequant":
			e = exchange.NewBequant(wrapper)
		case "bhex":
			e = exchange.NewHbtc(wrapper)
		case "binance":
			e = exchange.NewBinance(wrapper)
		case "binance-us":
			e = exchange.NewBinance(wrapper)
		case "bitfinex":
			e = exchange.NewBitfinex(wrapper)
		case "bitmart":
			e = exchange.NewBitmart(wrapper)
		case "bitrue":
			e = exchange.NewBitrue(wrapper)
		case "bitstamp":
			e = exchange.NewBitstamp(wrapper)
		case "btse":
			e = exchange.NewBtse(wrapper)
		case "bybit":
			e = exchange.NewBybit(wrapper)
		case "coinbase-pro":
			e = exchange.NewCoinbasePro(wrapper)
		case "digifinex":
			e = exchange.NewDigifinex(wrapper)
		case "ftx":
			e = exchange.NewFtx(wrapper)
		case "ftx-us":
			e = exchange.NewFtx(wrapper)
		case "gateio":
			e = exchange.NewGateio(wrapper)
		case "gemini":
			e = exchange.NewGemini(wrapper)
		case "hitbtc":
			e = exchange.NewHitBTC(wrapper)
		case "huobi":
			e = exchange.NewHuobi(wrapper)
		case "kraken":
			e = exchange.NewKraken(wrapper)
		case "kucoin":
			e, err = exchange.NewKucoin(appCtx, wrapper)
			if err != nil {
				log.Error().Stack().Err(errors.WithStack(err)).Msg("error initializing kucoin")
				continue LOOP
			}
		case "mexo":
			e = exchange.NewMexo(wrapper)
		case "okex":
			e = exchange.NewOKEx(wrapper)
		case "probit":
			e = exchange.NewProbit(wrapper)
		}

		appErrGroup.Go(func() error {
			return exchange.Start(appCtx, wrapper, e)
		})
	}

	err = appErrGroup.Wait()
	if err != nil {
		log.Error().Msg("exiting the app")
		return err
	}
	return nil
}
