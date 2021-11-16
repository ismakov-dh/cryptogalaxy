package storage

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
)

// influxDB is for connecting and inserting data to influxDB.
type influxDB struct {
	WriteAPI  api.WriteAPIBlocking
	DeleteAPI api.DeleteAPI
	QueryAPI  api.QueryAPI
	Cfg       *config.InfluxDB
}

type InfluxTimeVal struct {

	// Sometime, ticker and trade data that we receive from the exchanges will have multiple records for the same timestamp.
	// This data is deleted automatically by the influxDB as the system identifies unique data points by
	// their measurement, tag set, and timestamp. Also we cannot add a unique id or timestamp as a new tag to the data set
	// as it may significantly affect the performance of the influxDB read / writes. So to solve this problem,
	// here we are adding 1 nanosecond to each timestamp entry of exchange and market combo till it reaches
	// 1 millisecond to have a unique timestamp entry for each data point. This will not change anything
	// as we are maintaining only millisecond precision ticker and trade records.
	// Of course this will break if we have more than a million trades per millisecond per market in an exchange. But we
	// are excluding that scenario.
	TickerMap map[string]int64
	TradeMap  map[string]int64
	CandleMap map[string]int64
}

var times = make(map[string]map[string]map[string]int64)

func getTimeVal(exchange string, channel string, mktCommitName string) int64 {
	exch, ok := times[exchange]
	if !ok {
		exch = make(map[string]map[string]int64)
		times[exchange] = exch
	}

	values, ok := exch[channel]
	if !ok {
		values = make(map[string]int64)
		times[exchange][channel] = values
	}

	value := values[mktCommitName]
	if value == 0 || value == 999999 {
		value = 1
	} else {
		value++
	}
	values[mktCommitName] = value

	return value
}

// InitInfluxDB initializes influxdb connection with configured values.
func InitInfluxDB(cfg *config.InfluxDB) (Store, error) {
	if _, ok := stores[INFLUXDB]; !ok {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns = cfg.MaxIdleConns
		httpClient := &http.Client{
			Timeout:   time.Duration(cfg.ReqTimeoutSec) * time.Second,
			Transport: t,
		}
		client := influxdb2.NewClientWithOptions(
			cfg.URL,
			cfg.Token,
			influxdb2.DefaultOptions().SetHTTPClient(httpClient).SetUseGZip(true),
		)
		writeAPI := client.WriteAPIBlocking(cfg.Organization, cfg.Bucket)
		deleteAPI := client.DeleteAPI()
		queryAPI := client.QueryAPI(cfg.Organization)

		var ctx context.Context
		if cfg.ReqTimeoutSec > 0 {
			timeoutCtx, cancel := context.WithTimeout(
				context.Background(),
				time.Duration(cfg.ReqTimeoutSec)*time.Second,
			)
			ctx = timeoutCtx
			defer cancel()
		} else {
			ctx = context.Background()
		}
		_, err := client.Ready(ctx)
		if err != nil {
			return nil, err
		}
		stores[INFLUXDB] = &influxDB{
			WriteAPI:  writeAPI,
			DeleteAPI: deleteAPI,
			QueryAPI:  queryAPI,
			Cfg:       cfg,
		}
	}
	return stores[INFLUXDB], nil
}

// CommitTickers batch inserts input ticker data to influxdb.
func (i *influxDB) CommitTickers(appCtx context.Context, data []*Ticker) error {
	var sb strings.Builder
	for i := range data {
		ticker := data[i]
		// See influxTimeVal (exchange.go) struct doc for details.
		// Microsecond time precision is removed before adding custom nanosecond and then
		// again converted back to nanosecond precision.
		sb.WriteString(
			fmt.Sprintf(
				"ticker,exchange=%v,market=%v price=%v %v\n",
				ticker.Exchange,
				ticker.MktCommitName,
				ticker.Price,
				((ticker.Timestamp.UnixNano()/1e6)*1e6)+getTimeVal(ticker.Exchange, "ticker", ticker.MktCommitName),
			),
		)
	}
	var ctx context.Context
	if i.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(appCtx, time.Duration(i.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	err := i.WriteAPI.WriteRecord(ctx, sb.String())
	if err != nil {
		return err
	}
	return nil
}

// CommitTrades batch inserts input trade data to influxdb.
func (i *influxDB) CommitTrades(appCtx context.Context, data []*Trade) error {
	var sb strings.Builder
	for i := range data {
		trade := data[i]
		// See influxTimeVal (exchange.go) struct doc for details.
		// Microsecond time precision is removed before adding custom nanosecond and then
		// again converted back to nanosecond precision.
		sb.WriteString(
			fmt.Sprintf(
				"trade,exchange=%v,market=%v,side=%v trade_id=\"%v\",size=%v,price=%v %v\n",
				trade.Exchange,
				trade.MktCommitName,
				trade.Side,
				trade.TradeID,
				trade.Size,
				trade.Price,
				((trade.Timestamp.UnixNano()/1e6)*1e6)+getTimeVal(trade.Exchange, "trade", trade.MktCommitName),
			),
		)
	}
	var ctx context.Context
	if i.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(appCtx, time.Duration(i.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	err := i.WriteAPI.WriteRecord(ctx, sb.String())
	if err != nil {
		return err
	}
	return nil
}

func (i *influxDB) CommitCandles(_ context.Context, _ []*Candle) error { return nil }
