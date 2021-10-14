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

// InfluxDB is for connecting and inserting data to InfluxDB.
type InfluxDB struct {
	WriteAPI  api.WriteAPIBlocking
	DeleteAPI api.DeleteAPI
	QuerryAPI api.QueryAPI
	Cfg       *config.InfluxDB
}

type InfluxTimeVal struct {

	// Sometime, ticker and trade data that we receive from the exchanges will have multiple records for the same timestamp.
	// This data is deleted automatically by the InfluxDB as the system identifies unique data points by
	// their measurement, tag set, and timestamp. Also we cannot add a unique id or timestamp as a new tag to the data set
	// as it may significantly affect the performance of the InfluxDB read / writes. So to solve this problem,
	// here we are adding 1 nanosecond to each timestamp entry of exchange and market combo till it reaches
	// 1 millisecond to have a unique timestamp entry for each data point. This will not change anything
	// as we are maintaining only millisecond precision ticker and trade records.
	// Of course this will break if we have more than a million trades per millisecond per market in an exchange. But we
	// are excluding that scenario.
	TickerMap map[string]int64
	TradeMap  map[string]int64
}

var influxdb InfluxDB

// InitInfluxDB initializes influxdb connection with configured values.
func InitInfluxDB(cfg *config.InfluxDB) (*InfluxDB, error) {
	if influxdb.WriteAPI == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns = cfg.MaxIdleConns
		httpClient := &http.Client{
			Timeout:   time.Duration(cfg.ReqTimeoutSec) * time.Second,
			Transport: t,
		}
		client := influxdb2.NewClientWithOptions(cfg.URL, cfg.Token,
			influxdb2.DefaultOptions().SetHTTPClient(httpClient).
				SetUseGZip(true))
		writeAPI := client.WriteAPIBlocking(cfg.Organization, cfg.Bucket)
		deleteAPI := client.DeleteAPI()
		queryAPI := client.QueryAPI(cfg.Organization)

		var ctx context.Context
		if cfg.ReqTimeoutSec > 0 {
			timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ReqTimeoutSec)*time.Second)
			ctx = timeoutCtx
			defer cancel()
		} else {
			ctx = context.Background()
		}
		_, err := client.Ready(ctx)
		if err != nil {
			return nil, err
		}
		influxdb = InfluxDB{
			WriteAPI:  writeAPI,
			DeleteAPI: deleteAPI,
			QuerryAPI: queryAPI,
			Cfg:       cfg,
		}
	}
	return &influxdb, nil
}

// GetInfluxDB returns already prepared influxdb instance.
func GetInfluxDB() *InfluxDB {
	return &influxdb
}

// CommitTickers batch inserts input ticker data to influxdb.
func (i *InfluxDB) CommitTickers(appCtx context.Context, data []Ticker) error {
	var sb strings.Builder
	for i := range data {
		ticker := data[i]
		// See influxTimeVal (exchange.go) struct doc for details.
		// Microsecond time precision is removed before adding custom nanosecond and then
		// again converted back to nanosecond precision.
		sb.WriteString(fmt.Sprintf("ticker,exchange=%v,market=%v price=%v %v\n", ticker.Exchange, ticker.MktCommitName, ticker.Price, ((ticker.Timestamp.UnixNano()/1e6)*1e6)+ticker.InfluxVal))
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
func (i *InfluxDB) CommitTrades(appCtx context.Context, data []Trade) error {
	var sb strings.Builder
	for i := range data {
		trade := data[i]
		// See influxTimeVal (exchange.go) struct doc for details.
		// Microsecond time precision is removed before adding custom nanosecond and then
		// again converted back to nanosecond precision.
		sb.WriteString(fmt.Sprintf("trade,exchange=%v,market=%v,side=%v trade_id=\"%v\",size=%v,price=%v %v\n", trade.Exchange, trade.MktCommitName, trade.Side, trade.TradeID, trade.Size, trade.Price, ((trade.Timestamp.UnixNano()/1e6)*1e6)+trade.InfluxVal))
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
