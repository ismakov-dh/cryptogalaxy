package test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/initializer"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	nc "github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

// TestCryptogalaxy is a combination of unit and integration test for the app.
func TestCryptogalaxy(t *testing.T) {

	// Load config file values.
	cfgPath := "./config_test.json"
	cfgFile, err := os.Open(cfgPath)
	if err != nil {
		t.Log("ERROR : Not able to find config file :", cfgPath)
		t.FailNow()
	}
	var cfg config.Config
	if err = jsoniter.NewDecoder(cfgFile).Decode(&cfg); err != nil {
		t.Log("ERROR : Not able to parse JSON from config file :", cfgPath)
		t.FailNow()
	}
	cfgFile.Close()

	// For testing, mysql schema name should start with the name test to avoid mistakenly messing up with production one.
	if !strings.HasPrefix(cfg.Connection.MySQL.Schema, "test") {
		t.Log("ERROR : mysql schema name should start with test for testing")
		t.FailNow()
	}

	// For testing, elastic search index name should start with the name test to avoid mistakenly messing up with
	// production one.
	if !strings.HasPrefix(cfg.Connection.ES.IndexName, "test") {
		t.Log("ERROR : elastic search index name should start with test for testing")
		t.FailNow()
	}

	// For testing, influxdb bucket name should start with the name test to avoid mistakenly messing up with
	// production one.
	if !strings.HasPrefix(cfg.Connection.InfluxDB.Bucket, "test") {
		t.Log("ERROR : influxdb bucket name should start with test for testing")
		t.FailNow()
	}

	// For testing, nats subject name should start with the name test to avoid mistakenly messing up with
	// production one.
	if !strings.HasPrefix(cfg.Connection.NATS.SubjectBaseName, "test") {
		t.Log("ERROR : nats subject base name should start with test for testing")
		t.FailNow()
	}

	// For testing, clickhouse schema name should start with the name test to avoid mistakenly messing up with production one.
	if !strings.HasPrefix(cfg.Connection.ClickHouse.Schema, "test") {
		t.Log("ERROR : clickhouse schema name should start with test for testing")
		t.FailNow()
	}

	// Terminal output we can't actually test, so make file as terminal output.
	fOutFile, err := os.Create("./data_test/ter_storage_test.txt")
	if err != nil {
		t.Log("ERROR : not able to create test terminal storage file : ./data_test/ter_storage_test.txt")
		t.FailNow()
	}
	_ = storage.InitTerminal(fOutFile)

	// Delete all data from mysql to have fresh one.
	mysql, err := storage.InitMySQL(&cfg.Connection.MySQL)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	var mysqlCtx context.Context
	if cfg.Connection.MySQL.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Connection.MySQL.ReqTimeoutSec)*time.Second)
		mysqlCtx = timeoutCtx
		defer cancel()
	} else {
		mysqlCtx = context.Background()
	}
	_, err = mysql.DB.ExecContext(mysqlCtx, "truncate ticker")
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	if cfg.Connection.MySQL.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Connection.MySQL.ReqTimeoutSec)*time.Second)
		mysqlCtx = timeoutCtx
		defer cancel()
	} else {
		mysqlCtx = context.Background()
	}
	_, err = mysql.DB.ExecContext(mysqlCtx, "truncate trade")
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	// Delete all data from elastic search to have fresh one.
	es, err := storage.InitElasticSearch(&cfg.Connection.ES)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	indexNames := make([]string, 0, 1)
	indexNames = append(indexNames, es.IndexName)
	var buf bytes.Buffer
	deleteQuery := []byte(`{"query":{"match_all":{}}}`)
	buf.Grow(len(deleteQuery))
	buf.Write(deleteQuery)

	var esCtx context.Context
	if cfg.Connection.ES.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Connection.ES.ReqTimeoutSec)*time.Second)
		esCtx = timeoutCtx
		defer cancel()
	} else {
		esCtx = context.Background()
	}
	resp, err := es.ES.DeleteByQuery(indexNames, bytes.NewReader(buf.Bytes()), es.ES.DeleteByQuery.WithContext(esCtx))
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Log("ERROR : " + fmt.Errorf("code : %v, status : %v", resp.StatusCode, resp.Status()).Error())
		t.FailNow()
	}
	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	// Delete all data from influxdb to have fresh one.
	influx, err := storage.InitInfluxDB(&cfg.Connection.InfluxDB)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	var influxCtx context.Context
	if cfg.Connection.InfluxDB.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Connection.InfluxDB.ReqTimeoutSec)*time.Second)
		influxCtx = timeoutCtx
		defer cancel()
	} else {
		influxCtx = context.Background()
	}
	start := time.Date(2021, time.Month(7), 25, 1, 1, 1, 1, time.UTC)
	end := time.Now().UTC()
	err = influx.DeleteAPI.DeleteWithName(influxCtx, cfg.Connection.InfluxDB.Organization, cfg.Connection.InfluxDB.Bucket, start, end, "")
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	// Subscribe to NATS subject. Write received data to a file.
	nOutFile, err := os.Create("./data_test/nats_storage_test.txt")
	if err != nil {
		t.Log("ERROR : not able to create test nats storage file : ./data_test/nats_storage_test.txt")
		t.FailNow()
	}
	nats, err := storage.InitNATS(&cfg.Connection.NATS)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}
	go natsSub(cfg.Connection.NATS.SubjectBaseName+".*", nOutFile, nats, t)

	// Delete all data from clickhouse to have fresh one.
	clickhouse, err := storage.InitClickHouse(&cfg.Connection.ClickHouse)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	_, err = clickhouse.DB.Exec("truncate ticker")
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	_, err = clickhouse.DB.Exec("truncate trade")
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	// Execute the app for 2 minute, which is good enough time to get the data from exchanges.
	// After that cancel app execution through context error.
	// If there is any actual error from app execution, then stop testing.
	testErrGroup, testCtx := errgroup.WithContext(context.Background())

	testErrGroup.Go(func() error {
		return initializer.Start(testCtx, &cfg)
	})

	t.Log("INFO : Executing app for 2 minute to get the data from exchanges.")
	testErrGroup.Go(func() error {
		tick := time.NewTicker(2 * time.Minute)
		defer tick.Stop()
		select {
		case <-tick.C:
			return errors.New("canceling app execution and starting data test")
		case <-testCtx.Done():
			return testCtx.Err()
		}
	})

	err = testErrGroup.Wait()
	if err != nil {
		if err.Error() == "canceling app execution and starting data test" {
			t.Log("INFO : " + err.Error())
		} else {
			t.Log("ERROR : " + err.Error())
			t.FailNow()
		}
	}

	// Close file which has been set as the terminal output in the previous step.
	err = fOutFile.Close()
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	// Close file which has been set as the nats output in the previous step.
	err = nOutFile.Close()
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	// Read data from different storage which has been set in the app execution stage.
	// Then verify it.

	// FTX exchange.
	var ftxFail bool

	terTickers := make(map[string]storage.Ticker)
	terTrades := make(map[string]storage.Trade)
	mysqlTickers := make(map[string]storage.Ticker)
	mysqlTrades := make(map[string]storage.Trade)
	esTickers := make(map[string]storage.Ticker)
	esTrades := make(map[string]storage.Trade)
	influxTickers := make(map[string]storage.Ticker)
	influxTrades := make(map[string]storage.Trade)
	natsTickers := make(map[string]storage.Ticker)
	natsTrades := make(map[string]storage.Trade)
	clickHouseTickers := make(map[string]storage.Ticker)
	clickHouseTrades := make(map[string]storage.Trade)

	err = readTerminal("ftx", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : ftx exchange function")
		ftxFail = true
	}

	if !ftxFail {
		err = readMySQL("ftx", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ftx exchange function")
			ftxFail = true
		}
	}

	if !ftxFail {
		err = readElasticSearch("ftx", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ftx exchange function")
			ftxFail = true
		}
	}

	if !ftxFail {
		err = readInfluxDB("ftx", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ftx exchange function")
			ftxFail = true
		}
	}

	if !ftxFail {
		err = readNATS("ftx", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ftx exchange function")
			ftxFail = true
		}
	}

	if !ftxFail {
		err = readClickHouse("ftx", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ftx exchange function")
			ftxFail = true
		}
	}

	if !ftxFail {
		err = verifyData("ftx", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ftx exchange function")
			ftxFail = true
		} else {
			t.Log("SUCCESS : ftx exchange function")
		}
	}

	// Coinbase-Pro exchange.
	var coinbaseProFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("coinbase-pro", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : coinbase-pro exchange function")
		coinbaseProFail = true
	}

	if !coinbaseProFail {
		err = readMySQL("coinbase-pro", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : coinbase-pro exchange function")
			coinbaseProFail = true
		}
	}

	if !coinbaseProFail {
		err = readElasticSearch("coinbase-pro", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : coinbase-pro exchange function")
			coinbaseProFail = true
		}
	}

	if !coinbaseProFail {
		err = readInfluxDB("coinbase-pro", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : coinbase-pro exchange function")
			coinbaseProFail = true
		}
	}

	if !coinbaseProFail {
		err = readNATS("coinbase-pro", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : coinbase-pro exchange function")
			coinbaseProFail = true
		}
	}

	if !coinbaseProFail {
		err = readClickHouse("coinbase-pro", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : coinbase-pro exchange function")
			coinbaseProFail = true
		}
	}

	if !coinbaseProFail {
		err = verifyData("ftx", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : coinbase-pro exchange function")
			coinbaseProFail = true
		} else {
			t.Log("SUCCESS : coinbase-pro exchange function")
		}
	}

	// Binance exchange.
	var binanceFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("binance", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : binance exchange function")
		binanceFail = true
	}

	if !binanceFail {
		err = readMySQL("binance", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance exchange function")
			binanceFail = true
		}
	}

	if !binanceFail {
		err = readElasticSearch("binance", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance exchange function")
			binanceFail = true
		}
	}

	if !binanceFail {
		err = readInfluxDB("binance", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance exchange function")
			binanceFail = true
		}
	}

	if !binanceFail {
		err = readNATS("binance", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance exchange function")
			binanceFail = true
		}
	}

	if !binanceFail {
		err = readClickHouse("binance", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance exchange function")
			binanceFail = true
		}
	}

	if !binanceFail {
		err = verifyData("binance", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance exchange function")
			binanceFail = true
		} else {
			t.Log("SUCCESS : binance exchange function")
		}
	}

	// Bitfinex exchange.
	var bitfinexFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("bitfinex", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : bitfinex exchange function")
		bitfinexFail = true
	}

	if !bitfinexFail {
		err = readMySQL("bitfinex", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitfinex exchange function")
			bitfinexFail = true
		}
	}

	if !bitfinexFail {
		err = readElasticSearch("bitfinex", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitfinex exchange function")
			bitfinexFail = true
		}
	}

	if !bitfinexFail {
		err = readInfluxDB("bitfinex", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitfinex exchange function")
			bitfinexFail = true
		}
	}

	if !bitfinexFail {
		err = readNATS("bitfinex", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitfinex exchange function")
			bitfinexFail = true
		}
	}

	if !bitfinexFail {
		err = readClickHouse("bitfinex", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitfinex exchange function")
			bitfinexFail = true
		}
	}

	if !bitfinexFail {
		err = verifyData("bitfinex", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitfinex exchange function")
			bitfinexFail = true
		} else {
			t.Log("SUCCESS : bitfinex exchange function")
		}
	}

	// Hbtc exchange.
	var hbtcFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("hbtc", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : hbtc exchange function")
		hbtcFail = true
	}

	if !hbtcFail {
		err = readMySQL("hbtc", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : hbtc exchange function")
			hbtcFail = true
		}
	}

	if !hbtcFail {
		err = readElasticSearch("hbtc", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : hbtc exchange function")
			hbtcFail = true
		}
	}

	if !hbtcFail {
		err = readInfluxDB("hbtc", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : hbtc exchange function")
			hbtcFail = true
		}
	}

	if !hbtcFail {
		err = readNATS("hbtc", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : hbtc exchange function")
			hbtcFail = true
		}
	}

	if !hbtcFail {
		err = readClickHouse("hbtc", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : hbtc exchange function")
			hbtcFail = true
		}
	}

	if !hbtcFail {
		err = verifyData("hbtc", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : hbtc exchange function")
			hbtcFail = true
		} else {
			t.Log("SUCCESS : hbtc exchange function")
		}
	}

	// Huobi exchange.
	var huobiFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("huobi", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : huobi exchange function")
		huobiFail = true
	}

	if !huobiFail {
		err = readMySQL("huobi", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : huobi exchange function")
			huobiFail = true
		}
	}

	if !huobiFail {
		err = readElasticSearch("huobi", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : huobi exchange function")
			huobiFail = true
		}
	}

	if !huobiFail {
		err = readInfluxDB("huobi", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : huobi exchange function")
			huobiFail = true
		}
	}

	if !huobiFail {
		err = readNATS("huobi", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : huobi exchange function")
			huobiFail = true
		}
	}

	if !huobiFail {
		err = readClickHouse("huobi", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : huobi exchange function")
			huobiFail = true
		}
	}

	if !huobiFail {
		err = verifyData("huobi", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : huobi exchange function")
			huobiFail = true
		} else {
			t.Log("SUCCESS : huobi exchange function")
		}
	}

	// Gateio exchange.
	var gateioFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("gateio", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : gateio exchange function")
		gateioFail = true
	}

	if !gateioFail {
		err = readMySQL("gateio", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gateio exchange function")
			gateioFail = true
		}
	}

	if !gateioFail {
		err = readElasticSearch("gateio", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gateio exchange function")
			gateioFail = true
		}
	}

	if !gateioFail {
		err = readInfluxDB("gateio", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gateio exchange function")
			gateioFail = true
		}
	}

	if !gateioFail {
		err = readNATS("gateio", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gateio exchange function")
			gateioFail = true
		}
	}

	if !gateioFail {
		err = readClickHouse("gateio", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gateio exchange function")
			gateioFail = true
		}
	}

	if !gateioFail {
		err = verifyData("gateio", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gateio exchange function")
			gateioFail = true
		} else {
			t.Log("SUCCESS : gateio exchange function")
		}
	}

	// Kucoin exchange.
	var kucoinFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("kucoin", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : kucoin exchange function")
		kucoinFail = true
	}

	if !kucoinFail {
		err = readMySQL("kucoin", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kucoin exchange function")
			kucoinFail = true
		}
	}

	if !kucoinFail {
		err = readElasticSearch("kucoin", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kucoin exchange function")
			kucoinFail = true
		}
	}

	if !kucoinFail {
		err = readInfluxDB("kucoin", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kucoin exchange function")
			kucoinFail = true
		}
	}

	if !kucoinFail {
		err = readNATS("kucoin", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kucoin exchange function")
			kucoinFail = true
		}
	}

	if !kucoinFail {
		err = readClickHouse("kucoin", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kucoin exchange function")
			kucoinFail = true
		}
	}

	if !kucoinFail {
		err = verifyData("kucoin", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kucoin exchange function")
			kucoinFail = true
		} else {
			t.Log("SUCCESS : kucoin exchange function")
		}
	}

	// Bitstamp exchange.
	var bitstampFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("bitstamp", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : bitstamp exchange function")
		bitstampFail = true
	}

	if !bitstampFail {
		err = readMySQL("bitstamp", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitstamp exchange function")
			bitstampFail = true
		}
	}

	if !bitstampFail {
		err = readElasticSearch("bitstamp", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitstamp exchange function")
			bitstampFail = true
		}
	}

	if !bitstampFail {
		err = readInfluxDB("bitstamp", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitstamp exchange function")
			bitstampFail = true
		}
	}

	if !bitstampFail {
		err = readNATS("bitstamp", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitstamp exchange function")
			bitstampFail = true
		}
	}

	if !bitstampFail {
		err = readClickHouse("bitstamp", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitstamp exchange function")
			bitstampFail = true
		}
	}

	if !bitstampFail {
		err = verifyData("bitstamp", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitstamp exchange function")
			bitstampFail = true
		} else {
			t.Log("SUCCESS : bitstamp exchange function")
		}
	}

	// Bybit exchange.
	var bybitFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("bybit", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : bybit exchange function")
		bybitFail = true
	}

	if !bybitFail {
		err = readMySQL("bybit", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bybit exchange function")
			bybitFail = true
		}
	}

	if !bybitFail {
		err = readElasticSearch("bybit", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bybit exchange function")
			bybitFail = true
		}
	}

	if !bybitFail {
		err = readInfluxDB("bybit", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bybit exchange function")
			bybitFail = true
		}
	}

	if !bybitFail {
		err = readNATS("bybit", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bybit exchange function")
			bybitFail = true
		}
	}

	if !bybitFail {
		err = readClickHouse("bybit", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bybit exchange function")
			bybitFail = true
		}
	}

	if !bybitFail {
		err = verifyData("bybit", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bybit exchange function")
			bybitFail = true
		} else {
			t.Log("SUCCESS : bybit exchange function")
		}
	}

	// Probit exchange.
	var probitFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("probit", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : probit exchange function")
		probitFail = true
	}

	if !probitFail {
		err = readMySQL("probit", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : probit exchange function")
			probitFail = true
		}
	}

	if !probitFail {
		err = readElasticSearch("probit", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : probit exchange function")
			probitFail = true
		}
	}

	if !probitFail {
		err = readInfluxDB("probit", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : probit exchange function")
			probitFail = true
		}
	}

	if !probitFail {
		err = readNATS("probit", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : probit exchange function")
			probitFail = true
		}
	}

	if !probitFail {
		err = readClickHouse("probit", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : probit exchange function")
			probitFail = true
		}
	}

	if !probitFail {
		err = verifyData("probit", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : probit exchange function")
			probitFail = true
		} else {
			t.Log("SUCCESS : probit exchange function")
		}
	}

	// Gemini exchange.
	var geminiFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("gemini", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : gemini exchange function")
		geminiFail = true
	}

	if !geminiFail {
		err = readMySQL("gemini", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gemini exchange function")
			geminiFail = true
		}
	}

	if !geminiFail {
		err = readElasticSearch("gemini", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gemini exchange function")
			geminiFail = true
		}
	}

	if !geminiFail {
		err = readInfluxDB("gemini", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gemini exchange function")
			geminiFail = true
		}
	}

	if !geminiFail {
		err = readNATS("gemini", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gemini exchange function")
			geminiFail = true
		}
	}

	if !geminiFail {
		err = readClickHouse("gemini", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gemini exchange function")
			geminiFail = true
		}
	}

	if !geminiFail {
		err = verifyData("gemini", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : gemini exchange function")
			geminiFail = true
		} else {
			t.Log("SUCCESS : gemini exchange function")
		}
	}

	// Bitmart exchange.
	var bitmartFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("bitmart", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : bitmart exchange function")
		bitmartFail = true
	}

	if !bitmartFail {
		err = readMySQL("bitmart", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitmart exchange function")
			bitmartFail = true
		}
	}

	if !bitmartFail {
		err = readElasticSearch("bitmart", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitmart exchange function")
			bitmartFail = true
		}
	}

	if !bitmartFail {
		err = readInfluxDB("bitmart", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitmart exchange function")
			bitmartFail = true
		}
	}

	if !bitmartFail {
		err = readNATS("bitmart", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitmart exchange function")
			bitmartFail = true
		}
	}

	if !bitmartFail {
		err = readClickHouse("bitmart", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitmart exchange function")
			bitmartFail = true
		}
	}

	if !bitmartFail {
		err = verifyData("bitmart", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitmart exchange function")
			bitmartFail = true
		} else {
			t.Log("SUCCESS : bitmart exchange function")
		}
	}

	// Digifinex exchange.
	var digifinexFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("digifinex", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : digifinex exchange function")
		digifinexFail = true
	}

	if !digifinexFail {
		err = readMySQL("digifinex", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : digifinex exchange function")
			digifinexFail = true
		}
	}

	if !digifinexFail {
		err = readElasticSearch("digifinex", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : digifinex exchange function")
			digifinexFail = true
		}
	}

	if !digifinexFail {
		err = readInfluxDB("digifinex", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : digifinex exchange function")
			digifinexFail = true
		}
	}

	if !digifinexFail {
		err = readNATS("digifinex", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : digifinex exchange function")
			digifinexFail = true
		}
	}

	if !digifinexFail {
		err = readClickHouse("digifinex", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : digifinex exchange function")
			digifinexFail = true
		}
	}

	if !digifinexFail {
		err = verifyData("digifinex", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : digifinex exchange function")
			digifinexFail = true
		} else {
			t.Log("SUCCESS : digifinex exchange function")
		}
	}

	// Ascendex exchange.
	var ascendexFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("ascendex", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : ascendex exchange function")
		ascendexFail = true
	}

	if !ascendexFail {
		err = readMySQL("ascendex", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ascendex exchange function")
			ascendexFail = true
		}
	}

	if !ascendexFail {
		err = readElasticSearch("ascendex", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ascendex exchange function")
			ascendexFail = true
		}
	}

	if !ascendexFail {
		err = readInfluxDB("ascendex", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ascendex exchange function")
			ascendexFail = true
		}
	}

	if !ascendexFail {
		err = readNATS("ascendex", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ascendex exchange function")
			ascendexFail = true
		}
	}

	if !ascendexFail {
		err = readClickHouse("ascendex", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ascendex exchange function")
			ascendexFail = true
		}
	}

	if !ascendexFail {
		err = verifyData("ascendex", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ascendex exchange function")
			ascendexFail = true
		} else {
			t.Log("SUCCESS : ascendex exchange function")
		}
	}

	// Kraken exchange.
	var krakenFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("kraken", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : kraken exchange function")
		krakenFail = true
	}

	if !krakenFail {
		err = readMySQL("kraken", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kraken exchange function")
			krakenFail = true
		}
	}

	if !krakenFail {
		err = readElasticSearch("kraken", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kraken exchange function")
			krakenFail = true
		}
	}

	if !krakenFail {
		err = readInfluxDB("kraken", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kraken exchange function")
			krakenFail = true
		}
	}

	if !krakenFail {
		err = readNATS("kraken", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kraken exchange function")
			krakenFail = true
		}
	}

	if !krakenFail {
		err = readClickHouse("kraken", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kraken exchange function")
			krakenFail = true
		}
	}

	if !krakenFail {
		err = verifyData("kraken", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : kraken exchange function")
			krakenFail = true
		} else {
			t.Log("SUCCESS : kraken exchange function")
		}
	}

	// Binance US exchange.
	var binanceUSFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("binance-us", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : binance-us exchange function")
		binanceUSFail = true
	}

	if !binanceUSFail {
		err = readMySQL("binance-us", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance-us exchange function")
			binanceUSFail = true
		}
	}

	if !binanceUSFail {
		err = readElasticSearch("binance-us", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance-us exchange function")
			binanceUSFail = true
		}
	}

	if !binanceUSFail {
		err = readInfluxDB("binance-us", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance-us exchange function")
			binanceUSFail = true
		}
	}

	if !binanceUSFail {
		err = readNATS("binance-us", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance-us exchange function")
			binanceUSFail = true
		}
	}

	if !binanceUSFail {
		err = readClickHouse("binance-us", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance-us exchange function")
			binanceUSFail = true
		}
	}

	if !binanceUSFail {
		err = verifyData("binance-us", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance-us exchange function")
			binanceUSFail = true
		} else {
			t.Log("SUCCESS : binance-us exchange function")
		}
	}

	// OKEx exchange.
	var okexFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)
	influxTickers = make(map[string]storage.Ticker)
	influxTrades = make(map[string]storage.Trade)
	natsTickers = make(map[string]storage.Ticker)
	natsTrades = make(map[string]storage.Trade)
	clickHouseTickers = make(map[string]storage.Ticker)
	clickHouseTrades = make(map[string]storage.Trade)

	err = readTerminal("okex", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : okex exchange function")
		okexFail = true
	}

	if !okexFail {
		err = readMySQL("okex", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : okex exchange function")
			okexFail = true
		}
	}

	if !okexFail {
		err = readElasticSearch("okex", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : okex exchange function")
			okexFail = true
		}
	}

	if !okexFail {
		err = readInfluxDB("okex", influxTickers, influxTrades, influx)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : okex exchange function")
			okexFail = true
		}
	}

	if !okexFail {
		err = readNATS("okex", natsTickers, natsTrades)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : okex exchange function")
			okexFail = true
		}
	}

	if !okexFail {
		err = readClickHouse("okex", clickHouseTickers, clickHouseTrades, clickhouse)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : okex exchange function")
			okexFail = true
		}
	}

	if !okexFail {
		err = verifyData("okex", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, influxTickers, influxTrades, natsTickers, natsTrades, clickHouseTickers, clickHouseTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : okex exchange function")
			okexFail = true
		} else {
			t.Log("SUCCESS : okex exchange function")
		}
	}

	if ftxFail || coinbaseProFail || binanceFail || bitfinexFail || hbtcFail || huobiFail || gateioFail || kucoinFail || bitstampFail || bybitFail || probitFail || geminiFail || bitmartFail || digifinexFail || ascendexFail || krakenFail || binanceUSFail || okexFail {
		t.Log("INFO : May be 2 minute app execution time is not good enough to get the data. Try to increase it before actual debugging.")
	}
}

// readTerminal reads ticker and trade data for an exchange from a file, which has been set as terminal output,
// into passed in maps.
func readTerminal(exchName string, terTickers map[string]storage.Ticker, terTrades map[string]storage.Trade) error {
	outFile, err := os.OpenFile("./data_test/ter_storage_test.txt", os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer outFile.Close()
	rd := bufio.NewReader(outFile)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if line == "\n" {
			continue
		}
		words := strings.Fields(line)
		if words[1] == exchName {
			switch words[0] {
			case "Ticker":
				key := words[2]
				price, err := strconv.ParseFloat(words[3], 64)
				if err != nil {
					return err
				}
				val := storage.Ticker{
					Price: price,
				}
				terTickers[key] = val
			case "Trade":
				key := words[2]
				size, err := strconv.ParseFloat(words[3], 64)
				if err != nil {
					return err
				}
				price, err := strconv.ParseFloat(words[4], 64)
				if err != nil {
					return err
				}
				val := storage.Trade{
					Size:  size,
					Price: price,
				}
				terTrades[key] = val
			}
		}
	}
	return nil
}

// readMySQL reads ticker and trade data for an exchange from mysql into passed in maps.
func readMySQL(exchName string, mysqlTickers map[string]storage.Ticker, mysqlTrades map[string]storage.Trade, mysql *storage.MySQL) error {
	var ctx context.Context
	if mysql.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(mysql.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	tickerRows, err := mysql.DB.QueryContext(ctx, "select market, avg(price) as price from ticker where exchange = ? group by market", exchName)
	if err != nil {
		return err
	}
	defer tickerRows.Close()
	for tickerRows.Next() {
		var market string
		var price float64
		err = tickerRows.Scan(&market, &price)
		if err != nil {
			return err
		}
		val := storage.Ticker{
			Price: price,
		}
		mysqlTickers[market] = val
	}
	err = tickerRows.Err()
	if err != nil {
		return err
	}

	if mysql.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(mysql.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	tradeRows, err := mysql.DB.QueryContext(ctx, "select market, avg(size) as size, avg(price) as price from trade where exchange = ? group by market", exchName)
	if err != nil {
		return err
	}
	defer tradeRows.Close()
	for tradeRows.Next() {
		var market string
		var size float64
		var price float64
		err = tradeRows.Scan(&market, &size, &price)
		if err != nil {
			return err
		}
		val := storage.Trade{
			Size:  size,
			Price: price,
		}
		mysqlTrades[market] = val
	}
	err = tradeRows.Err()
	if err != nil {
		return err
	}
	return nil
}

// readElasticSearch reads ticker and trade data for an exchange from elastic search into passed in maps.
func readElasticSearch(exchName string, esTickers map[string]storage.Ticker, esTrades map[string]storage.Trade, es *storage.ElasticSearch) error {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"size": 0,
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"exchange": exchName,
			},
		},
		"aggs": map[string]interface{}{
			"by_channel": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "channel",
				},
				"aggs": map[string]interface{}{
					"by_market": map[string]interface{}{
						"terms": map[string]interface{}{
							"field": "market",
						},
						"aggs": map[string]interface{}{
							"size": map[string]interface{}{
								"avg": map[string]interface{}{
									"field": "size",
								},
							},
							"price": map[string]interface{}{
								"avg": map[string]interface{}{
									"field": "price",
								},
							},
						},
					},
				},
			},
		},
	}
	if err := jsoniter.NewEncoder(&buf).Encode(query); err != nil {
		return err
	}

	var ctx context.Context
	if es.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(es.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	res, err := es.ES.Search(
		es.ES.Search.WithIndex(es.IndexName),
		es.ES.Search.WithBody(&buf),
		es.ES.Search.WithTrackTotalHits(false),
		es.ES.Search.WithPretty(),
		es.ES.Search.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return err
	}

	var esResp esResp
	if err := json.NewDecoder(res.Body).Decode(&esResp); err != nil {
		return err
	}
	for _, channel := range esResp.Aggregations.ByChannel.Buckets {
		switch channel.Key {
		case "ticker":
			for _, market := range channel.ByMarket.Buckets {
				val := storage.Ticker{
					Price: market.Price.Value,
				}
				esTickers[market.Key] = val
			}
		case "trade":
			for _, market := range channel.ByMarket.Buckets {
				val := storage.Trade{
					Size:  market.Size.Value,
					Price: market.Price.Value,
				}
				esTrades[market.Key] = val
			}
		}
	}
	return nil
}

// readInfluxDB reads ticker and trade data for an exchange from influxdb into passed in maps.
func readInfluxDB(exchName string, influxTickers map[string]storage.Ticker, influxTrades map[string]storage.Trade, influx *storage.InfluxDB) error {
	var ctx context.Context
	if influx.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(influx.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	result, err := influx.QuerryAPI.Query(ctx, `
	                         from(bucket: "`+influx.Cfg.Bucket+`")
                             |> range(start: -1h)
                             |> filter(fn: (r) =>
                                  r._measurement == "ticker" and
                                  r._field == "price" and
                                  r.exchange == "`+exchName+`" 
                                )
                             |> group(columns: ["market"])
                             |> mean()
                        `)
	if err != nil {
		return err
	}
	for result.Next() {
		market, ok := result.Record().ValueByKey("market").(string)
		if !ok {
			return errors.New("cannot convert influxdb trade market to string")
		}
		price, ok := result.Record().Value().(float64)
		if !ok {
			return errors.New("cannot convert influxdb trade price to float")
		}
		val := storage.Ticker{
			Price: price,
		}
		influxTickers[market] = val
	}
	err = result.Err()
	if err != nil {
		return err
	}

	if influx.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(influx.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	result, err = influx.QuerryAPI.Query(ctx, `
	                         from(bucket: "`+influx.Cfg.Bucket+`")
                             |> range(start: -1h)
                             |> filter(fn: (r) =>
                                  r._measurement == "trade" and
                                  (r._field == "size" or r._field == "price") and
                                  r.exchange == "`+exchName+`" 
                                )
                             |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                             |> group(columns: ["market"])
                             |> reduce(
                                  identity: {
                                      count:  1.0,
                                      size_sum: 0.0,
                                      size_avg: 0.0,
                                      price_sum: 0.0,
                                      price_avg: 0.0
                                  },
                                  fn: (r, accumulator) => ({
                                      count:  accumulator.count + 1.0,
                                      size_sum: accumulator.size_sum + r.size,
                                      size_avg: accumulator.size_sum / accumulator.count,
                                      price_sum: accumulator.price_sum + r.price,
                                      price_avg: accumulator.price_sum / accumulator.count
                                  })
                                )
                             |> drop(columns: ["count", "size_sum", "price_sum"])
	                    `)
	if err != nil {
		return err
	}
	for result.Next() {
		market, ok := result.Record().ValueByKey("market").(string)
		if !ok {
			return errors.New("cannot convert influxdb trade market to string")
		}
		size, ok := result.Record().ValueByKey("size_avg").(float64)
		if !ok {
			return errors.New("cannot convert influxdb trade size to float")
		}
		price, ok := result.Record().ValueByKey("price_avg").(float64)
		if !ok {
			return errors.New("cannot convert influxdb trade price to float")
		}
		val := storage.Trade{
			Size:  size,
			Price: price,
		}
		influxTrades[market] = val
	}
	err = result.Err()
	if err != nil {
		return err
	}
	return nil
}

// readNATS reads ticker and trade data for an exchange from a file, which has been set as nats output,
// into passed in maps.
func readNATS(exchName string, natsTickers map[string]storage.Ticker, natsTrades map[string]storage.Trade) error {
	outFile, err := os.OpenFile("./data_test/nats_storage_test.txt", os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer outFile.Close()
	rd := bufio.NewReader(outFile)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if line == "\n" {
			continue
		}
		words := strings.Fields(line)
		if words[1] == exchName {
			switch words[0] {
			case "Ticker":
				key := words[2]
				price, err := strconv.ParseFloat(words[3], 64)
				if err != nil {
					return err
				}
				val := storage.Ticker{
					Price: price,
				}
				natsTickers[key] = val
			case "Trade":
				key := words[2]
				size, err := strconv.ParseFloat(words[3], 64)
				if err != nil {
					return err
				}
				price, err := strconv.ParseFloat(words[4], 64)
				if err != nil {
					return err
				}
				val := storage.Trade{
					Size:  size,
					Price: price,
				}
				natsTrades[key] = val
			}
		}
	}
	return nil
}

// readClickHouse reads ticker and trade data for an exchange from clickhouse into passed in maps.
func readClickHouse(exchName string, clickHouseTickers map[string]storage.Ticker, clickHouseTrades map[string]storage.Trade, clickHouse *storage.ClickHouse) error {
	tickerRows, err := clickHouse.DB.Query("select market, avg(price) as price from ticker where exchange = ? group by market", exchName)
	if err != nil {
		return err
	}
	defer tickerRows.Close()
	for tickerRows.Next() {
		var market string
		var price float64
		err = tickerRows.Scan(&market, &price)
		if err != nil {
			return err
		}
		val := storage.Ticker{
			Price: price,
		}
		clickHouseTickers[market] = val
	}
	err = tickerRows.Err()
	if err != nil {
		return err
	}

	tradeRows, err := clickHouse.DB.Query("select market, avg(size) as size, avg(price) as price from trade where exchange = ? group by market", exchName)
	if err != nil {
		return err
	}
	defer tradeRows.Close()
	for tradeRows.Next() {
		var market string
		var size float64
		var price float64
		err = tradeRows.Scan(&market, &size, &price)
		if err != nil {
			return err
		}
		val := storage.Trade{
			Size:  size,
			Price: price,
		}
		clickHouseTrades[market] = val
	}
	err = tradeRows.Err()
	if err != nil {
		return err
	}
	return nil
}

// verifyData checks whether all the configured storage system for an exchange got the required data or not.
func verifyData(exchName string, terTickers map[string]storage.Ticker, terTrades map[string]storage.Trade,
	mysqlTickers map[string]storage.Ticker, mysqlTrades map[string]storage.Trade,
	esTickers map[string]storage.Ticker, esTrades map[string]storage.Trade,
	influxTickers map[string]storage.Ticker, influxTrades map[string]storage.Trade,
	natsTickers map[string]storage.Ticker, natsTrades map[string]storage.Trade,
	clickHouseTickers map[string]storage.Ticker, clickHouseTrades map[string]storage.Trade,
	cfg *config.Config) error {

	for _, exch := range cfg.Exchanges {
		if exch.Name == exchName {
			for _, market := range exch.Markets {
				var marketCommitName string
				if market.CommitName != "" {
					marketCommitName = market.CommitName
				} else {
					marketCommitName = market.ID
				}
				for _, info := range market.Info {
					switch info.Channel {
					case "ticker":
						for _, str := range info.Storages {
							switch str {
							case "terminal":
								terTicker := terTickers[marketCommitName]
								if terTicker.Price <= 0 {
									return fmt.Errorf("%s ticker data stored in terminal is not complete", market.ID)
								}
							case "mysql":
								sqlTicker := mysqlTickers[marketCommitName]
								if sqlTicker.Price <= 0 {
									return fmt.Errorf("%s ticker data stored in mysql is not complete", market.ID)
								}
							case "elastic_search":
								esTicker := esTickers[marketCommitName]
								if esTicker.Price <= 0 {
									return fmt.Errorf("%s ticker data stored in elastic search is not complete", market.ID)
								}
							case "influxdb":
								influxTicker := influxTickers[marketCommitName]
								if influxTicker.Price <= 0 {
									return fmt.Errorf("%s ticker data stored in influxdb is not complete", market.ID)
								}
							case "nats":
								natsTicker := natsTickers[marketCommitName]
								if natsTicker.Price <= 0 {
									return fmt.Errorf("%s ticker data stored in nats is not complete", market.ID)
								}
							case "clickhouse":
								clickHouseTicker := clickHouseTickers[marketCommitName]
								if clickHouseTicker.Price <= 0 {
									return fmt.Errorf("%s ticker data stored in clickhouse is not complete", market.ID)
								}
							}
						}
					case "trade":
						for _, str := range info.Storages {
							switch str {
							case "terminal":
								terTrade := terTrades[marketCommitName]
								if terTrade.Size <= 0 || terTrade.Price <= 0 {
									return fmt.Errorf("%s trade data stored in terminal is not complete", market.ID)
								}
							case "mysql":
								sqlTrade := mysqlTrades[marketCommitName]
								if sqlTrade.Size <= 0 || sqlTrade.Price <= 0 {
									return fmt.Errorf("%s trade data stored in mysql is not complete", market.ID)
								}
							case "elastic_search":
								esTrade := esTrades[marketCommitName]
								if esTrade.Size <= 0 || esTrade.Price <= 0 {
									return fmt.Errorf("%s trade data stored in elastic search is not complete", market.ID)
								}
							case "influxdb":
								influxTrade := influxTrades[marketCommitName]
								if influxTrade.Size <= 0 || influxTrade.Price <= 0 {
									return fmt.Errorf("%s trade data stored in influxdb is not complete", market.ID)
								}
							case "nats":
								natsTrade := natsTrades[marketCommitName]
								if natsTrade.Size <= 0 || natsTrade.Price <= 0 {
									return fmt.Errorf("%s trade data stored in nats is not complete", market.ID)
								}
							case "clickhouse":
								clickHouseTrade := clickHouseTrades[marketCommitName]
								if clickHouseTrade.Size <= 0 || clickHouseTrade.Price <= 0 {
									return fmt.Errorf("%s trade data stored in clickhouse is not complete", market.ID)
								}
							}
						}
					}
				}
			}
		}
	}
	return nil
}

// Subscribe to NATS subject. Write received data to a file.
func natsSub(subject string, out io.Writer, nats *storage.NATS, t *testing.T) {
	if _, err := nats.Basic.Subscribe(subject, func(m *nc.Msg) {
		if strings.HasSuffix(m.Subject, "ticker") {
			ticker := storage.Ticker{}
			err := jsoniter.Unmarshal(m.Data, &ticker)
			if err != nil {
				t.Log("ERROR : " + err.Error())
			}
			fmt.Fprintf(out, "%-15s%-15s%-15s%20f\n\n", "Ticker", ticker.Exchange, ticker.MktCommitName, ticker.Price)
		} else {
			trade := storage.Trade{}
			err := jsoniter.Unmarshal(m.Data, &trade)
			if err != nil {
				t.Log("ERROR : " + err.Error())
			}
			fmt.Fprintf(out, "%-15s%-15s%-5s%20f%20f\n\n", "Trade", trade.Exchange, trade.MktCommitName, trade.Size, trade.Price)
		}
	}); err != nil {
		t.Log("ERROR : " + err.Error())
	}
}

type esResp struct {
	Aggregations struct {
		ByChannel struct {
			Buckets []struct {
				Key      string `json:"key"`
				ByMarket struct {
					Buckets []struct {
						Key  string `json:"key"`
						Size struct {
							Value float64 `json:"value"`
						} `json:"size"`
						Price struct {
							Value float64 `json:"value"`
						} `json:"price"`
					} `json:"buckets"`
				} `json:"by_market"`
			} `json:"buckets"`
		} `json:"by_channel"`
	} `json:"aggregations"`
}
