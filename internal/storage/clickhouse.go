package storage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"strings"
)

// clickhouse is for connecting and inserting data to clickhouse.
type clickhouse struct {
	DB  *sql.DB
	Cfg *config.ClickHouse
}

// clickhouse timestamp format.
const clickHouseTimestamp = "2006-01-02 15:04:05.999"

// InitClickHouse initializes clickhouse connection with configured values.
func InitClickHouse(cfg *config.ClickHouse) (Store, error) {
	if _, ok := stores[CLICKHOUSE]; !ok {
		var dataSourceName strings.Builder
		dataSourceName.WriteString(cfg.URL + "?")
		dataSourceName.WriteString("database=" + cfg.Schema)
		dataSourceName.WriteString("&read_timeout=" + fmt.Sprintf("%d", cfg.ReqTimeoutSec) + "&write_timeout=" + fmt.Sprintf("%d", cfg.ReqTimeoutSec))
		if strings.Trim(cfg.User, "") != "" && strings.Trim(cfg.Password, "") != "" {
			dataSourceName.WriteString("&username=" + cfg.User + "&password=" + cfg.Password)
		}
		if cfg.Compression {
			dataSourceName.WriteString("&compress=1")
		}
		prefix := false
		for i, v := range cfg.AltHosts {
			if strings.Trim(v, "") != "" {
				if !prefix {
					dataSourceName.WriteString("&alt_hosts=")
					prefix = true
				}
				if i == len(cfg.AltHosts)-1 {
					dataSourceName.WriteString(v)
				} else {
					dataSourceName.WriteString(v + ",")
				}
			}
		}

		db, err := sql.Open("clickhouse", dataSourceName.String())
		if err != nil {
			return nil, err
		}

		err = db.Ping()
		if err != nil {
			return nil, err
		}

		stores[CLICKHOUSE] = &clickhouse{
			DB:  db,
			Cfg: cfg,
		}
	}
	return stores[CLICKHOUSE], nil
}

// CommitTickers batch inserts input ticker data to clickHouse.
func (c *clickhouse) CommitTickers(_ context.Context, data []*Ticker) error {
	tx, err := c.DB.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO ticker (exchange, market, price, timestamp) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := range data {
		ticker := data[i]
		_, err := stmt.Exec(
			ticker.Exchange,
			ticker.MktCommitName,
			ticker.Price,
			ticker.Timestamp.Format(clickHouseTimestamp),
		)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// CommitTrades batch inserts input trade data to clickHouse.
func (c *clickhouse) CommitTrades(_ context.Context, data []*Trade) error {
	tx, err := c.DB.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`
INSERT INTO trade (exchange, market, trade_id, side, size, price, timestamp)
VALUES (?, ?, ?, ?, ?, ?, ?);
`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := range data {
		trade := data[i]
		_, err := stmt.Exec(
			trade.Exchange,
			trade.MktCommitName,
			trade.TradeID, trade.Side,
			trade.Size,
			trade.Price,
			trade.Timestamp.Format(clickHouseTimestamp),
		)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (c *clickhouse) CommitCandles(_ context.Context, data []*Candle) error {
	tx, err := c.DB.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`
INSERT INTO candle (exchange, market, open, high, low, close, volume, timestamp)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, candle := range data {
		_, err := stmt.Exec(
			candle.Exchange,
			candle.MktCommitName,
			candle.Open,
			candle.High,
			candle.Low,
			candle.Close,
			candle.Volume,
			candle.Timestamp.Format(clickHouseTimestamp),
		)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
