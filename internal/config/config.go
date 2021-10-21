package config

// Config contains config values for the app.
// Struct values are loaded from user defined JSON config file.
type Config struct {
	Markets    map[string][]Market `json:"markets"`
	Exchanges  map[string]Exchange `json:"exchanges"`
	Connection Connection          `json:"connection"`
	Log        Log                 `json:"log"`
}

// Exchange contains config values for different exchanges.
type Exchange struct {
	RestUrl            string `json:"rest_url"`
	WebsocketUrl       string `json:"websocket_url"`
	WebsocketThreshold int    `json:"websocket_threshold"`
	WebsocketTimeout   int    `json:"websocket_timeout"`
	Retry              Retry  `json:"retry"`
}

// Market contains config values for different markets.
type Market struct {
	ID         string `json:"id"`
	Info       []Info `json:"info"`
	CommitName string `json:"commit_name"`
}

// Info contains config values for different market channels.
type Info struct {
	Channel          string   `json:"channel"`
	Connector        string   `json:"connector"`
	WsConsiderIntSec int      `json:"websocket_consider_interval_sec"`
	RESTPingIntSec   int      `json:"rest_ping_interval_sec"`
	Storages         []string `json:"storages"`
}

// Retry contains config values for retry process.
type Retry struct {
	Number   int `json:"number"`
	GapSec   int `json:"gap_sec"`
	ResetSec int `json:"reset_sec"`
}

// Connection contains config values for different API and storage connections.
type Connection struct {
	WS         WS         `json:"websocket"`
	REST       REST       `json:"rest"`
	Terminal   Terminal   `json:"terminal"`
	MySQL      MySQL      `json:"mysql"`
	ES         ES         `json:"elastic_search"`
	InfluxDB   InfluxDB   `json:"influxdb"`
	NATS       NATS       `json:"nats"`
	ClickHouse ClickHouse `json:"clickhouse"`
	S3         S3         `json:"s3"`
}

// WS contains config values for websocket connection.
type WS struct {
	ConnTimeoutSec int `json:"conn_timeout_sec"`
	ReadTimeoutSec int `json:"read_timeout_sec"`
}

// REST contains config values for REST API connection.
type REST struct {
	ReqTimeoutSec       int `json:"request_timeout_sec"`
	MaxIdleConns        int `json:"max_idle_conns"`
	MaxIdleConnsPerHost int `json:"max_idle_conns_per_host"`
}

type Buffs struct {
	TickerCommitBuf int `json:"ticker_commit_buffer"`
	TradeCommitBuf  int `json:"trade_commit_buffer"`
	CandleCommitBuf int `json:"candle_commit_buffer"`
}

// Terminal contains config values for terminal display.
type Terminal struct {
	Buffs
}

// MySQL contains config values for mysql.
type MySQL struct {
	User               string `josn:"user"`
	Password           string `json:"password"`
	URL                string `json:"URL"`
	Schema             string `json:"schema"`
	ReqTimeoutSec      int    `json:"request_timeout_sec"`
	ConnMaxLifetimeSec int    `json:"conn_max_lifetime_sec"`
	MaxOpenConns       int    `json:"max_open_conns"`
	MaxIdleConns       int    `json:"max_idle_conns"`
	Buffs
}

// ES contains config values for elastic search.
type ES struct {
	Addresses           []string `json:"addresses"`
	Username            string   `json:"username"`
	Password            string   `json:"password"`
	IndexName           string   `json:"index_name"`
	ReqTimeoutSec       int      `json:"request_timeout_sec"`
	MaxIdleConns        int      `json:"max_idle_conns"`
	MaxIdleConnsPerHost int      `json:"max_idle_conns_per_host"`
	Buffs
}

// InfluxDB contains config values for influxdb.
type InfluxDB struct {
	Organization  string `josn:"organization"`
	Bucket        string `json:"bucket"`
	Token         string `json:"token"`
	URL           string `json:"URL"`
	ReqTimeoutSec int    `json:"request_timeout_sec"`
	MaxIdleConns  int    `json:"max_idle_conns"`
	Buffs
}

// NATS contains config values for nats.
type NATS struct {
	Addresses       []string `json:"addresses"`
	Username        string   `json:"username"`
	Password        string   `json:"password"`
	SubjectBaseName string   `json:"subject_base_name"`
	ReqTimeoutSec   int      `json:"request_timeout_sec"`
	Buffs
}

// ClickHouse contains config values for clickhouse.
type ClickHouse struct {
	User          string   `josn:"user"`
	Password      string   `json:"password"`
	URL           string   `json:"URL"`
	Schema        string   `json:"schema"`
	ReqTimeoutSec int      `json:"request_timeout_sec"`
	AltHosts      []string `json:"alt_hosts"`
	Compression   bool     `json:"compression"`
	Buffs
}

// S3 contains config values for s3.
type S3 struct {
	AWSRegion           string `json:"aws_region"`
	AccessKeyID         string `json:"access_key_id"`
	SecretAccessKey     string `json:"secret_access_key"`
	Bucket              string `json:"bucket"`
	UsePrefixForObjName bool   `json:"use_prefix_for_object_name"`
	ReqTimeoutSec       int    `json:"request_timeout_sec"`
	MaxIdleConns        int    `json:"max_idle_conns"`
	MaxIdleConnsPerHost int    `json:"max_idle_conns_per_host"`
	Buffs
}

// Log contains config values for logging.
type Log struct {
	Level    string `json:"level"`
	FilePath string `json:"file_path"`
}
