package exchange

import "time"

// cfgLookupKey is a key in the config lookup map.
type cfgLookupKey struct {
	market  string
	channel string
}

// cfgLookupVal is a value in the config lookup map.
type cfgLookupVal struct {
	connector        string
	wsConsiderIntSec int
	wsLastUpdated    time.Time
	terStr           bool
	mysqlStr         bool
	esStr            bool
	influxStr        bool
	natsStr          bool
	clickHouseStr    bool
	s3Str            bool
	id               int
	mktCommitName    string
}
