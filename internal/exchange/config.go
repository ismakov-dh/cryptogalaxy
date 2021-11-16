package exchange

import (
	"time"
)

type cfgLookupKey struct {
	market  string
	channel string
}

type cfgLookupVal struct {
	connector        string
	wsConsiderIntSec int
	wsLastUpdated    time.Time
	storages         []string
	id               int
	mktCommitName    string
}
