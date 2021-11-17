package main

import (
	"context"
	"flag"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/initializer"
)

func main() {
	// Load config file values.
	// Default path for file is ./config.json.
	cfgPath := flag.String("config", "./config.json", "configuration JSON file path")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Println("Error loading config from:", *cfgPath)
		fmt.Println(err)
		fmt.Println("exiting the app")
	}

	// Start the app.
	err = initializer.Start(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
		fmt.Println("exiting the app")
	}
}
