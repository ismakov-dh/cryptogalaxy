package main

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/rs/zerolog/log"
)

// This function will query all the exchanges for market info and store it in a csv file.
// Users can look up to this csv file to give market ID in the app configuration.
// CSV file created at ./examples/markets.csv.
func main() {
	cfg, err := config.Load("./examples/config.json")
	if err != nil {
		log.Error().Err(err).Msg("error loading config")
		return
	}

	f, err := os.Create("./examples/markets.csv")
	if err != nil {
		log.Error().Err(err).Msg("csv file create")
		return
	}
	w := csv.NewWriter(f)
	defer f.Close()

	// FTX exchange.
	exchName := "ftx"
	resp, err := http.Get(cfg.Exchanges[exchName].RestURL + "markets")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	ftxMarkets := ftxResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&ftxMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range ftxMarkets.Result {
		if record.Type != "spot" {
			continue
		}
		if err = w.Write([]string{exchName, record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Coinbase-Pro exchange.
	exchName = "coinbase-pro"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "products")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	coinbaseProMarkets := []coinbaseProResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&coinbaseProMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range coinbaseProMarkets {
		if record.Status == "online" {
			if err = w.Write([]string{exchName, record.Name}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Binance exchange.
	exchName = "binance"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "exchangeInfo")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	binanceMarkets := binanceResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&binanceMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range binanceMarkets.Result {
		if record.Status == "TRADING" {
			if err = w.Write([]string{exchName, record.Name}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Bitfinex exchange.
	exchName = "bitfinex"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "conf/pub:list:pair:exchange")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	bitfinexMarkets := bitfinexResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bitfinexMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range bitfinexMarkets[0] {
		if err = w.Write([]string{exchName, record}); err != nil {
			log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// BHEX exchange.
	exchName = "hbtc"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "openapi/v1/pairs")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	hbtcMarkets := []hbtcRespRes{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&hbtcMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range hbtcMarkets {
		if err = w.Write([]string{"bhex", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Huobi exchange.
	exchName = "huobi"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "v1/common/symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	huobiMarkets := huobiResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&huobiMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range huobiMarkets.Data {
		if record.Status == "online" {
			if err = w.Write([]string{exchName, record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Gateio exchange.
	exchName = "gateio"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "spot/currency_pairs")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	gateioMarkets := []gateioResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&gateioMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range gateioMarkets {
		if record.Status == "tradable" {
			if err = w.Write([]string{exchName, record.Name}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Kucoin exchange.
	exchName = "kucoin"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	kucoinMarkets := kucoinResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&kucoinMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range kucoinMarkets.Data {
		if record.Status {
			if err = w.Write([]string{exchName, record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Bitstamp exchange.
	exchName = "bitstamp"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "trading-pairs-info")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	bitstampMarkets := []bitstampResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bitstampMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range bitstampMarkets {
		if record.Status == "Enabled" {
			if err = w.Write([]string{exchName, record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Bybit exchange.
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "v2/public/symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", "bybit").Msg("exchange request for markets")
		return
	}
	bybitMarkets := bybitResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bybitMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "bybit").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range bybitMarkets.Result {
		if record.QuoteCurrency != "USDT" || record.Status != "Trading" {
			continue
		}
		if err = w.Write([]string{"bybit", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "bybit").Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Probit exchange.
	exchName = "probit"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "market")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	probitMarkets := probitResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&probitMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range probitMarkets.Data {
		if !record.Status {
			if err = w.Write([]string{exchName, record.ID}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Gemini exchange.
	exchName = "gemini"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	geminiMarkets := []string{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&geminiMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range geminiMarkets {
		if err = w.Write([]string{exchName, record}); err != nil {
			log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Bitmart exchange.
	exchName = "bitmart"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	bitmartMarkets := bitmartResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bitmartMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range bitmartMarkets.Data.Symbols {
		if err = w.Write([]string{exchName, record}); err != nil {
			log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Digifinex exchange.
	exchName = "digifinex"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "spot/symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	digifinexMarkets := digifinexResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&digifinexMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range digifinexMarkets.SymbolList {
		if record.Status == "TRADING" {
			if err = w.Write([]string{exchName, record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Ascendex exchange.
	exchName = "ascendex"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "products")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	ascendexMarkets := ascendexResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&ascendexMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range ascendexMarkets.Data {
		if record.Status == "Normal" {
			if err = w.Write([]string{exchName, record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Kraken exchange.
	exchName = "kraken"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "AssetPairs")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	krakenMarkets := krakenResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&krakenMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range krakenMarkets.Result {
		if market, ok := record["wsname"].(string); ok {
			if err = w.Write([]string{exchName, market}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		} else {
			log.Error().Str("exchange", exchName).Interface("market", record["wsname"]).Msg("cannot convert market name to string")
			return
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Binance US exchange.
	exchName = "binance-us"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "exchangeInfo")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	binanceUSMarkets := binanceUSResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&binanceUSMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range binanceUSMarkets.Result {
		if record.Status == "TRADING" {
			if err = w.Write([]string{exchName, record.Name}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// OKEx exchange.
	exchName = "okex"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "public/instruments?instType=SPOT")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	okexMarkets := okexResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&okexMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range okexMarkets.Data {
		if record.Status == "live" {
			if err = w.Write([]string{exchName, record.InstID}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// FTX US exchange.
	exchName = "ftx-us"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "markets")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	ftxUSMarkets := ftxUSResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&ftxUSMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range ftxUSMarkets.Result {
		if record.Type != "spot" {
			continue
		}
		if err = w.Write([]string{exchName, record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// HitBTC exchange.
	exchName = "hitbtc"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "symbol")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	hitBTCMarkets := make(map[string]hitBTCResp)
	if err = jsoniter.NewDecoder(resp.Body).Decode(&hitBTCMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for market, record := range hitBTCMarkets {
		if record.Type == "spot" {
			if err = w.Write([]string{exchName, market}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// AAX exchange.
	exchName = "aax"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "instruments")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	aaxMarkets := aaxResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&aaxMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range aaxMarkets.Data {
		if record.Type != "spot" {
			continue
		}
		if err = w.Write([]string{exchName, record.Symbol}); err != nil {
			log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Bitrue exchange.
	exchName = "bitrue"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "exchangeInfo")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	bitrueMarkets := bitrueResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bitrueMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range bitrueMarkets.Symbols {
		if record.Status == "TRADING" {
			if err = w.Write([]string{exchName, record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// BTSE exchange.
	exchName = "btse"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "market_summary")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	btseMarkets := []btseResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&btseMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range btseMarkets {
		if record.Active && !record.Futures {
			if err = w.Write([]string{exchName, record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Mexo exchange.
	exchName = "mexo"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "v1/brokerInfo")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	mexoMarkets := mexoResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&mexoMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range mexoMarkets.Symbols {
		if record.Status == "TRADING" {
			if err = w.Write([]string{exchName, record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	// Bequant exchange.
	exchName = "bequant"
	resp, err = http.Get(cfg.Exchanges[exchName].RestURL + "symbol")
	if err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("exchange request for markets")
		return
	}
	bequantMarkets := make(map[string]bequantResp)
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bequantMarkets); err != nil {
		log.Error().Err(err).Str("exchange", exchName).Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for market, record := range bequantMarkets {
		if record.Type == "spot" {
			if err = w.Write([]string{exchName, market}); err != nil {
				log.Error().Err(err).Str("exchange", exchName).Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Printf("got market info from %s\n", exchName)

	fmt.Println("CSV file generated successfully at ./examples/markets.csv")
}

type ftxResp struct {
	Result []ftxRespRes `json:"result"`
}
type ftxRespRes struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type coinbaseProResp struct {
	Name   string `json:"id"`
	Status string `json:"status"`
}

type binanceResp struct {
	Result []binanceRespRes `json:"symbols"`
}
type binanceRespRes struct {
	Name   string `json:"symbol"`
	Status string `json:"status"`
}

type bitfinexResp [][]string

type hbtcRespRes struct {
	Name string `json:"symbol"`
}

type huobiResp struct {
	Data []huobiRespData `json:"data"`
}
type huobiRespData struct {
	Symbol string `json:"symbol"`
	Status string `json:"state"`
}

type gateioResp struct {
	Name   string `json:"id"`
	Status string `json:"trade_status"`
}

type kucoinResp struct {
	Data []kucoinRespData `json:"data"`
}
type kucoinRespData struct {
	Symbol string `json:"symbol"`
	Status bool   `json:"enableTrading"`
}

type bitstampResp struct {
	Symbol string `json:"url_symbol"`
	Status string `json:"trading"`
}

type bybitResp struct {
	Result []bybitRespRes `json:"result"`
}
type bybitRespRes struct {
	Name          string `json:"name"`
	QuoteCurrency string `json:"quote_currency"`
	Status        string `json:"status"`
}

type probitResp struct {
	Data []probitRespData `json:"data"`
}
type probitRespData struct {
	ID     string `json:"id"`
	Status bool   `json:"closed"`
}

type bitmartResp struct {
	Data bitmartRespData `json:"data"`
}
type bitmartRespData struct {
	Symbols []string `json:"symbols"`
}

type digifinexResp struct {
	SymbolList []digifinexRespRes `json:"symbol_list"`
}
type digifinexRespRes struct {
	Symbol string `json:"symbol"`
	Status string `json:"status"`
}

type ascendexResp struct {
	Data []ascendexRespData `json:"data"`
}
type ascendexRespData struct {
	Symbol string `json:"symbol"`
	Status string `json:"status"`
}

type krakenResp struct {
	Result map[string]map[string]interface{} `json:"result"`
}

type binanceUSResp struct {
	Result []binanceUSRespRes `json:"symbols"`
}
type binanceUSRespRes struct {
	Name   string `json:"symbol"`
	Status string `json:"status"`
}

type okexResp struct {
	Data []okexRespData `json:"data"`
}
type okexRespData struct {
	InstID string `json:"instId"`
	Status string `json:"state"`
}

type ftxUSResp struct {
	Result []ftxUSRespRes `json:"result"`
}
type ftxUSRespRes struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type hitBTCResp struct {
	Type string `json:"type"`
}

type aaxResp struct {
	Data []aaxRespData `json:"data"`
}
type aaxRespData struct {
	Symbol string `json:"symbol"`
	Type   string `json:"type"`
	Status string `json:"status"`
}

type bitrueResp struct {
	Symbols []bitrueRespSymb `json:"symbols"`
}
type bitrueRespSymb struct {
	Symbol string `json:"symbol"`
	Status string `json:"status"`
}

type btseResp struct {
	Symbol  string `json:"symbol"`
	Active  bool   `json:"active"`
	Futures bool   `json:"futures"`
}

type mexoResp struct {
	Symbols []mexoRespSymb `json:"symbols"`
}
type mexoRespSymb struct {
	Symbol string `json:"symbol"`
	Status string `json:"status"`
}

type bequantResp struct {
	Type string `json:"type"`
}
