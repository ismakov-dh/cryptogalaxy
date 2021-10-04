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
	f, err := os.Create("./examples/markets.csv")
	if err != nil {
		log.Error().Err(err).Str("exchange", "ftx").Msg("csv file create")
		return
	}
	w := csv.NewWriter(f)
	defer f.Close()

	// FTX exchange.
	resp, err := http.Get(config.FtxRESTBaseURL + "markets")
	if err != nil {
		log.Error().Err(err).Str("exchange", "ftx").Msg("exchange request for markets")
		return
	}
	ftxMarkets := ftxResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&ftxMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "ftx").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range ftxMarkets.Result {
		if record.Type != "spot" {
			continue
		}
		if err = w.Write([]string{"ftx", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "ftx").Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Println("got market info from FTX")

	// Coinbase-Pro exchange.
	resp, err = http.Get(config.CoinbaseProRESTBaseURL + "products")
	if err != nil {
		log.Error().Err(err).Str("exchange", "coinbase-pro").Msg("exchange request for markets")
		return
	}
	coinbaseProMarkets := []coinbaseProResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&coinbaseProMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "coinbase-pro").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range coinbaseProMarkets {
		if record.Status == "online" {
			if err = w.Write([]string{"coinbase-pro", record.Name}); err != nil {
				log.Error().Err(err).Str("exchange", "coinbase-pro").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from Coinbase Pro")

	// Binance exchange.
	resp, err = http.Get(config.BinanceRESTBaseURL + "exchangeInfo")
	if err != nil {
		log.Error().Err(err).Str("exchange", "binance").Msg("exchange request for markets")
		return
	}
	binanceMarkets := binanceResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&binanceMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "binance").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range binanceMarkets.Result {
		if record.Status == "TRADING" {
			if err = w.Write([]string{"binance", record.Name}); err != nil {
				log.Error().Err(err).Str("exchange", "binance").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from Binance")

	// Bitfinex exchange.
	resp, err = http.Get(config.BitfinexRESTBaseURL + "conf/pub:list:pair:exchange")
	if err != nil {
		log.Error().Err(err).Str("exchange", "bitfinex").Msg("exchange request for markets")
		return
	}
	bitfinexMarkets := bitfinexResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bitfinexMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "bitfinex").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range bitfinexMarkets[0] {
		if err = w.Write([]string{"bitfinex", record}); err != nil {
			log.Error().Err(err).Str("exchange", "bitfinex").Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Println("got market info from Bitfinex")

	// BHEX exchange.
	resp, err = http.Get(config.BHEXRESTBaseURL + "openapi/v1/pairs")
	if err != nil {
		log.Error().Err(err).Str("exchange", "hbtc").Msg("exchange request for markets")
		return
	}
	hbtcMarkets := []hbtcRespRes{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&hbtcMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "hbtc").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range hbtcMarkets {
		if err = w.Write([]string{"bhex", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "hbtc").Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Println("got market info from BHEX")

	// Huobi exchange.
	resp, err = http.Get(config.HuobiRESTBaseURL + "v1/common/symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", "huobi").Msg("exchange request for markets")
		return
	}
	huobiMarkets := huobiResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&huobiMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "huobi").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range huobiMarkets.Data {
		if record.Status == "online" {
			if err = w.Write([]string{"huobi", record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", "huobi").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from Huobi")

	// Gateio exchange.
	resp, err = http.Get(config.GateioRESTBaseURL + "spot/currency_pairs")
	if err != nil {
		log.Error().Err(err).Str("exchange", "gateio").Msg("exchange request for markets")
		return
	}
	gateioMarkets := []gateioResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&gateioMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "gateio").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range gateioMarkets {
		if record.Status == "tradable" {
			if err = w.Write([]string{"gateio", record.Name}); err != nil {
				log.Error().Err(err).Str("exchange", "gateio").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from Gateio")

	// Kucoin exchange.
	resp, err = http.Get(config.KucoinRESTBaseURL + "symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", "kucoin").Msg("exchange request for markets")
		return
	}
	kucoinMarkets := kucoinResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&kucoinMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "kucoin").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range kucoinMarkets.Data {
		if record.Status {
			if err = w.Write([]string{"kucoin", record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", "kucoin").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from Kucoin")

	// Bitstamp exchange.
	resp, err = http.Get(config.BitstampRESTBaseURL + "trading-pairs-info")
	if err != nil {
		log.Error().Err(err).Str("exchange", "bitstamp").Msg("exchange request for markets")
		return
	}
	bitstampMarkets := []bitstampResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bitstampMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "bitstamp").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range bitstampMarkets {
		if record.Status == "Enabled" {
			if err = w.Write([]string{"bitstamp", record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", "bitstamp").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from Bitstamp")

	// Bybit exchange.
	resp, err = http.Get(config.BybitRESTBaseURL + "v2/public/symbols")
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
	fmt.Println("got market info from Bybit")

	// Probit exchange.
	resp, err = http.Get(config.ProbitRESTBaseURL + "market")
	if err != nil {
		log.Error().Err(err).Str("exchange", "probit").Msg("exchange request for markets")
		return
	}
	probitMarkets := probitResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&probitMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "probit").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range probitMarkets.Data {
		if !record.Status {
			if err = w.Write([]string{"probit", record.ID}); err != nil {
				log.Error().Err(err).Str("exchange", "probit").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from Probit")

	// Gemini exchange.
	resp, err = http.Get(config.GeminiRESTBaseURL + "symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", "gemini").Msg("exchange request for markets")
		return
	}
	geminiMarkets := []string{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&geminiMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "gemini").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range geminiMarkets {
		if err = w.Write([]string{"gemini", record}); err != nil {
			log.Error().Err(err).Str("exchange", "gemini").Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Println("got market info from Gemini")

	// Bitmart exchange.
	resp, err = http.Get(config.BitmartRESTBaseURL + "symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", "bitmart").Msg("exchange request for markets")
		return
	}
	bitmartMarkets := bitmartResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bitmartMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "bitmart").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range bitmartMarkets.Data.Symbols {
		if err = w.Write([]string{"bitmart", record}); err != nil {
			log.Error().Err(err).Str("exchange", "bitmart").Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Println("got market info from Bitmart")

	// Digifinex exchange.
	resp, err = http.Get(config.DigifinexRESTBaseURL + "spot/symbols")
	if err != nil {
		log.Error().Err(err).Str("exchange", "digifinex").Msg("exchange request for markets")
		return
	}
	digifinexMarkets := digifinexResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&digifinexMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "digifinex").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range digifinexMarkets.SymbolList {
		if record.Status == "TRADING" {
			if err = w.Write([]string{"digifinex", record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", "digifinex").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from Digifinex")

	// Ascendex exchange.
	resp, err = http.Get(config.AscendexRESTBaseURL + "products")
	if err != nil {
		log.Error().Err(err).Str("exchange", "ascendex").Msg("exchange request for markets")
		return
	}
	ascendexMarkets := ascendexResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&ascendexMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "ascendex").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range ascendexMarkets.Data {
		if record.Status == "Normal" {
			if err = w.Write([]string{"ascendex", record.Symbol}); err != nil {
				log.Error().Err(err).Str("exchange", "ascendex").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from Ascendex")

	// Kraken exchange.
	resp, err = http.Get(config.KrakenRESTBaseURL + "AssetPairs")
	if err != nil {
		log.Error().Err(err).Str("exchange", "kraken").Msg("exchange request for markets")
		return
	}
	krakenMarkets := krakenResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&krakenMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "kraken").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range krakenMarkets.Result {
		if market, ok := record["wsname"].(string); ok {
			if err = w.Write([]string{"kraken", market}); err != nil {
				log.Error().Err(err).Str("exchange", "kraken").Msg("writing markets to csv")
				return
			}
		} else {
			log.Error().Str("exchange", "kraken").Interface("market", record["wsname"]).Msg("cannot convert market name to string")
			return
		}
	}
	w.Flush()
	fmt.Println("got market info from Kraken")

	// Binance US exchange.
	resp, err = http.Get(config.BinanceUSRESTBaseURL + "exchangeInfo")
	if err != nil {
		log.Error().Err(err).Str("exchange", "binance-us").Msg("exchange request for markets")
		return
	}
	binanceUSMarkets := binanceUSResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&binanceUSMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "binance-us").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range binanceUSMarkets.Result {
		if record.Status == "TRADING" {
			if err = w.Write([]string{"binance-us", record.Name}); err != nil {
				log.Error().Err(err).Str("exchange", "binance-us").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from Binance US")

	// OKEx exchange.
	resp, err = http.Get(config.OKExRESTBaseURL + "public/instruments?instType=SPOT")
	if err != nil {
		log.Error().Err(err).Str("exchange", "okex").Msg("exchange request for markets")
		return
	}
	okexMarkets := okexResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&okexMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "okex").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range okexMarkets.Data {
		if record.Status == "live" {
			if err = w.Write([]string{"okex", record.InstID}); err != nil {
				log.Error().Err(err).Str("exchange", "okex").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from OKEx")

	// FTX US exchange.
	resp, err = http.Get(config.FtxUSRESTBaseURL + "markets")
	if err != nil {
		log.Error().Err(err).Str("exchange", "ftx-us").Msg("exchange request for markets")
		return
	}
	ftxUSMarkets := ftxUSResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&ftxUSMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "ftx-us").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range ftxUSMarkets.Result {
		if record.Type != "spot" {
			continue
		}
		if err = w.Write([]string{"ftx-us", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "ftx-us").Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Println("got market info from FTX US")

	// HitBTC exchange.
	resp, err = http.Get(config.HitBTCRESTBaseURL + "symbol")
	if err != nil {
		log.Error().Err(err).Str("exchange", "hitbtc").Msg("exchange request for markets")
		return
	}
	hitBTCMarkets := make(map[string]hitBTCResp)
	if err = jsoniter.NewDecoder(resp.Body).Decode(&hitBTCMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "hitbtc").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for market, record := range hitBTCMarkets {
		if record.Type == "spot" {
			if err = w.Write([]string{"hitbtc", market}); err != nil {
				log.Error().Err(err).Str("exchange", "hitbtc").Msg("writing markets to csv")
				return
			}
		}
	}
	w.Flush()
	fmt.Println("got market info from HitBTC")

	// AAX exchange.
	resp, err = http.Get(config.AAXRESTBaseURL + "instruments")
	if err != nil {
		log.Error().Err(err).Str("exchange", "aax").Msg("exchange request for markets")
		return
	}
	aaxMarkets := aaxResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&aaxMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "aax").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range aaxMarkets.Data {
		if record.Type != "spot" {
			continue
		}
		if err = w.Write([]string{"aax", record.Symbol}); err != nil {
			log.Error().Err(err).Str("exchange", "aax").Msg("writing markets to csv")
			return
		}
	}
	w.Flush()
	fmt.Println("got market info from AAX")

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
