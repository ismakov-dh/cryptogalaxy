package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
)

// S3 is for connecting and inserting data to S3.
type S3 struct {
	Client *awss3.Client
	Cfg    *config.S3
}

var _s3 *S3

// InitS3 initializes S3 connection with configured values.
func InitS3(cfg *config.S3) (*S3, error) {
	if _s3 == nil {
		httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
			tr.MaxIdleConns = cfg.MaxIdleConns
			tr.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
		}).WithTimeout(time.Duration(cfg.ReqTimeoutSec) * time.Second)
		awsConfig, err := awscfg.LoadDefaultConfig(context.TODO(),
			awscfg.WithRegion(cfg.AWSRegion),
			awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
			awscfg.WithHTTPClient(httpClient))
		if err != nil {
			return nil, err
		}
		awsClient := awss3.NewFromConfig(awsConfig)

		_s3 = &S3{
			Client: awsClient,
			Cfg:    cfg,
		}
		stores[SS3] = _s3
	}
	return _s3, nil
}

// CommitTickers batch inserts input ticker data to S3.
func (s *S3) CommitTickers(appCtx context.Context, data []*Ticker) error {
	var fileName strings.Builder
	if s.Cfg.UsePrefixForObjName {
		nBig, err := rand.Int(rand.Reader, big.NewInt(10))
		if err != nil {
			return err
		}
		fileName.WriteString(strconv.Itoa(int(nBig.Int64())))
		fileName.WriteString("/")
	}
	fileName.WriteString("ticker/")
	fileName.WriteString(data[0].Exchange)
	fileName.WriteString(strconv.Itoa(int(time.Now().UTC().UnixNano())))
	fileName.WriteString(".json")
	s3ObjName := fileName.String()
	s3Data, err := jsoniter.Marshal(data)
	if err != nil {
		return err
	}
	input := &awss3.PutObjectInput{
		Bucket: &s.Cfg.Bucket,
		Key:    &s3ObjName,
		Body:   bytes.NewReader(s3Data),
	}
	_, err = s.Client.PutObject(appCtx, input)
	if err != nil {
		return err
	}
	return nil
}

// CommitTrades batch inserts input trade data to S3.
func (s *S3) CommitTrades(appCtx context.Context, data []*Trade) error {
	var fileName strings.Builder
	if s.Cfg.UsePrefixForObjName {
		nBig, err := rand.Int(rand.Reader, big.NewInt(10))
		if err != nil {
			return err
		}
		fileName.WriteString(strconv.Itoa(int(nBig.Int64())))
		fileName.WriteString("/")
	}
	fileName.WriteString("trade/")
	fileName.WriteString(data[0].Exchange)
	fileName.WriteString(strconv.Itoa(int(time.Now().UTC().UnixNano())))
	fileName.WriteString(".json")
	s3ObjName := fileName.String()
	s3Data, err := jsoniter.Marshal(data)
	if err != nil {
		return err
	}
	input := &awss3.PutObjectInput{
		Bucket: &s.Cfg.Bucket,
		Key:    &s3ObjName,
		Body:   bytes.NewReader(s3Data),
	}
	_, err = s.Client.PutObject(appCtx, input)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3) CommitCandles(_ context.Context, _ []*Candle) error { return nil }
