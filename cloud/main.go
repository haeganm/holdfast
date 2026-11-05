package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Configuration comes from the environment, which is how Lambda passes it:
//
//	TABLE                     DynamoDB table (required)
//	PEER_TOKEN                bearer token edges must send (required in Lambda, 32+ characters)
//	TRACK_TTL                 how long an aircraft lives without a new report (default 5m)
//	AWS_ENDPOINT_URL_DYNAMODB the SDK's own override, for DynamoDB Local
//
// Outside Lambda it serves plain HTTP on -listen, so the experiment harness
// can run it as a peer against DynamoDB Local.
func main() {
	listen := flag.String("listen", ":8080", "listen address outside Lambda")
	create := flag.Bool("create-table", false, "create the table if it is missing (DynamoDB Local)")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *listen, *create); err != nil {
		log.Error("cloud peer", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, listen string, create bool) error {
	inLambda := os.Getenv("AWS_LAMBDA_RUNTIME_API") != ""
	p, err := fromEnv(context.Background(), log, inLambda)
	if err != nil {
		return err
	}
	if inLambda {
		lambda.Start(adapt(p))
		return nil
	}
	if create {
		// DynamoDB Local can take a few seconds to come up next to us.
		deadline := time.Now().Add(30 * time.Second)
		for {
			err := ensureTable(context.Background(), p.db, p.table)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("create table %s: %w", p.table, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	if p.token == "" {
		log.Warn("no PEER_TOKEN: anyone who can reach this port can write to the table")
	}
	log.Info("listening", "addr", listen, "table", p.table)
	srv := &http.Server{Addr: listen, Handler: p, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

func fromEnv(ctx context.Context, log *slog.Logger, inLambda bool) (*peer, error) {
	p := &peer{table: os.Getenv("TABLE"), token: os.Getenv("PEER_TOKEN"), ttl: 5 * time.Minute, now: time.Now, log: log}
	if p.table == "" {
		return nil, errors.New("TABLE is not set")
	}
	if inLambda && len(p.token) < 32 {
		return nil, errors.New("PEER_TOKEN must be at least 32 characters")
	}
	if s := os.Getenv("TRACK_TTL"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("TRACK_TTL=%q: want a positive duration", s)
		}
		p.ttl = d
	}
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	p.db = dynamodb.NewFromConfig(cfg)
	return p, nil
}

func ensureTable(ctx context.Context, db *dynamodb.Client, table string) error {
	_, err := db.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            &table,
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("icao"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("icao"), KeyType: types.KeyTypeHash}},
	})
	var exists *types.ResourceInUseException
	if errors.As(err, &exists) {
		return nil
	}
	return err
}

// adapt turns an API Gateway HTTP API (payload 2.0) event into a request for
// h and h's response back into the event's reply.
func adapt(h http.Handler) func(context.Context, events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	return func(ctx context.Context, e events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
		body := []byte(e.Body)
		if e.IsBase64Encoded {
			var err error
			if body, err = base64.StdEncoding.DecodeString(e.Body); err != nil {
				return events.APIGatewayV2HTTPResponse{StatusCode: http.StatusBadRequest, Body: "body is not valid base64"}, nil
			}
		}
		target := e.RawPath
		if e.RawQueryString != "" {
			target += "?" + e.RawQueryString
		}
		r, err := http.NewRequestWithContext(ctx, e.RequestContext.HTTP.Method, target, bytes.NewReader(body))
		if err != nil {
			return events.APIGatewayV2HTTPResponse{StatusCode: http.StatusBadRequest, Body: "bad request"}, nil
		}
		for k, v := range e.Headers { // payload 2.0 joins repeated headers with commas
			r.Header.Set(k, v)
		}
		r.RemoteAddr = e.RequestContext.HTTP.SourceIP
		resp := &buffered{header: http.Header{}, status: http.StatusOK}
		h.ServeHTTP(resp, r)
		out := events.APIGatewayV2HTTPResponse{StatusCode: resp.status, Headers: map[string]string{}, Body: resp.body.String()}
		for k, v := range resp.header {
			out.Headers[k] = strings.Join(v, ",")
		}
		return out, nil
	}
}
