// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package traces

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/common"
)

func Start(cfg *Config) error {
	logger, err := common.CreateLogger(cfg.SkipSettingGRPCLogger)
	if err != nil {
		return err
	}

	var exp *otlptrace.Exporter
	if cfg.UseHTTP {
		var exporterOpts []otlptracehttp.Option

		logger.Info("starting HTTP exporter")
		exporterOpts, err = httpExporterOptions(cfg)
		if err != nil {
			return err
		}
		exp, err = otlptracehttp.New(context.Background(), exporterOpts...)
		if err != nil {
			return fmt.Errorf("failed to obtain OTLP HTTP exporter: %w", err)
		}
	} else {
		var exporterOpts []otlptracegrpc.Option

		logger.Info("starting gRPC exporter")
		exporterOpts, err = grpcExporterOptions(cfg)
		if err != nil {
			return err
		}
		exp, err = otlptracegrpc.New(context.Background(), exporterOpts...)
		if err != nil {
			return fmt.Errorf("failed to obtain OTLP gRPC exporter: %w", err)
		}
	}

	defer func() {
		logger.Info("stopping the exporter")
		if tempError := exp.Shutdown(context.Background()); tempError != nil {
			logger.Error("failed to stop the exporter", zap.Error(tempError))
		}
	}()

	var ssp sdktrace.SpanProcessor
	if cfg.Batch {
		queueSize := sdktrace.DefaultMaxQueueSize * cfg.WorkerCount
		batchSize := sdktrace.DefaultMaxExportBatchSize * cfg.WorkerCount
		if cfg.Rate > 0.5*sdktrace.DefaultMaxQueueSize {
			queueSize = 2 * int(cfg.Rate) * cfg.WorkerCount
			batchSize = int(cfg.Rate>>1) * cfg.WorkerCount
		}

		ssp = sdktrace.NewBatchSpanProcessor(exp, sdktrace.WithBatchTimeout(time.Second), sdktrace.WithMaxQueueSize(queueSize), sdktrace.WithMaxExportBatchSize(batchSize))
		defer func() {
			logger.Info("stop the batch span processor")
			if tempError := ssp.Shutdown(context.Background()); tempError != nil {
				logger.Error("failed to stop the batch span processor", zap.Error(tempError))
			}
		}()
	}

	var attributes []attribute.KeyValue
	// may be overridden by `--otlp-attributes service.name="foo"`
	attributes = append(attributes, semconv.ServiceNameKey.String(cfg.ServiceName))
	attributes = append(attributes, cfg.GetAttributes()...)

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, attributes...)),
	)

	if cfg.Batch {
		tracerProvider.RegisterSpanProcessor(ssp)
	}
	otel.SetTracerProvider(tracerProvider)

	if err = Run(cfg, logger); err != nil {
		logger.Error("failed to execute the test scenario.", zap.Error(err))
		return err
	}

	return nil
}

// Run executes the test scenario.
func Run(c *Config, logger *zap.Logger) error {
	if c.TotalDuration > 0 {
		c.NumTraces = 0
	} else if c.NumTraces <= 0 {
		return fmt.Errorf("either `traces` or `duration` must be greater than 0")
	}

	if c.Rate <= 0 {
		logger.Info("generation of traces isn't being throttled")
	} else {
		logger.Info("generation of traces is limited", zap.Float64("per-second", float64(c.Rate)))
	}

	var statusCode codes.Code

	switch strings.ToLower(c.StatusCode) {
	case "0", "unset", "":
		statusCode = codes.Unset
	case "1", "error":
		statusCode = codes.Error
	case "2", "ok":
		statusCode = codes.Ok
	default:
		return fmt.Errorf("expected `status-code` to be one of (Unset, Error, Ok) or (0, 1, 2), got %q instead", c.StatusCode)
	}

	wg := sync.WaitGroup{}

	running := &atomic.Bool{}
	running.Store(true)

	telemetryAttributes := c.GetTelemetryAttributes()

	stopChan := make(chan struct{})
	counter := &Counter{
		stop:   stopChan,
		logger: logger,
	}

	go counter.countSpeed()

	for i := 0; i < c.WorkerCount; i++ {
		wg.Add(1)
		w := worker{
			numTraces:        c.NumTraces,
			numChildSpans:    int(math.Max(1, float64(c.NumChildSpans))),
			propagateContext: c.PropagateContext,
			statusCode:       statusCode,
			limitPerSecond:   c.Rate,
			totalDuration:    c.TotalDuration,
			running:          running,
			wg:               &wg,
			logger:           logger.With(zap.Int("worker", i)),
			loadSize:         c.LoadSize,
			spanDuration:     c.SpanDuration,
			counter:          counter,
		}

		go w.simulateTraces(telemetryAttributes)
	}
	if c.TotalDuration > 0 {
		time.Sleep(c.TotalDuration)
		running.Store(false)
	}
	wg.Wait()

	close(stopChan)
	return nil
}

type Counter struct {
	spanNum int64
	stop    chan struct{}
	logger  *zap.Logger
}

func (c *Counter) countSpeed() {
	const speedInterval = 5
	run := true
	ticker := time.NewTicker(time.Second * speedInterval)
	for run {
		select {
		case <-c.stop:
			run = false
		case <-ticker.C:
			spanSpeed := atomic.LoadInt64(&c.spanNum)
			atomic.AddInt64(&c.spanNum, -spanSpeed)
			c.logger.Info("signal speed",
				zap.Int64("span", spanSpeed/speedInterval))
		}
	}
	ticker.Stop()
}

func (c *Counter) Add() {
	atomic.AddInt64(&c.spanNum, 1)
}
