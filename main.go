package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/coreos/go-systemd/v22/sdjournal"
	"go.uber.org/zap"

	"github.com/pengubco/journald-to-cwl/batch"
	"github.com/pengubco/journald-to-cwl/config"
	"github.com/pengubco/journald-to-cwl/cwl"
	"github.com/pengubco/journald-to-cwl/journal"
)

var (
	region     string
	instanceID string
	cwlClient  *cloudwatchlogs.Client
)

func main() {
	// The reason main() calls execute() is that Go does not support both:
	// 1) Running deferred functions and 2) Setting a non-zero exit code at the same time.
	os.Exit(execute())
}

func execute() int {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = logger.Sync()
	}()
	zap.ReplaceGlobals(logger)

	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := initializeAWS(); err != nil {
		zap.S().Error(err)
		return 1
	}

	c, err := config.InitalizeConfig(instanceID, flag.Args())
	if err != nil {
		zap.S().Error(err)
		return 1
	}
	zap.S().Infof("Use config, %+v", c)

	cursor, err := NewFilebasedCursor(c.StateFile)
	if err != nil {
		zap.S().Errorf("cannot open or create journal cursor file %s, %w.", c.StateFile, err)
		return 1
	}
	defer cursor.Close()

	journalReader, err := initializeJournalReader(cursor)
	if err != nil {
		zap.S().Error(err)
		return 1
	}
	defer journalReader.Close()

	// The three background goroutines. Read -> Batch -> Write.
	// If any of them returns, all of them should return. We achieve this by sharing the same ctx and cancel.

	// Read journald entries.
	reader := journal.NewReader(journalReader, journal.WithWaitForDataTimeout(time.Second))
	go func() {
		defer cancel()
		reader.Read(ctx)
	}()

	// Batch journald entries to Cloudwatch log events.
	batcherOpts := []batch.Option{
		batch.WithSkipAuditLog(c.SkipAuditLog),
	}
	// Add message filter if patterns are configured.
	if len(c.IgnorePatterns) > 0 {
		batcherOpts = append(batcherOpts, batch.WithMessageFilter(c.ShouldFilterMessage))
	}
	batcher := batch.NewBatcher(reader.Entries(), batch.NewEntryToEventConverter(instanceID, time.Now), batcherOpts...)
	go func() {
		defer cancel()
		batcher.Batch(ctx)
	}()

	// Write batches to Cloudwatch log.
	writer := cwl.NewWriter(batcher.Batches(), cwlClient, c.LogGroup, c.LogStream, func(v string) error {
		return cursor.Set(v)
	})
	go func() {
		defer cancel()
		writer.Write(ctx)
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	select {
	case s := <-ch:
		zap.S().Infof("exit signal %v", s)
	case <-ctx.Done():
		return 1
	}

	return 0
}

func initializeAWS() error {
	timeout := 20 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("cannot load default aws config, %w", err)
	}
	client := imds.NewFromConfig(cfg)
	document, err := client.GetInstanceIdentityDocument(ctx, nil)
	if err != nil {
		return err
	}
	region = document.Region
	instanceID = document.InstanceIdentityDocument.InstanceID
	// Use the default: retry attempts of 3 and max backoff of 20 seconds.
	// https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/retries-timeouts/
	cwlClient = cloudwatchlogs.NewFromConfig(cfg, func(o *cloudwatchlogs.Options) {
		o.Region = region
	})

	return nil
}

func initializeJournalReader(cursor Cursor) (*sdjournal.Journal, error) {
	j, err := sdjournal.NewJournal()
	if err != nil {
		return nil, fmt.Errorf("cannot create journal reader, %v", err)
	}

	v, err := cursor.Get()
	if err != nil {
		zap.S().Errorf("cannot read journal cursor, %v. start with the oldest entry.", err)
		_ = j.SeekHead()
		return j, nil
	}
	if err := j.SeekCursor(v); err != nil {
		zap.S().Errorf("cannot seek to cursor, seek to head instead. %v", err)
		_ = j.SeekHead()
	}
	_, _ = j.Next()
	return j, nil
}
