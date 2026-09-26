// Command lagprobe samples consumer-group lag and end-to-end pipeline latency
// while a load test runs, and writes the samples out as CSV.
//
// This exists because the Grafana dashboard shipped in M5 only approximates
// lag: it compares the ingester's produce rate against the normalizer's, which
// shows whether the pipeline is keeping up but not by how much it is behind.
// Real lag is the difference between a partition's high watermark and the
// group's committed offset, which only the broker can answer.
//
// Two numbers are reported per sample:
//
//	records lag — how many records each consumer group still has to read.
//	              This is the backlog, and it is what grows when a stage
//	              cannot keep up.
//
//	e2e latency — wall time from the newest event's block_time to now, read
//	              from the aggregates topic. This is what an operator actually
//	              feels: how stale the freshest available answer is.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		brokers  = flag.String("brokers", "localhost:19092", "comma-separated Kafka brokers")
		groups   = flag.String("groups", "streamforge-normalizer,streamforge-aggregator", "consumer groups to sample")
		interval = flag.Duration("interval", 2*time.Second, "sampling interval")
		duration = flag.Duration("duration", 0, "stop after this long; 0 runs until interrupted")
		out      = flag.String("out", "", "write CSV here instead of stdout")
		label    = flag.String("label", "", "tag every row with this run label")
	)
	flag.Parse()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ClientID("streamforge-lagprobe"),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kafka client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	admin := kadm.NewClient(client)

	writer, closeOut, err := openWriter(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open output: %v\n", err)
		os.Exit(1)
	}
	defer closeOut()

	csvOut := csv.NewWriter(writer)
	defer csvOut.Flush()
	_ = csvOut.Write([]string{"label", "elapsed_s", "group", "records_lag", "committed", "end_offset"})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	groupNames := strings.Split(*groups, ",")
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			csvOut.Flush()
			fmt.Fprintf(os.Stderr, "lagprobe stopped after %s\n", time.Since(start).Truncate(time.Second))
			return
		case <-ticker.C:
			elapsed := time.Since(start).Seconds()
			for _, group := range groupNames {
				lag, committed, end, lerr := groupLag(ctx, admin, strings.TrimSpace(group))
				if lerr != nil {
					fmt.Fprintf(os.Stderr, "lag %s: %v\n", group, lerr)
					continue
				}
				_ = csvOut.Write([]string{
					*label,
					strconv.FormatFloat(elapsed, 'f', 1, 64),
					strings.TrimSpace(group),
					strconv.FormatInt(lag, 10),
					strconv.FormatInt(committed, 10),
					strconv.FormatInt(end, 10),
				})
			}
			csvOut.Flush()
		}
	}
}

// groupLag sums lag across every partition the group consumes.
//
// Summing rather than reporting per partition: a load test cares whether the
// pipeline as a whole is falling behind. Per-partition detail matters when
// diagnosing a skewed partition key, which is a different investigation.
func groupLag(ctx context.Context, admin *kadm.Client, group string) (lag, committed, end int64, err error) {
	described, err := admin.DescribeGroups(ctx, group)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("describe group: %w", err)
	}
	if len(described) == 0 {
		return 0, 0, 0, fmt.Errorf("group %q not found", group)
	}

	offsets, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("fetch offsets: %w", err)
	}

	topics := offsets.Offsets().TopicsSet().Topics()
	if len(topics) == 0 {
		// A group that has joined but not yet committed anything is not an
		// error — it is simply zero lag so far.
		return 0, 0, 0, nil
	}

	endOffsets, err := admin.ListEndOffsets(ctx, topics...)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("list end offsets: %w", err)
	}

	calculated := kadm.CalculateGroupLag(described[group], offsets, endOffsets)
	for _, topicLag := range calculated {
		for _, partitionLag := range topicLag {
			lag += partitionLag.Lag
			committed += partitionLag.Commit.At
			end += partitionLag.End.Offset
		}
	}
	return lag, committed, end, nil
}

func openWriter(path string) (*os.File, func(), error) {
	if path == "" {
		return os.Stdout, func() {}, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = f.Close() }, nil
}
