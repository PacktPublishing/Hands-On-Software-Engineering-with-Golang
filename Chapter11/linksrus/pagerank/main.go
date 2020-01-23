package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi"
	linkgraphproto "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi/proto"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi"
	textindexerproto "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi/proto"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/partition"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/service/pagerank"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
)

var (
	appName = "linksrus-pagerank"
	appSha  = "populated-at-link-time"
	logger  *logrus.Entry
)

func main() {
	host, _ := os.Hostname()
	rootLogger := logrus.New()
	rootLogger.SetFormatter(new(logrus.JSONFormatter))
	logger = rootLogger.WithFields(logrus.Fields{
		"app":  appName,
		"sha":  appSha,
		"host": host,
	})

	if err := makeApp().Run(os.Args); err != nil {
		logger.WithField("err", err).Error("shutting down due to error")
		_ = os.Stderr.Sync()
		os.Exit(1)
	}
}

func makeApp() *cli.App {
	app := cli.NewApp()
	app.Name = appName
	app.Version = appSha
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "link-graph-api",
			EnvVar: "LINK_GRAPH_API",
			Usage:  "The gRPC endpoint for connecting to the link graph",
		},
		cli.StringFlag{
			Name:   "text-indexer-api",
			EnvVar: "TEXT_INDEXER_API",
			Usage:  "The gRPC endpoint for connecting to the text indexer",
		},
		cli.IntFlag{
			Name:   "num-workers",
			Value:  runtime.NumCPU(),
			EnvVar: "NUM_WORKERS",
			Usage:  "The number of workers to use for calculating PageRank scores",
		},
		cli.DurationFlag{
			Name:   "update-interval",
			Value:  5 * time.Minute,
			EnvVar: "UPDATE_INTERVAL",
			Usage:  "The time between subsequent PageRank score updates",
		},
		cli.StringFlag{
			Name:   "partition-detection-mode",
			Value:  "single",
			EnvVar: "PARTITION_DETECTION_MODE",
			Usage:  "The partition detection mode to use. Supported values are 'dns=HEADLESS_SERVICE_NAME' (k8s) and 'single' (local dev mode)",
		},
		cli.IntFlag{
			Name:   "pprof-port",
			Value:  6060,
			EnvVar: "PPROF_PORT",
			Usage:  "The port for exposing pprof endpoints",
		},
	}
	app.Action = runMain
	return app
}

func runMain(appCtx *cli.Context) error {
	var wg sync.WaitGroup
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	// Start crawler
	partDet, err := getPartitionDetector(appCtx.String("partition-detection-mode"))
	if err != nil {
		return err
	}

	graphAPI, indexerAPI, err := getAPIs(ctx, appCtx.String("link-graph-api"), appCtx.String("text-indexer-api"))
	if err != nil {
		return err
	}

	var pageRankCfg pagerank.Config
	pageRankCfg.ComputeWorkers = appCtx.Int("num-workers")
	pageRankCfg.UpdateInterval = appCtx.Duration("update-interval")
	pageRankCfg.GraphAPI = graphAPI
	pageRankCfg.IndexAPI = indexerAPI
	pageRankCfg.PartitionDetector = partDet
	pageRankCfg.Logger = logger
	prSvc, err := pagerank.NewService(pageRankCfg)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := prSvc.Run(ctx); err != nil {
			logger.WithField("err", err).Error("pagerank service exited with error")
			cancelFn()
		}
	}()

	// Start pprof server
	pprofListener, err := net.Listen("tcp", fmt.Sprintf(":%d", appCtx.Int("pprof-port")))
	if err != nil {
		return err
	}
	defer func() { _ = pprofListener.Close() }()

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.WithField("port", appCtx.Int("pprof-port")).Info("listening for pprof requests")
		srv := new(http.Server)
		_ = srv.Serve(pprofListener)
	}()

	// Start signal watcher
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGHUP)
		select {
		case s := <-sigCh:
			logger.WithField("signal", s.String()).Infof("shutting down due to signal")
			_ = pprofListener.Close()
			cancelFn()
		case <-ctx.Done():
		}
	}()

	// Keep running until we receive a signal
	wg.Wait()
	return nil
}

func getAPIs(ctx context.Context, linkGraphAPI, textIndexerAPI string) (*linkgraphapi.LinkGraphClient, *textindexerapi.TextIndexerClient, error) {
	if linkGraphAPI == "" {
		return nil, nil, xerrors.Errorf("link graph API must be specified with --link-graph-api")
	}
	if textIndexerAPI == "" {
		return nil, nil, xerrors.Errorf("text indexer API must be specified with --text-indexer-api")
	}

	dialCtx, cancelFn := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFn()
	linkGraphConn, err := grpc.DialContext(dialCtx, linkGraphAPI, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, xerrors.Errorf("could not connect to link graph API: %w", err)
	}
	graphCli := linkgraphapi.NewLinkGraphClient(ctx, linkgraphproto.NewLinkGraphClient(linkGraphConn))

	dialCtx, cancelFn = context.WithTimeout(ctx, 5*time.Second)
	defer cancelFn()
	indexerConn, err := grpc.DialContext(dialCtx, textIndexerAPI, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, xerrors.Errorf("could not connect to text indexer API: %w", err)
	}
	indexerCli := textindexerapi.NewTextIndexerClient(ctx, textindexerproto.NewTextIndexerClient(indexerConn))

	return graphCli, indexerCli, nil
}

func getPartitionDetector(mode string) (partition.Detector, error) {
	switch {
	case mode == "single":
		return partition.Fixed{Partition: 0, NumPartitions: 1}, nil
	case strings.HasPrefix(mode, "dns="):
		tokens := strings.Split(mode, "=")
		return partition.DetectFromSRVRecords(tokens[1]), nil
	default:
		return nil, xerrors.Errorf("unsupported partition detection mode: %q", mode)
	}
}
