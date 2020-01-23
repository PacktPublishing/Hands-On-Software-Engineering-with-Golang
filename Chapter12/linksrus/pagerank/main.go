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
	"sync"
	"syscall"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi"
	linkgraphproto "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi/proto"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi"
	textindexerproto "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi/proto"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/linksrus/pagerank/service"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
)

var (
	appName = "linksrus-distributed-pagerank"
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
			Name:   "mode",
			EnvVar: "MODE",
			Usage:  "The operation mode to use (master or worker)",
		},
		cli.StringFlag{
			Name:   "master-endpoint",
			EnvVar: "MASTER_ENDPOINT",
			Usage:  "The endpoint for connecting to the master node (worker mode)",
		},
		cli.DurationFlag{
			Name:   "master-dial-timeout",
			EnvVar: "MASTER_DIAL_TIMEOUT",
			Value:  10 * time.Second,
			Usage:  "The timeout for establishing a connection to the master node (worker mode)",
		},
		cli.StringFlag{
			Name:   "link-graph-api",
			EnvVar: "LINK_GRAPH_API",
			Usage:  "The gRPC endpoint for connecting to the link graph (worker mode)",
		},
		cli.StringFlag{
			Name:   "text-indexer-api",
			EnvVar: "TEXT_INDEXER_API",
			Usage:  "The gRPC endpoint for connecting to the text indexer (worker mode)",
		},
		cli.IntFlag{
			Name:   "num-workers",
			Value:  runtime.NumCPU(),
			EnvVar: "NUM_WORKERS",
			Usage:  "The number of workers to use for calculating PageRank scores (worker mode)",
		},
		cli.IntFlag{
			Name:   "master-port",
			Value:  8080,
			EnvVar: "MASTER_PORT",
			Usage:  "The port where the master listens for incoming connections (master mode)",
		},
		cli.DurationFlag{
			Name:   "update-interval",
			Value:  5 * time.Minute,
			EnvVar: "UPDATE_INTERVAL",
			Usage:  "The time between subsequent PageRank score updates (master mode)",
		},
		cli.IntFlag{
			Name:   "min-workers-for-update",
			Value:  0,
			EnvVar: "MIN_WORKERS_FOR_UPDATE",
			Usage:  "The minimum number of workers that must be connected before making a new pass; 0 indicates that at least one worker is required (master mode)",
		},
		cli.DurationFlag{
			Name:   "worker-acquire-timeout",
			Value:  0,
			EnvVar: "WORKER_ACQUIRE_TIMEOUT",
			Usage:  "The time that the master waits for the requested number of workers to be connected before skipping a pass (master mode)",
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
	var (
		serviceRunner interface {
			Run(context.Context) error
		}
		ctx, cancelFn = context.WithCancel(context.Background())
		err           error
		logger        = logger.WithField("mode", appCtx.String("mode"))
	)
	defer cancelFn()

	switch appCtx.String("mode") {
	case "master":
		if serviceRunner, err = service.NewMasterNode(service.MasterConfig{
			ListenAddress:        fmt.Sprintf(":%d", appCtx.Int("master-port")),
			UpdateInterval:       appCtx.Duration("update-interval"),
			MinWorkers:           appCtx.Int("min-workers-for-update"),
			WorkerAcquireTimeout: appCtx.Duration("worker-acquire-timeout"),
			Logger:               logger,
		}); err != nil {
			return err
		}
	case "worker":
		graphAPI, indexerAPI, err := getAPIs(appCtx.String("link-graph-api"), appCtx.String("text-indexer-api"))
		if err != nil {
			return err
		}

		if serviceRunner, err = service.NewWorkerNode(service.WorkerConfig{
			MasterEndpoint:    appCtx.String("master-endpoint"),
			MasterDialTimeout: appCtx.Duration("master-dial-timeout"),
			GraphAPI:          graphAPI,
			IndexAPI:          indexerAPI,
			ComputeWorkers:    appCtx.Int("num-workers"),
			Logger:            logger,
		}); err != nil {
			return err
		}
	default:
		return xerrors.Errorf("unsupported mode %q; please specify one of: master, worker", appCtx.String("role"))
	}

	var wg sync.WaitGroup

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

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := serviceRunner.Run(ctx); err != nil {
			logger.WithField("err", err).Error("pagerank service exited with error")
			cancelFn()
		}
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

func getAPIs(linkGraphAPI, textIndexerAPI string) (*linkgraphapi.LinkGraphClient, *textindexerapi.TextIndexerClient, error) {
	if linkGraphAPI == "" {
		return nil, nil, xerrors.Errorf("link graph API must be specified with --link-graph-api")
	}
	if textIndexerAPI == "" {
		return nil, nil, xerrors.Errorf("text indexer API must be specified with --text-indexer-api")
	}

	dialCtx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFn()
	linkGraphConn, err := grpc.DialContext(dialCtx, linkGraphAPI, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, xerrors.Errorf("could not connect to link graph API: %w", err)
	}
	graphCli := linkgraphapi.NewLinkGraphClient(context.Background(), linkgraphproto.NewLinkGraphClient(linkGraphConn))

	dialCtx, cancelFn = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFn()
	indexerConn, err := grpc.DialContext(dialCtx, textIndexerAPI, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, xerrors.Errorf("could not connect to text indexer API: %w", err)
	}
	indexerCli := textindexerapi.NewTextIndexerClient(context.Background(), textindexerproto.NewTextIndexerClient(indexerConn))

	return graphCli, indexerCli, nil
}
