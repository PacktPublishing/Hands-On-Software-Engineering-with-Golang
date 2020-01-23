package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi"
	linkgraphproto "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi/proto"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi"
	textindexerproto "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi/proto"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/service/frontend"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
)

var (
	appName = "linksrus-frontend"
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
			Name:   "results-per-page",
			Value:  10,
			EnvVar: "RESULTS_PER_PAGE",
			Usage:  "The number of entries for each result page",
		},
		cli.IntFlag{
			Name:   "max-summary-length",
			Value:  256,
			EnvVar: "RESULTS_PER_PAGE",
			Usage:  "The maximum length of the summary for each matched document in characters",
		},
		cli.IntFlag{
			Name:   "fe-port",
			Value:  8080,
			EnvVar: "FE_PORT",
			Usage:  "The port for exposing the front-end",
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

	graphAPI, indexerAPI, err := getAPIs(ctx, appCtx.String("link-graph-api"), appCtx.String("text-indexer-api"))
	if err != nil {
		return err
	}

	var frontendCfg frontend.Config
	frontendCfg.ListenAddr = fmt.Sprintf(":%d", appCtx.Int("fe-port"))
	frontendCfg.ResultsPerPage = appCtx.Int("results-per-page")
	frontendCfg.MaxSummaryLength = appCtx.Int("max-summary-length")
	frontendCfg.GraphAPI = graphAPI
	frontendCfg.IndexAPI = indexerAPI
	frontendCfg.Logger = logger
	feSvc, err := frontend.NewService(frontendCfg)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := feSvc.Run(ctx); err != nil {
			logger.WithField("err", err).Error("front-end service exited with error")
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
