package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/store/es"
	memindex "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/store/memory"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi/proto"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
)

var (
	appName = "linksrus-textindexer"
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
			Name:   "text-indexer-uri",
			Value:  "in-memory://",
			EnvVar: "TEXT_INDEXER_URI",
			Usage:  "The URI for connecting to the text indexer (supported URIs: in-memory://, es://node1:9200,...,nodeN:9200)",
		},
		cli.IntFlag{
			Name:   "grpc-port",
			Value:  8080,
			EnvVar: "GRPC_PORT",
			Usage:  "The port for exposing the gRPC endpoints for accessing the text indexer",
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

	indexer, err := getTextIndexer(appCtx.String("text-indexer-uri"))
	if err != nil {
		return err
	}

	// Start gRPC server
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", appCtx.Int("grpc-port")))
	if err != nil {
		return err
	}
	defer func() { _ = grpcListener.Close() }()

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv := grpc.NewServer()
		proto.RegisterTextIndexerServer(srv, textindexerapi.NewTextIndexerServer(indexer))
		logger.WithField("port", appCtx.Int("grpc-port")).Info("listening for gRPC connections")
		_ = srv.Serve(grpcListener)
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
			_ = grpcListener.Close()
			_ = pprofListener.Close()
			cancelFn()
		case <-ctx.Done():
		}
	}()

	// Keep running until we receive a signal
	wg.Wait()
	return nil
}

func getTextIndexer(textIndexerURI string) (index.Indexer, error) {
	if textIndexerURI == "" {
		return nil, xerrors.Errorf("text indexer URI must be specified with --text-indexer-uri")
	}

	uri, err := url.Parse(textIndexerURI)
	if err != nil {
		return nil, xerrors.Errorf("could not parse text indexer URI: %w", err)
	}

	switch uri.Scheme {
	case "in-memory":
		logger.Info("using in-memory indexer")
		return memindex.NewInMemoryBleveIndexer()
	case "es":
		nodes := strings.Split(uri.Host, ",")
		for i := 0; i < len(nodes); i++ {
			nodes[i] = "http://" + nodes[i]
		}
		logger.Info("using ES indexer")
		return es.NewElasticSearchIndexer(nodes, false)
	default:
		return nil, xerrors.Errorf("unsupported link graph URI scheme: %q", uri.Scheme)
	}
}
