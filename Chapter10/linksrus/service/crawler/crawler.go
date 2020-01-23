package crawler

import (
	"context"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	crawler_pipeline "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/crawler"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/crawler/privnet"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/partition"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/juju/clock"
	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

//go:generate mockgen -package mocks -destination mocks/mocks.go github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/service/crawler GraphAPI,IndexAPI
//go:generate mockgen -package mocks -destination mocks/mock_iterator.go github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph LinkIterator

// GraphAPI defines as set of API methods for accessing the link graph.
type GraphAPI interface {
	UpsertLink(link *graph.Link) error
	UpsertEdge(edge *graph.Edge) error
	RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error
	Links(fromID, toID uuid.UUID, retrievedBefore time.Time) (graph.LinkIterator, error)
}

// IndexAPI defines a set of API methods for indexing crawled documents.
type IndexAPI interface {
	Index(doc *index.Document) error
}

// Config encapsulates the settings for configuring the web-crawler service.
type Config struct {
	// An API for managing and interating links and edges in the link graph.
	GraphAPI GraphAPI

	// An API for indexing documents.
	IndexAPI IndexAPI

	// An API for detecting private network addresses. If not specified,
	// a default implementation that handles the private network ranges
	// defined in RFC1918 will be used instead.
	PrivateNetworkDetector crawler_pipeline.PrivateNetworkDetector

	// An API for performing HTTP requests. If not specified,
	// http.DefaultClient will be used instead.
	URLGetter crawler_pipeline.URLGetter

	// An API for detecting the partition assignments for this service.
	PartitionDetector partition.Detector

	// A clock instance for generating time-related events. If not specified,
	// the default wall-clock will be used instead.
	Clock clock.Clock

	// The number of concurrent workers used for retrieving links.
	FetchWorkers int

	// The time between subsequent crawler passes.
	UpdateInterval time.Duration

	// The minimum amount of time before re-indexing an already-crawled link.
	ReIndexThreshold time.Duration

	// The logger to use. If not defined an output-discarding logger will
	// be used instead.
	Logger *logrus.Entry
}

func (cfg *Config) validate() error {
	var err error
	if cfg.PrivateNetworkDetector == nil {
		cfg.PrivateNetworkDetector, err = privnet.NewDetector()
	}
	if cfg.URLGetter == nil {
		cfg.URLGetter = http.DefaultClient
	}
	if cfg.GraphAPI == nil {
		err = multierror.Append(err, xerrors.Errorf("graph API has not been provided"))
	}
	if cfg.IndexAPI == nil {
		err = multierror.Append(err, xerrors.Errorf("index API has not been provided"))
	}
	if cfg.PartitionDetector == nil {
		err = multierror.Append(err, xerrors.Errorf("partition detector has not been provided"))
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.WallClock
	}
	if cfg.FetchWorkers <= 0 {
		err = multierror.Append(err, xerrors.Errorf("invalid value for fetch workers"))
	}
	if cfg.UpdateInterval == 0 {
		err = multierror.Append(err, xerrors.Errorf("invalid value for update interval"))
	}
	if cfg.ReIndexThreshold == 0 {
		err = multierror.Append(err, xerrors.Errorf("invalid value for re-index threshold"))
	}
	if cfg.Logger == nil {
		cfg.Logger = logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard})
	}
	return err
}

// Service implements the web-crawler component for the Links 'R' Us project.
type Service struct {
	cfg     Config
	crawler *crawler_pipeline.Crawler
}

// NewService creates a new crawler service instance with the specified config.
func NewService(cfg Config) (*Service, error) {
	if err := cfg.validate(); err != nil {
		return nil, xerrors.Errorf("crawler service: config validation failed: %w", err)
	}

	return &Service{
		cfg: cfg,
		crawler: crawler_pipeline.NewCrawler(crawler_pipeline.Config{
			PrivateNetworkDetector: cfg.PrivateNetworkDetector,
			URLGetter:              cfg.URLGetter,
			Graph:                  cfg.GraphAPI,
			Indexer:                cfg.IndexAPI,
			FetchWorkers:           cfg.FetchWorkers,
		}),
	}, nil
}

// Name implements service.Service
func (svc *Service) Name() string { return "crawler" }

// Run implements service.Service
func (svc *Service) Run(ctx context.Context) error {
	svc.cfg.Logger.WithField("update_interval", svc.cfg.UpdateInterval.String()).Info("starting service")
	defer svc.cfg.Logger.Info("stopped service")

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-svc.cfg.Clock.After(svc.cfg.UpdateInterval):
			curPartition, numPartitions, err := svc.cfg.PartitionDetector.PartitionInfo()
			if err != nil {
				if xerrors.Is(err, partition.ErrNoPartitionDataAvailableYet) {
					svc.cfg.Logger.Warn("deferring crawler update pass: partition data not yet available")
					continue
				}
				return err
			}
			if err := svc.crawlGraph(ctx, curPartition, numPartitions); err != nil {
				return err
			}
		}
	}
}

func (svc *Service) crawlGraph(ctx context.Context, curPartition, numPartitions int) error {
	partRange, err := partition.NewFullRange(numPartitions)
	if err != nil {
		return xerrors.Errorf("crawler: unable to compute ID ranges for partition: %w", err)
	}

	fromID, toID, err := partRange.PartitionExtents(curPartition)
	if err != nil {
		return xerrors.Errorf("crawler: unable to compute ID ranges for partition: %w", err)
	}

	svc.cfg.Logger.WithFields(logrus.Fields{
		"partition":      curPartition,
		"num_partitions": numPartitions,
	}).Info("starting new crawl pass")

	startAt := svc.cfg.Clock.Now()
	linkIt, err := svc.cfg.GraphAPI.Links(fromID, toID, svc.cfg.Clock.Now().Add(-svc.cfg.ReIndexThreshold))
	if err != nil {
		return xerrors.Errorf("crawler: unable to retrieve links iterator: %w", err)
	}

	processed, err := svc.crawler.Crawl(ctx, linkIt)
	if err != nil {
		return xerrors.Errorf("crawler: unable to complete crawling the link graph: %w", err)
	} else if err = linkIt.Close(); err != nil {
		return xerrors.Errorf("crawler: unable to complete crawling the link graph: %w", err)
	}

	svc.cfg.Logger.WithFields(logrus.Fields{
		"processed_link_count": processed,
		"elapsed_time":         svc.cfg.Clock.Now().Sub(startAt).String(),
	}).Info("completed crawl pass")
	return nil
}
