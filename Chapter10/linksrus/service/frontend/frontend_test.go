package frontend

import (
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/service/frontend/mocks"
	"github.com/golang/mock/gomock"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(FrontendTestSuite))

type FrontendTestSuite struct {
}

func (s *FrontendTestSuite) TestSubmitLink(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	fe, mockGraph, _ := s.setupService(c, ctrl)
	mockGraph.EXPECT().UpsertLink(&graph.Link{
		URL: "http://www.example.com",
	}).Return(nil)

	req := httptest.NewRequest("POST", submitLinkEndpoint, nil)
	req.Form = url.Values{}
	req.Form.Add("link", "http://www.example.com")
	res := httptest.NewRecorder()
	fe.router.ServeHTTP(res, req)

	c.Assert(res.Code, gc.Equals, http.StatusOK)
}

func (s *FrontendTestSuite) TestSearch(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	mockIt := s.mockIterator(ctrl, 10)

	fe, _, mockIndex := s.setupService(c, ctrl)
	mockIndex.EXPECT().Search(gomock.Any()).Return(mockIt, nil)

	fe.tplExecutor = func(_ *template.Template, _ io.Writer, data map[string]interface{}) error {
		pgDetails := data["pagination"].(*paginationDetails)
		c.Assert(pgDetails.From, gc.Equals, 1)
		c.Assert(pgDetails.To, gc.Equals, 2)
		c.Assert(pgDetails.Total, gc.Equals, 10)
		c.Assert(pgDetails.PrevLink, gc.Equals, "")
		c.Assert(pgDetails.NextLink, gc.Equals, "/search?q=KEYWORD&offset=2")
		return nil
	}

	req := httptest.NewRequest("GET", searchEndpoint+"?q=KEYWORD", nil)
	res := httptest.NewRecorder()
	fe.router.ServeHTTP(res, req)

	c.Assert(res.Code, gc.Equals, http.StatusOK)
}

func (s *FrontendTestSuite) TestPaginatedSearchOnSecondPage(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	mockIt := s.mockIterator(ctrl, 10)

	fe, _, mockIndex := s.setupService(c, ctrl)
	mockIndex.EXPECT().Search(gomock.Any()).Return(mockIt, nil)

	fe.tplExecutor = func(_ *template.Template, _ io.Writer, data map[string]interface{}) error {
		pgDetails := data["pagination"].(*paginationDetails)
		c.Assert(pgDetails.From, gc.Equals, 3)
		c.Assert(pgDetails.To, gc.Equals, 4)
		c.Assert(pgDetails.Total, gc.Equals, 10)
		c.Assert(pgDetails.PrevLink, gc.Equals, "/search?q=KEYWORD")
		c.Assert(pgDetails.NextLink, gc.Equals, "/search?q=KEYWORD&offset=4")
		return nil
	}

	req := httptest.NewRequest("GET", searchEndpoint+"?q=KEYWORD&offset=2", nil)
	res := httptest.NewRecorder()
	fe.router.ServeHTTP(res, req)

	c.Assert(res.Code, gc.Equals, http.StatusOK)
}

func (s *FrontendTestSuite) TestPaginatedSearchOnThirdPage(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	mockIt := s.mockIterator(ctrl, 10)

	fe, _, mockIndex := s.setupService(c, ctrl)
	mockIndex.EXPECT().Search(gomock.Any()).Return(mockIt, nil)

	fe.tplExecutor = func(_ *template.Template, _ io.Writer, data map[string]interface{}) error {
		pgDetails := data["pagination"].(*paginationDetails)
		c.Assert(pgDetails.From, gc.Equals, 5)
		c.Assert(pgDetails.To, gc.Equals, 6)
		c.Assert(pgDetails.Total, gc.Equals, 10)
		c.Assert(pgDetails.PrevLink, gc.Equals, "/search?q=KEYWORD&offset=2")
		c.Assert(pgDetails.NextLink, gc.Equals, "/search?q=KEYWORD&offset=6")
		return nil
	}

	req := httptest.NewRequest("GET", searchEndpoint+"?q=KEYWORD&offset=4", nil)
	res := httptest.NewRecorder()
	fe.router.ServeHTTP(res, req)

	c.Assert(res.Code, gc.Equals, http.StatusOK)
}

func (s *FrontendTestSuite) TestPaginatedSearchOnLastPage(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	mockIt := s.mockIterator(ctrl, 10)

	fe, _, mockIndex := s.setupService(c, ctrl)
	mockIndex.EXPECT().Search(gomock.Any()).Return(mockIt, nil)

	fe.tplExecutor = func(_ *template.Template, _ io.Writer, data map[string]interface{}) error {
		pgDetails := data["pagination"].(*paginationDetails)
		c.Assert(pgDetails.From, gc.Equals, 9)
		c.Assert(pgDetails.To, gc.Equals, 10)
		c.Assert(pgDetails.Total, gc.Equals, 10)
		c.Assert(pgDetails.PrevLink, gc.Equals, "/search?q=KEYWORD&offset=6")
		c.Assert(pgDetails.NextLink, gc.Equals, "")
		return nil
	}

	req := httptest.NewRequest("GET", searchEndpoint+"?q=KEYWORD&offset=8", nil)
	res := httptest.NewRecorder()
	fe.router.ServeHTTP(res, req)

	c.Assert(res.Code, gc.Equals, http.StatusOK)
}

func (s *FrontendTestSuite) setupService(c *gc.C, ctrl *gomock.Controller) (*Service, *mocks.MockGraphAPI, *mocks.MockIndexAPI) {
	mockGraph := mocks.NewMockGraphAPI(ctrl)
	mockIndexer := mocks.NewMockIndexAPI(ctrl)

	fe, err := NewService(Config{
		GraphAPI:       mockGraph,
		IndexAPI:       mockIndexer,
		ListenAddr:     ":0",
		ResultsPerPage: 2,
	})
	c.Assert(err, gc.IsNil)

	return fe, mockGraph, mockIndexer
}

func (s *FrontendTestSuite) mockIterator(ctrl *gomock.Controller, numResults int) *mocks.MockIterator {
	it := mocks.NewMockIterator(ctrl)
	it.EXPECT().TotalCount().Return(uint64(numResults))

	nextDoc := 0
	it.EXPECT().Next().DoAndReturn(func() bool {
		nextDoc++
		return nextDoc < numResults
	}).MaxTimes(numResults)

	it.EXPECT().Document().DoAndReturn(func() *index.Document {
		return &index.Document{
			URL:     fmt.Sprintf("http://www.example.com/%d", nextDoc),
			Title:   fmt.Sprintf("Title %d", nextDoc),
			Content: fmt.Sprintf("Keyword %d", nextDoc),
		}
	}).MaxTimes(numResults)

	it.EXPECT().Error().Return(nil)
	it.EXPECT().Close().Return(nil)
	return it
}
