package retail_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter04/retail"
)

func TestPriceForItem(t *testing.T) {
	priceSvc := spinUpTestServer(t, map[string]interface{}{"price": 10.0})
	defer priceSvc.Close()

	vatSvc := spinUpTestServer(t, map[string]interface{}{"vat_rate": 0.29})
	defer vatSvc.Close()

	pc := retail.NewPriceCalculator(priceSvc.URL, vatSvc.URL)
	got, err := pc.PriceForItem("1b6f8e0f-bbda-4f4e-ade5-aa1abcc99586")
	if err != nil {
		t.Fatal(err)
	}

	if exp := 12.9; got != exp {
		t.Fatalf("expected calculated retail price to be %f; got %f", exp, got)
	}
}

func spinUpTestServer(t *testing.T, res map[string]interface{}) *httptest.Server {
	encResponse, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, wErr := w.Write(encResponse); wErr != nil {
			t.Fatal(wErr)
		}
	}))
}
