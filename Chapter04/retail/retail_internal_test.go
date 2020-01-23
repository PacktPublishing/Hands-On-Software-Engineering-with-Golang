package retail

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"testing"
)

func TestPriceForItem(t *testing.T) {
	pc := &PriceCalculator{
		priceSvc: stubSvcCaller{
			"price": 42.0,
		},
		vatSvc: stubSvcCaller{
			"vat_rate": 0.10,
		},
	}

	got, err := pc.PriceForItem("foo")
	if err != nil {
		t.Fatal(err)
	}

	if exp := 46.2; got != exp {
		t.Fatalf("expected calculated retail price to be %f; got %f", exp, got)
	}
}

func TestVatSvcErrorHandling(t *testing.T) {
	pc := &PriceCalculator{
		priceSvc: stubSvcCaller{
			"price": 42.0,
		},
		vatSvc: stubErrCaller{
			err: errors.New("unexpected response status code: 404"),
		},
	}

	expErr := "unable to retrieve vat percent: call to remote service failed: unexpected response status code: 404"
	_, err := pc.PriceForItem("foo")
	if err == nil || err.Error() != expErr {
		t.Fatalf("expected to get error:\n %s\ngot:\n %v", expErr, err)
	}
}

func TestVatInclusivePrice(t *testing.T) {
	specs := []struct {
		price   float64
		vatRate float64
		exp     float64
	}{
		{42.0, 0.1, 46.2},
		{10.0, 0, 10.0},
	}

	for specIndex, spec := range specs {
		if got := vatInclusivePrice(spec.price, spec.vatRate); got != spec.exp {
			t.Errorf("[spec %d] expected to get: %f; got: %f", specIndex, spec.exp, got)
		}
	}
}

type stubSvcCaller map[string]interface{}

func (c stubSvcCaller) Call(map[string]interface{}) (io.ReadCloser, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

type stubErrCaller struct {
	err error
}

func (c stubErrCaller) Call(map[string]interface{}) (io.ReadCloser, error) {
	return nil, c.err
}
