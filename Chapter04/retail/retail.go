package retail

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/xerrors"
)

type svcCaller interface {
	Call(req map[string]interface{}) (io.ReadCloser, error)
}

// PriceCalculator estimates the VAT-inclusive retail prices of items.
type PriceCalculator struct {
	priceSvc svcCaller
	vatSvc   svcCaller
}

// NewPriceCalculator creates a PriceCalculator instance that queries the
// provided endpoints for item price and VAT information.
func NewPriceCalculator(priceSvcEndpoint, vatSvcEndpoint string) *PriceCalculator {
	return &PriceCalculator{
		priceSvc: restEndpointCaller(priceSvcEndpoint),
		vatSvc:   restEndpointCaller(vatSvcEndpoint),
	}
}

// PriceForItem calculates the VAT-inclusive retail price of itemUUID with the
// currently applicable VAT rates.
func (pc *PriceCalculator) PriceForItem(itemUUID string) (float64, error) {
	return pc.PriceForItemAtDate(itemUUID, time.Now())
}

// PriceForItemAtDate calculates the VAT-inclusive retail price of itemUUID
// with the VAT rates that applied at a particular date.
func (pc *PriceCalculator) PriceForItemAtDate(itemUUID string, date time.Time) (float64, error) {
	priceRes := struct {
		Price float64 `json:"price"`
	}{}

	if err := pc.callService(
		pc.priceSvc,
		map[string]interface{}{
			"item":   itemUUID,
			"period": date,
		},
		&priceRes,
	); err != nil {
		return 0, xerrors.Errorf("unable to retrieve item price: %w", err)
	}

	vatRes := struct {
		Rate float64 `json:"vat_rate"`
	}{}

	if err := pc.callService(
		pc.vatSvc,
		map[string]interface{}{"period": date},
		&vatRes,
	); err != nil {
		return 0, xerrors.Errorf("unable to retrieve vat percent: %w", err)
	}

	return vatInclusivePrice(priceRes.Price, vatRes.Rate), nil
}

// vatInclusivePrice applies a vat rate to a price and returns the result.
func vatInclusivePrice(price, rate float64) float64 {
	return price * (1.0 + rate)
}

// callService performs an RPC and decodes the response into res.
func (pc *PriceCalculator) callService(svc svcCaller, req map[string]interface{}, res interface{}) error {
	svcRes, err := svc.Call(req)
	if err != nil {
		return xerrors.Errorf("call to remote service failed: %w", err)
	}
	defer drainAndClose(svcRes)

	if err = json.NewDecoder(svcRes).Decode(res); err != nil {
		return xerrors.Errorf("unable to decode remote service response: %w", err)
	}

	return nil
}

// restEndpointCaller is a convenience type for perfoming GET requests to REST
// endpoints.
type restEndpointCaller string

// Call implements svcCaller for the restEndpointCaller type.
func (ep restEndpointCaller) Call(req map[string]interface{}) (io.ReadCloser, error) {
	var params = make(url.Values)
	for k, v := range req {
		params.Set(k, fmt.Sprint(v))
	}

	url := fmt.Sprintf("%s?%s", string(ep), params.Encode())
	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		drainAndClose(res.Body)
		return nil, xerrors.Errorf("unexpected response status code: %d", res.StatusCode)
	}

	return res.Body, nil
}

func drainAndClose(r io.ReadCloser) {
	if r == nil {
		return
	}
	_, _ = io.Copy(ioutil.Discard, r)
	_ = r.Close()
}
