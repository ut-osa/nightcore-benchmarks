// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	pb "cs.utexas.edu/zjia/hipster-frontend/genproto"
	"cs.utexas.edu/zjia/hipster-frontend/money"
)

func (fe *frontendServer) homeHandler(ctx context.Context, input map[string]string) (map[string]interface{}, error) {
	log := ctx.Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.WithField("currency", defaultCurrency).Info("home")
	currencies, err := fe.getCurrencies(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not retrieve currencies")
	}
	products, err := fe.getProducts(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not retrieve products")
	}
	cart, err := fe.getCart(ctx, sessionID(input))
	if err != nil {
		return nil, errors.Wrap(err, "could not retrieve cart")
	}

	type productView struct {
		Item  *pb.Product
		Price *pb.Money
	}
	ps := make([]productView, len(products))
	for i, p := range products {
		price, err := fe.convertCurrency(ctx, p.GetPriceUsd(), defaultCurrency)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to do currency conversion for product %s", p.GetId())
		}
		ps[i] = productView{p, price}
	}

	return map[string]interface{}{
		"currencies":    currencies,
		"products":      ps,
		"cart_size":     len(cart),
		"ad":            fe.chooseAd(ctx, []string{}, log),
	}, nil
}

func (fe *frontendServer) productHandler(ctx context.Context, input map[string]string) (map[string]interface{}, error) {
	log := ctx.Value(ctxKeyLog{}).(logrus.FieldLogger)
	id := input["id"]
	if id == "" {
		return nil, errors.New("product id not specified")
	}
	log.WithField("id", id).WithField("currency", defaultCurrency).
		Debug("serving product page")

	p, err := fe.getProduct(ctx, id)
	if err != nil {
		return nil, errors.Wrap(err, "could not retrieve product")
	}
	currencies, err := fe.getCurrencies(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not retrieve currencies")
	}

	cart, err := fe.getCart(ctx, sessionID(input))
	if err != nil {
		return nil, errors.Wrap(err, "could not retrieve cart")
	}

	price, err := fe.convertCurrency(ctx, p.GetPriceUsd(), defaultCurrency)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert currency")
	}

	product := struct {
		Item  *pb.Product
		Price *pb.Money
	}{p, price}

	return map[string]interface{}{
		"ad":              fe.chooseAd(ctx, p.Categories, log),
		"currencies":      currencies,
		"product":         product,
		"cart_size":       len(cart),
	}, nil
}

func (fe *frontendServer) addToCartHandler(ctx context.Context, input map[string]string) (map[string]interface{}, error) {
	log := ctx.Value(ctxKeyLog{}).(logrus.FieldLogger)
	quantity, _ := strconv.ParseUint(input["quantity"], 10, 32)
	productID := input["product_id"]
	if productID == "" || quantity == 0 {
		return nil, errors.New("invalid form input")
	}
	log.WithField("product", productID).WithField("quantity", quantity).Debug("adding to cart")

	p, err := fe.getProduct(ctx, productID)
	if err != nil {
		return nil, errors.Wrap(err, "could not retrieve product")
	}

	if err := fe.insertCart(ctx, sessionID(input), p.GetId(), int32(quantity)); err != nil {
		return nil, errors.Wrap(err, "failed to add to cart")
	}
	return map[string]interface{}{
		"status": "ok",
	}, nil
}

func (fe *frontendServer) viewCartHandler(ctx context.Context, input map[string]string) (map[string]interface{}, error) {
	log := ctx.Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.Debug("view user cart")
	currencies, err := fe.getCurrencies(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not retrieve currencies")
	}
	cart, err := fe.getCart(ctx, sessionID(input))
	if err != nil {
		return nil, errors.Wrap(err, "could not retrieve cart")
	}

	recommendations, err := fe.getRecommendations(ctx, sessionID(input), cartIDs(cart))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get product recommendations")
	}

	shippingCost, err := fe.getShippingQuote(ctx, cart, defaultCurrency)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get shipping quote")
	}

	type cartItemView struct {
		Item     *pb.Product
		Quantity int32
		Price    *pb.Money
	}
	items := make([]cartItemView, len(cart))
	totalPrice := pb.Money{CurrencyCode: defaultCurrency}
	for i, item := range cart {
		p, err := fe.getProduct(ctx, item.GetProductId())
		if err != nil {
			return nil, errors.Wrapf(err, "could not retrieve product #%s", item.GetProductId())
		}
		price, err := fe.convertCurrency(ctx, p.GetPriceUsd(), defaultCurrency)
		if err != nil {
			return nil, errors.Wrapf(err, "could not convert currency for product #%s", item.GetProductId())
		}

		multPrice := money.MultiplySlow(*price, uint32(item.GetQuantity()))
		items[i] = cartItemView{
			Item:     p,
			Quantity: item.GetQuantity(),
			Price:    &multPrice}
		totalPrice = money.Must(money.Sum(totalPrice, multPrice))
	}
	totalPrice = money.Must(money.Sum(totalPrice, *shippingCost))

	year := time.Now().Year()
	return map[string]interface{}{
		"currencies":       currencies,
		"recommendations":  recommendations,
		"cart_size":        len(cart),
		"shipping_cost":    shippingCost,
		"total_cost":       totalPrice,
		"items":            items,
		"expiration_years": []int{year, year + 1, year + 2, year + 3, year + 4},
	}, nil
}

func (fe *frontendServer) placeOrderHandler(ctx context.Context, input map[string]string) (map[string]interface{}, error) {
	log := ctx.Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.Debug("placing order")

	var (
		email         = input["email"]
		streetAddress = input["street_address"]
		zipCode, _    = strconv.ParseInt(input["zip_code"], 10, 32)
		city          = input["city"]
		state         = input["state"]
		country       = input["country"]
		ccNumber      = input["credit_card_number"]
		ccMonth, _    = strconv.ParseInt(input["credit_card_expiration_month"], 10, 32)
		ccYear, _     = strconv.ParseInt(input["credit_card_expiration_year"], 10, 32)
		ccCVV, _      = strconv.ParseInt(input["credit_card_cvv"], 10, 32)
	)

	order, err := pb.NewCheckoutServiceClient(fe.conn).
		PlaceOrder(ctx, &pb.PlaceOrderRequest{
			Email: email,
			CreditCard: &pb.CreditCardInfo{
				CreditCardNumber:          ccNumber,
				CreditCardExpirationMonth: int32(ccMonth),
				CreditCardExpirationYear:  int32(ccYear),
				CreditCardCvv:             int32(ccCVV)},
			UserId:       sessionID(input),
			UserCurrency: defaultCurrency,
			Address: &pb.Address{
				StreetAddress: streetAddress,
				City:          city,
				State:         state,
				ZipCode:       int32(zipCode),
				Country:       country},
		})
	if err != nil {
		return nil, errors.Wrap(err, "failed to complete the order")
	}
	log.WithField("order", order.GetOrder().GetOrderId()).Info("order placed")

	order.GetOrder().GetItems()

	totalPaid := *order.GetOrder().GetShippingCost()
	for _, v := range order.GetOrder().GetItems() {
		totalPaid = money.Must(money.Sum(totalPaid, *v.GetCost()))
	}

	return map[string]interface{}{
		"order":           order.GetOrder(),
		"total_paid":      &totalPaid,
	}, nil
}

// chooseAd queries for advertisements available and randomly chooses one, if
// available. It ignores the error retrieving the ad since it is not critical.
func (fe *frontendServer) chooseAd(ctx context.Context, ctxKeys []string, log logrus.FieldLogger) *pb.Ad {
	ads, err := fe.getAd(ctx, ctxKeys)
	if err != nil {
		log.WithField("error", err).Warn("failed to retrieve ads")
		return nil
	}
	return ads[rand.Intn(len(ads))]
}

func currentCurrency(r *http.Request) string {
	return defaultCurrency
}

func sessionID(input map[string]string) string {
	value, exist := input["session_id"]
	if exist {
		return value
	} else {
		return ""
	}
}

func cartIDs(c []*pb.CartItem) []string {
	out := make([]string, len(c))
	for i, v := range c {
		out[i] = v.GetProductId()
	}
	return out
}

func renderMoney(money pb.Money) string {
	return fmt.Sprintf("%s %d.%02d", money.GetCurrencyCode(), money.GetUnits(), money.GetNanos()/10000000)
}
