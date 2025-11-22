package main

import (
	"fmt"
	"time"

	"github.com/minhajuddin/onioncache/pkg/core"
)

type PricingPlanCache struct{}

var _ core.Cache = &PricingPlanCache{}

func main() {
	r := core.NewRegistry().
		AddCache(&PricingPlanCache{}).
		StartLoopGoRoutine()

	fmt.Println("main loop is sleeping for 10 minutes...")
	time.Sleep(10 * time.Minute)

	fmt.Println("main loop is exiting now, so stopping registry")
	r.StopLoopGoRoutine()
}
