package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/minhajuddin/onioncache/pkg/core"
)

type PricingPlanCache struct{}

func (p *PricingPlanCache) ID() string {
	return "pricing_plan_cache"
}

func (p *PricingPlanCache) VersionSQL() string {
	return `SELECT
    /* to avoid random sorting */
    MD5(CAST((ARRAY_AGG(t.* ORDER BY t)) AS text)) version
		FROM
		pricing_plans t;
	`
}

func (p *PricingPlanCache) RowSQL() string {
	return "SELECT name FROM pricing_plans"
	// return "SELECT id, name, price, created_at FROM pricing_plans"
}

// TODO: Put this in a wrapper which manages updates using mutexes
var _ core.Cache = &PricingPlanCache{}

func main() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	r := core.NewRegistry(conn).
		AddCache(&PricingPlanCache{}).
		StartLoopGoRoutine()

	fmt.Println("main loop is sleeping for 10 minutes...")
	time.Sleep(10 * time.Minute)

	fmt.Println("main loop is exiting now, so stopping registry")
	r.StopLoopGoRoutine()
}
