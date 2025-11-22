package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/minhajuddin/onioncache/pkg/core"
)

type PricingPlanCache struct {
	Plans *core.SafeMap[int, PricingPlan]
}

type PricingPlan struct {
	ID   int
	Name string
}

func (p *PricingPlan) String() string {
	return fmt.Sprintf("PricingPlan{ID: %d, Name: %s}", p.ID, p.Name)
}

func (p *PricingPlanCache) ID() string {
	return "pricing_plan_cache"
}

func (p *PricingPlanCache) Reset(rows pgx.Rows) error {
	plans, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[PricingPlan])
	if err != nil {
		fmt.Println("Error collecting pricing plans:", err)
		return err
	}

	plansMap := make(map[int]PricingPlan)
	for _, plan := range plans {
		plansMap[plan.ID] = *plan
	}
	p.Plans.SetAll(plansMap)

	fmt.Printf("Pricing Plans in Cache: %#v\n", plans)

	return nil
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
	return "SELECT id, name FROM pricing_plans"
}

var _ core.Cache = &PricingPlanCache{}

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	ppc := &PricingPlanCache{
		Plans: core.NewSafeMap[int, PricingPlan](),
	}
	r := core.NewRegistry(conn, nil).
		AddCache(ppc).
		StartLoopGoroutine(ctx)

	go func() {
		for {
			thirdPlan, ok := ppc.Plans.Get(3)
			fmt.Println(">> Fetched plan with ID 3:", thirdPlan, "found:", ok)
			time.Sleep(3 * time.Second)
		}
	}()

	fmt.Println("main loop is sleeping for 10 minutes...")
	time.Sleep(10 * time.Minute)

	fmt.Println("main loop is exiting now, so stopping registry")
	r.StopLoopGoroutine()
}
