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
	Plans map[int]PricingPlan
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

	// TODO: mutex
	plansMap := make(map[int]PricingPlan)
	for _, plan := range plans {
		plansMap[plan.ID] = *plan
	}
	p.Plans = plansMap

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

	ppc := &PricingPlanCache{}
	r := core.NewRegistry(conn).
		AddCache(ppc).
		StartLoopGoRoutine()

	go func() {
		for {
			thirdPlan, ok := ppc.Plans[3]
			fmt.Println(">> Fetched plan with ID 3:", thirdPlan, "found:", ok)
			time.Sleep(3 * time.Second)
		}
	}()

	fmt.Println("main loop is sleeping for 10 minutes...")
	time.Sleep(10 * time.Minute)

	fmt.Println("main loop is exiting now, so stopping registry")
	r.StopLoopGoRoutine()
}
