package main

import (
	"fmt"

	"github.com/minhajuddin/onioncache/pkg/core"
)

func main() {
	r := core.NewRegistry()

	realmToName := map[string]string{}

	fmt.Println("Hello, World!", r, realmToName)
}
