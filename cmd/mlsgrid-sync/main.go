package main

import (
	"fmt"
	"os"

	"github.com/piotrsenkow/mlsgrid-sync/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
