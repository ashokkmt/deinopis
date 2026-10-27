// Command dino-sim starts an isolated local DEINOPIS registry demonstration.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"deinopis/internal/simulator"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runner, err := simulator.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer runner.Stop()
	fmt.Printf("SIMULATED local registry ready\nrun directory: %s\nchain ID: %s\nregistry: %s\ncode hash: %s\nPress Ctrl-C to stop the owned Anvil process. Run artifacts are retained.\n", runner.RunDir, runner.ChainID, runner.Registry.Hex(), runner.RegistryHash.Hex())
	<-ctx.Done()
}
