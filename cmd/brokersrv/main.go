package main

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/vmkteam/brokersrv/pkg/app"

	"github.com/BurntSushi/toml"
	"github.com/namsral/flag"
	"github.com/nats-io/nats.go"
	"github.com/vmkteam/embedlog"
)

const appName = "brokersrv"

var (
	fs           = flag.NewFlagSetWithEnvPrefix(os.Args[0], "BROKERSRV", 0)
	flConfigPath = fs.String("config", "config.toml", "Path to config file")
	flVerbose    = fs.Bool("verbose", false, "enable debug output")
	flJSONLogs   = fs.Bool("json", false, "enable json output")
	flDev        = fs.Bool("dev", false, "enable dev mode")
	cfg          app.Config
)

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	flag.DefaultConfigFlagname = "config.flag"
	exitOnError(fs.Parse(os.Args[1:]))

	// setup logger
	sl, ctx := embedlog.NewLogger(*flVerbose, *flJSONLogs), context.Background()
	if *flDev {
		sl = embedlog.NewDevLogger()
	}
	slog.SetDefault(sl.Log()) // set default logger

	version := appVersion()
	sl.Print(ctx, "starting", "app", appName, "version", version)
	if _, err := toml.DecodeFile(*flConfigPath, &cfg); err != nil {
		exitOnError(err)
	}

	// connect to NATS cluster
	nc, err := nats.Connect(cfg.NATS.URL, nats.Name(appName), nats.MaxReconnects(100), nats.ReconnectWait(3*time.Second))
	exitOnError(err)

	// create & run app
	a := app.New(appName, sl, cfg, nc)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Run
	go func() {
		if err := a.Run(ctx); err != nil {
			exitOnError(err)
		}
	}()
	<-quit
	a.Shutdown(5 * time.Second)
}

// exitOnError calls log.Fatal if err wasn't nil.
func exitOnError(err error) {
	if err != nil {
		//nolint:sloglint
		slog.Error(err.Error())
		os.Exit(1)
	}
}

// appVersion returns app version from VCS info
func appVersion() string {
	result := "devel"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return result
	}

	for _, v := range info.Settings {
		if v.Key == "vcs.revision" {
			result = v.Value
		}
	}

	if len(result) > 8 {
		result = result[:8]
	}

	return result
}
