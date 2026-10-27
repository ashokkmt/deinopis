package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"deinopis/internal/api"
	"deinopis/internal/cli"
	"deinopis/internal/connections"
	"deinopis/internal/decode"
	"deinopis/internal/ingest"
	"deinopis/internal/model"
	"deinopis/internal/output"
	"deinopis/internal/packs"
	"deinopis/internal/pipeline"
	"deinopis/internal/rawstore"
	"deinopis/internal/witness"
	"deinopis/web"
)

var Version = "dev"

func init() {
	if Version != "dev" {
		return
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" && len(setting.Value) >= 7 {
			Version = "dev-" + setting.Value[:7]
			return
		}
	}
}

func main() {
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "--marketplace-online") || strings.HasPrefix(arg, "--marketplace-url") {
			fmt.Fprintln(os.Stderr, "unsupported option: marketplace access is not part of DEINOPIS")
			os.Exit(2)
		}
	}
	if len(os.Args) == 2 && (os.Args[1] == "-v" || os.Args[1] == "--version") {
		fmt.Printf("DEINOPIS CLI Version: %s\n", Version)
		return
	}

	// 0. Ergonomic CLI dispatch (e.g. ./bin/dino -packs, ./bin/dino -replay all, ./bin/dino -stop, etc.)
	if len(os.Args) > 1 {
		cmd := os.Args[1]
		switch cmd {
		case "stop", "-stop", "--stop":
			cli.StopDaemon(cli.DefaultPIDFile)
			return
		case "status", "-status", "--status":
			cli.StatusDaemon(cli.DefaultPIDFile, ":9090")
			return
		case "sources", "-sources", "--sources":
			srcDir := "sources"
			for i, a := range os.Args {
				if (a == "-sources-dir" || a == "--sources-dir") && i+1 < len(os.Args) {
					srcDir = os.Args[i+1]
				}
			}
			if len(os.Args) >= 3 && !strings.HasPrefix(os.Args[2], "-") {
				sub := os.Args[2]
				switch sub {
				case "validate":
					for i, a := range os.Args {
						if (a == "-f" || a == "--f") && i+1 < len(os.Args) {
							cli.ValidateSourceCLI(os.Args[i+1])
							return
						}
					}
					fmt.Fprintln(os.Stderr, "Usage: dino sources validate -f <file.yaml>")
					return
				case "test":
					for i, a := range os.Args {
						if (a == "-f" || a == "--f") && i+1 < len(os.Args) {
							cli.TestSourceCLI(os.Args[i+1])
							return
						}
					}
					fmt.Fprintln(os.Stderr, "Usage: dino sources test -f <file.yaml>")
					return
				case "apply":
					for i, a := range os.Args {
						if (a == "-f" || a == "--f") && i+1 < len(os.Args) {
							cli.ApplySourceCLI(os.Args[i+1], ":9090", srcDir)
							return
						}
					}
					fmt.Fprintln(os.Stderr, "Usage: dino sources apply -f <file.yaml>")
					return
				case "rollback":
					cli.RollbackSourceCLI(":9090", srcDir)
					return
				case "list":
					cli.ListSourcesCLI(":9090", srcDir)
					return
				case "get":
					if len(os.Args) >= 4 {
						cli.GetSourceCLI(os.Args[3], ":9090", srcDir)
						return
					}
					fmt.Fprintln(os.Stderr, "Usage: dino sources get <name>")
					return
				}
			}
			cli.ListSourcesCLI(":9090", srcDir)
			return
		case "marketplace", "-marketplace", "--marketplace":
			fmt.Fprintln(os.Stderr, "unsupported command: marketplace distribution has been removed")
			os.Exit(2)
		case "sinks", "-sinks", "--sinks":
			sinkDir := "sinks"
			for i, a := range os.Args {
				if (a == "-sinks-dir" || a == "--sinks-dir") && i+1 < len(os.Args) {
					sinkDir = os.Args[i+1]
				}
			}
			if len(os.Args) >= 3 && !strings.HasPrefix(os.Args[2], "-") {
				sub := os.Args[2]
				switch sub {
				case "validate":
					for i, a := range os.Args {
						if (a == "-f" || a == "--f") && i+1 < len(os.Args) {
							cli.ValidateSinkCLI(os.Args[i+1])
							return
						}
					}
					fmt.Fprintln(os.Stderr, "Usage: dino sinks validate -f <file.yaml>")
					return
				case "test":
					for i, a := range os.Args {
						if (a == "-f" || a == "--f") && i+1 < len(os.Args) {
							cli.TestSinkCLI(os.Args[i+1])
							return
						}
					}
					fmt.Fprintln(os.Stderr, "Usage: dino sinks test -f <file.yaml>")
					return
				case "apply":
					for i, a := range os.Args {
						if (a == "-f" || a == "--f") && i+1 < len(os.Args) {
							cli.ApplySinkCLI(os.Args[i+1], ":9090", sinkDir)
							return
						}
					}
					fmt.Fprintln(os.Stderr, "Usage: dino sinks apply -f <file.yaml>")
					return
				case "rollback":
					cli.RollbackSinkCLI(":9090", sinkDir)
					return
				case "list":
					cli.ListSinksCLI(":9090", sinkDir)
					return
				case "get":
					if len(os.Args) >= 4 {
						cli.GetSinkCLI(os.Args[3], ":9090", sinkDir)
						return
					}
					fmt.Fprintln(os.Stderr, "Usage: dino sinks get <name>")
					return
				}
			}
			cli.ListSinksCLI(":9090", sinkDir)
			return

		case "packs", "-packs", "--packs":
			pDir, uiAddr, local := "packs", ":9090", false
			for i, a := range os.Args {
				if (a == "-packs-dir" || a == "--packs-dir") && i+1 < len(os.Args) {
					pDir = os.Args[i+1]
				}
				if a == "--admin-token" && i+1 < len(os.Args) {
					cli.SetAdminToken(os.Args[i+1])
				}
				if a == "--ui" && i+1 < len(os.Args) {
					uiAddr = os.Args[i+1]
				}
				if a == "--local" {
					local = true
				}
			}
			os.Args = append([]string{os.Args[0], os.Args[1]}, stripCommandOptions(os.Args[2:], true)...)
			if len(os.Args) >= 3 && !strings.HasPrefix(os.Args[2], "-") {
				sub := os.Args[2]
				switch sub {
				case "validate":
					for i, a := range os.Args {
						if (a == "-f" || a == "--f") && i+1 < len(os.Args) {
							cli.ValidatePackCLI(os.Args[i+1])
							return
						}
					}
					fmt.Fprintln(os.Stderr, "Usage: dino packs validate -f <file.yaml>")
					return
				case "test":
					var file, sample string
					for i, a := range os.Args {
						if (a == "-f" || a == "--f") && i+1 < len(os.Args) {
							file = os.Args[i+1]
						}
						if (a == "-sample" || a == "--sample") && i+1 < len(os.Args) {
							sample = os.Args[i+1]
						}
					}
					if file != "" {
						cli.TestPackCLI(file, sample)
						return
					}
					fmt.Fprintln(os.Stderr, "Usage: dino packs test -f <file.yaml> [-sample <sample_log_or_file>]")
					return
				case "apply":
					for i, a := range os.Args {
						if (a == "-f" || a == "--f") && i+1 < len(os.Args) {
							if local {
								cli.ApplyPackLocalCLI(os.Args[i+1], pDir)
							} else {
								cli.ApplyPackCLI(os.Args[i+1], uiAddr, pDir)
							}
							return
						}
					}
					fmt.Fprintln(os.Stderr, "Usage: dino packs apply -f <file.yaml> [--local]")
					return
				case "install", "fetch", "upgrade", "outdated", "remove":
					fmt.Fprintf(os.Stderr, "unsupported command: dino packs %s was removed\n", sub)
					os.Exit(2)
				case "rollback":
					cli.RollbackPackCLI(uiAddr, pDir, local)
					return
				case "list":
					cli.ListPacksCLI(uiAddr, pDir)
					return
				case "get":
					if len(os.Args) >= 4 {
						cli.GetPackCLI(os.Args[3], uiAddr, pDir)
						return
					}
					fmt.Fprintln(os.Stderr, "Usage: dino packs get <name>")
					return
				}
			}
			for i, a := range os.Args {
				if (a == "-f" || a == "--f") && i+1 < len(os.Args) {
					cli.ApplyPackCLI(os.Args[i+1], ":9090", pDir)
					return
				}
			}
			cli.ListPacksCLI(":9090", pDir)
			return
		case "replay", "-replay", "--replay":
			if len(os.Args) >= 3 {
				target := os.Args[2]
				if target == "all" || target == "--all" {
					cli.ReplayAllCLI(":9090")
					return
				}
				cli.ReplaySingleCLI(target, ":9090")
				return
			}
			cli.ListQuarantineCLI(":9090", "data/deinopis.db")
			return
		case "replay-all", "-replay-all", "--replay-all":
			cli.ReplayAllCLI(":9090")
			return
		}
	}

	dbPath := flag.String("db", "data/deinopis.db", "Path to SQLite raw store database")
	packsDir := flag.String("packs-dir", "packs", "Path to YAML parser packs directory")
	sourcesDir := flag.String("sources-dir", "sources", "Path to YAML Ingestion Source resources directory")
	sinksDir := flag.String("sinks-dir", "sinks", "Path to YAML Delivery Sink resources directory")
	workers := flag.Int("workers", 4, "Number of concurrent pipeline workers")
	readStdin := flag.Bool("stdin", false, "Read logs from stdin")
	stdinFraming := flag.String("stdin-framing", "line", "stdin framing: line, ndjson, or document (multiline JSON/XML)")
	verifyRawID := flag.String("verify", "", "Cryptographically verify a raw record by raw_id")
	verifyEventID := flag.String("verify-event", "", "Cryptographically verify a normalized event by event_id")

	// Detached daemon & CLI flags
	detached := flag.Bool("d", false, "Run DEINOPIS engine in detached/daemon mode in the background")
	stopFlag := flag.Bool("stop", false, "Stop running background DEINOPIS engine daemon")
	statusFlag := flag.Bool("status", false, "Check status of background DEINOPIS engine daemon")
	replayFlag := flag.String("replay", "", "Manage quarantine DLQ: list pending logs, or replay with <quarantine_id> or 'all'")
	replayAllFlag := flag.Bool("replay-all", false, "Replay all quarantined DLQ records")

	// Multi-transport ingestion flags
	syslogUDP := flag.String("syslog-udp", "127.0.0.1:1514", "UDP address for Syslog listener (loopback by default; use an explicit public bind for remote senders, 'none' to disable)")
	syslogTCP := flag.String("syslog-tcp", "127.0.0.1:1514", "TCP address for Syslog listener (loopback by default; use an explicit public bind for remote senders, 'none' to disable)")
	syslogTLS := flag.String("syslog-tls", "none", "TCP address for Syslog TLS (RFC 5425) listener (:7514 for non-root, 'none' to disable)")
	tlsCert := flag.String("tls-cert", "", "Path to X.509 certificate file for Syslog TLS")
	tlsKey := flag.String("tls-key", "", "Path to private key file for Syslog TLS")
	tlsClientCA := flag.String("tls-client-ca", "", "Path to optional client CA file for Syslog TLS mutual authentication (mTLS)")
	httpAddr := flag.String("http", "127.0.0.1:8080", "HTTP address for REST ingestion (loopback by default; use an explicit public bind for remote senders, 'none' to disable)")
	httpKey := flag.String("http-key", "", "Optional API key for HTTP POST authentication")
	inboxDir := flag.String("inbox", "data/inbox", "Path to monitored spool inbox directory ('none' to disable)")
	inputFile := flag.String("file", "", "One-shot batch file to ingest immediately")

	// Output sink flags
	outputFile := flag.String("output-file", "data/output/normalized.ndjson", "Path to write normalized NDJSON output ('none' to disable)")
	stdoutOutput := flag.Bool("stdout", true, "Emit normalized NDJSON events to stdout")

	// Management UI and Control Plane flag
	uiAddr := flag.String("ui", "127.0.0.1:9090", "Address for Management Web UI and Control Plane API (loopback by default; use TLS termination before public exposure, 'none' to disable)")
	adminToken := flag.String("admin-token", "", "Optional secret token for authenticating control-plane mutation API routes")
	flag.Parse()

	// Handle standalone actions parsed via flags
	if *stopFlag {
		cli.StopDaemon(cli.DefaultPIDFile)
		return
	}
	if *statusFlag {
		cli.StatusDaemon(cli.DefaultPIDFile, *uiAddr)
		return
	}
	if *replayAllFlag {
		cli.ReplayAllCLI(*uiAddr)
		return
	}
	if *replayFlag != "" {
		if *replayFlag == "all" {
			cli.ReplayAllCLI(*uiAddr)
			return
		}
		if *replayFlag == "list" {
			cli.ListQuarantineCLI(*uiAddr, *dbPath)
			return
		}
		cli.ReplaySingleCLI(*replayFlag, *uiAddr)
		return
	}

	// Detached background mode
	if *detached {
		cli.RunDaemonMode(cli.DefaultPIDFile, cli.DefaultLogFile, *uiAddr, *packsDir)
		return
	}

	// Ensure directory exists for database
	if dir := filepath.Dir(*dbPath); dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0755)
	}

	store, err := rawstore.New(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Failed to initialize raw store at %s: %v\n", *dbPath, err)
		os.Exit(1)
	}
	defer store.Close()

	// Verification mode
	if *verifyRawID != "" {
		ctx := context.Background()
		raw, err := store.Retrieve(ctx, *verifyRawID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Verification failed: %v\n", err)
			os.Exit(1)
		}
		matches, computed, err := store.Verify(ctx, *verifyRawID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Verification calculation failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("\n--- Cryptographic Integrity Report ---\n")
		fmt.Printf("Raw ID:       %s\n", raw.RawID)
		fmt.Printf("Received:     %s\n", raw.ReceivedAt.Format(time.RFC3339Nano))
		fmt.Printf("Transport:    %s (%s)\n", raw.Transport, raw.SourceIP)
		fmt.Printf("Stored Hash:  %s\n", raw.RawSHA256)
		fmt.Printf("Actual Hash:  %s\n", computed)
		fmt.Printf("Integrity:    ")
		if matches {
			fmt.Println("PASSED (Cryptographically Exact)")
		} else {
			fmt.Println("FAILED (TAMPER DETECTED)")
		}
		fmt.Printf("Byte Count:   %d bytes\n", raw.ByteCount)
		fmt.Printf("Payload Preview:\n%s\n", string(raw.Payload))
		fmt.Printf("--------------------------------------\n")
		return
	}

	// Normalized event verification mode
	if *verifyEventID != "" {
		ctx := context.Background()
		report, err := store.VerifyNormalized(ctx, *verifyEventID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Event verification failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("\n--- Cryptographic Normalized Event Integrity Report ---\n")
		fmt.Printf("Event ID:     %s\n", report.EventID)
		fmt.Printf("Raw ID:       %s\n", report.RawID)
		fmt.Printf("Stored Hash:  %s\n", report.StoredHash)
		fmt.Printf("Actual Hash:  %s\n", report.ComputedHash)
		fmt.Printf("Message:      %s\n", report.StoredMessage)
		fmt.Printf("Raw Source:   ")
		if report.RawValid {
			fmt.Println("PASSED (Cryptographically Exact)")
		} else {
			fmt.Println("FAILED (RAW LOG TAMPERED)")
		}
		fmt.Printf("Integrity:    ")
		if report.Passed {
			fmt.Println("PASSED (Cryptographically Exact)")
		} else {
			fmt.Printf("FAILED (TAMPER DETECTED: %s)\n", report.FailureReason)
		}
		fmt.Printf("-------------------------------------------------------\n")
		return
	}

	// The window sealer runs even without a witness; uploading is enabled only
	// when the complete mutually authenticated witness configuration is present.
	vaultClient, streamID, err := configureVaultWitness(store)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: vault witness configuration: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "========================================================\n")
	fmt.Fprintf(os.Stderr, " DEINOPIS: Universal Log Pre-processing Framework (SIH 26156)\n")

	// 1. Load Parser Packs
	var packManager *packs.SnapshotManager
	if *packsDir != "" {
		if _, err := os.Stat(*packsDir); err == nil {
			packLock, err := packs.AcquireDirectoryLock(*packsDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
				os.Exit(1)
			}
			defer packLock.Close()
			if err := packs.RecoverTransactions(*packsDir); err != nil {
				fmt.Fprintf(os.Stderr, "FATAL: Failed to recover parser pack transaction: %v\n", err)
				os.Exit(1)
			}
			snap, err := packs.LoadDir(*packsDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "WARNING: Failed to load parser packs from %s: %v\n", *packsDir, err)
			} else {
				packManager = packs.NewSnapshotManager(snap)
				fmt.Fprintf(os.Stderr, " Loaded %d parser packs from %s\n", len(snap.ListPacks()), *packsDir)
			}
		}
	}

	// 2. Configure the atomically replaceable output snapshot.
	staticSinks := map[string]output.OutputSink{}
	if *stdoutOutput {
		staticSinks["cli-stdout"] = output.NewStdoutSink()
	}
	if *outputFile != "" && *outputFile != "none" {
		fileSink, err := output.NewNDJSONSink(*outputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: Failed to create NDJSON file sink at %s: %v\n", *outputFile, err)
		} else {
			staticSinks["cli-ndjson"] = fileSink
			fmt.Fprintf(os.Stderr, " Output sink active: %s (NDJSON)\n", *outputFile)
		}
	}
	dynamicSink := output.NewDynamicSink()
	if err := dynamicSink.Replace(staticSinks); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: output initialization failed: %v\n", err)
		os.Exit(1)
	}
	var compositeSink output.OutputSink = dynamicSink

	// 3. Initialize & Start Pipeline
	p := pipeline.New(pipeline.Config{
		Workers:     *workers,
		BufferSize:  2000,
		PackManager: packManager,
	}, store, compositeSink)
	p.Start()

	// 4. Initialize Ingestion Adapters
	mgr := ingest.NewManager(2000)
	var staticAdapters []ingest.IngestAdapter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := store.RunVaultSealer(ctx, time.Second); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "ERROR: vault window sealer stopped: %v\n", err)
		}
	}()
	if vaultClient != nil {
		go func() {
			if err := store.RunVaultUploader(ctx, vaultClient, time.Second); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "ERROR: vault uploader stopped: %v\n", err)
			}
		}()
		go runVaultHeartbeats(ctx, store, vaultClient, streamID)
	}

	if *syslogUDP != "" && *syslogUDP != "none" {
		staticAdapters = append(staticAdapters, ingest.NewSyslogUDPListener(*syslogUDP))
		fmt.Fprintf(os.Stderr, " Ingest adapter configured: Syslog UDP on %s\n", *syslogUDP)
	}
	if *syslogTCP != "" && *syslogTCP != "none" {
		staticAdapters = append(staticAdapters, ingest.NewSyslogTCPListener(*syslogTCP))
		fmt.Fprintf(os.Stderr, " Ingest adapter configured: Syslog TCP on %s\n", *syslogTCP)
	}
	if *syslogTLS != "" && *syslogTLS != "none" {
		if *tlsCert == "" || *tlsKey == "" {
			fmt.Fprintln(os.Stderr, "FATAL: -syslog-tls requires -tls-cert and -tls-key")
			os.Exit(1)
		}
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: failed to load Syslog TLS cert/key: %v\n", err)
			os.Exit(1)
		}
		tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		if *tlsClientCA != "" {
			caCert, err := os.ReadFile(*tlsClientCA)
			if err != nil {
				fmt.Fprintf(os.Stderr, "FATAL: failed to read Syslog TLS client CA: %v\n", err)
				os.Exit(1)
			}
			caPool := x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(caCert) {
				fmt.Fprintf(os.Stderr, "FATAL: Syslog TLS client CA contains no valid certificates: %s\n", *tlsClientCA)
				os.Exit(1)
			}
			tlsCfg.ClientCAs = caPool
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
		staticAdapters = append(staticAdapters, ingest.NewSyslogTLSListener(*syslogTLS, tlsCfg))
		fmt.Fprintf(os.Stderr, " Ingest adapter configured: Syslog TLS (RFC 5425) on %s\n", *syslogTLS)
	}
	if *httpAddr != "" && *httpAddr != "none" {
		staticAdapters = append(staticAdapters, ingest.NewHTTPListener(*httpAddr, *httpKey))
		fmt.Fprintf(os.Stderr, " Ingest adapter configured: HTTP POST on %s/api/v1/ingest\n", *httpAddr)
	}
	if *inboxDir != "" && *inboxDir != "none" {
		staticAdapters = append(staticAdapters, ingest.NewFileWatcher(*inboxDir, 1*time.Second))
		fmt.Fprintf(os.Stderr, " Ingest adapter configured: Spool Inbox on %s\n", *inboxDir)
	}

	connMgr := connections.NewManager(*sinksDir)
	connMgr.SetResourceDirs(*sourcesDir, *sinksDir)
	if *sourcesDir != "" {
		if err := connMgr.LoadDir(*sourcesDir); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: source configuration: %v\n", err)
			os.Exit(1)
		}
	}
	if *sinksDir != "" {
		if err := connMgr.LoadDir(*sinksDir); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: sink configuration: %v\n", err)
			os.Exit(1)
		}
	}
	runtimeReconciler := &connections.Runtime{Ingest: mgr, Output: dynamicSink, StaticAdapters: staticAdapters, StaticSinks: staticSinks, Secrets: connections.SecretResolver{Dir: "secrets"}}
	if err := runtimeReconciler.Reconcile(connMgr.List()); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: runtime reconcile: %v\n", err)
		os.Exit(1)
	}
	connMgr.SetReconciler(runtimeReconciler.Reconcile)
	if err := mgr.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Ingest manager start error: %v\n", err)
		os.Exit(1)
	}
	p.ConnectIngest(ctx, mgr.Channel())

	// 5. Initialize Management Control Plane & UI Server
	var uiServer *api.Server
	if *uiAddr != "" && *uiAddr != "none" {
		cfgInfo := api.ConfigInfo{
			UIAddress:  *uiAddr,
			SyslogUDP:  *syslogUDP,
			SyslogTCP:  *syslogTCP,
			SyslogTLS:  *syslogTLS,
			HTTPIngest: *httpAddr,
			InboxDir:   *inboxDir,
			DBPath:     *dbPath,
			Workers:    *workers,
			AdminToken: *adminToken,
		}
		uiServer = api.NewServer(*uiAddr, store, p, packManager, *packsDir, cfgInfo, web.Dist(), Version)
		uiServer.SetIngestTracker(mgr)
		uiServer.SetConnManager(connMgr, *sinksDir)
		if err := uiServer.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: Failed to start Management UI on %s: %v\n", *uiAddr, err)
		} else {
			fmt.Fprintf(os.Stderr, " Management Control Plane & UI active: http://%s\n", *uiAddr)
		}
	}

	hasListeners := (*syslogUDP != "" && *syslogUDP != "none") ||
		(*syslogTCP != "" && *syslogTCP != "none") ||
		(*syslogTLS != "" && *syslogTLS != "none") ||
		(*httpAddr != "" && *httpAddr != "none") ||
		(*inboxDir != "" && *inboxDir != "none")
	for _, configured := range connMgr.List() {
		if configured.Kind == "Source" && configured.Spec.Enabled {
			hasListeners = true
			break
		}
	}

	doneFile := make(chan struct{})
	if *inputFile != "" {
		go func() {
			defer close(doneFile)
			fmt.Fprintf(os.Stderr, " Ingesting one-shot file: %s...\n", *inputFile)
			if err := ingest.IngestFile(ctx, *inputFile, mgr.Inbound()); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: Failed to ingest file %s: %v\n", *inputFile, err)
			} else {
				fmt.Fprintf(os.Stderr, " Finished ingesting file: %s\n", *inputFile)
			}
		}()
	}

	// 6. Check if stdin is requested or is a pipe
	stat, _ := os.Stdin.Stat()
	isPipe := (stat.Mode() & os.ModeCharDevice) == 0

	doneReading := make(chan struct{})
	if *readStdin || isPipe {
		go func() {
			defer close(doneReading)
			submit := func(payload []byte) {
				if len(payload) == 0 {
					return
				}
				rec := model.IngestedRecord{Transport: "stdin", SourceIP: "127.0.0.1", RawBytes: append([]byte(nil), payload...), ReceivedAt: time.Now().UTC(), Metadata: model.ReceiveMetadata{Listener: "stdin"}}
				if err := p.SubmitSync(context.Background(), rec); err != nil {
					fmt.Fprintf(os.Stderr, "WARN: failed to ingest record: %v\n", err)
				}
			}
			switch *stdinFraming {
			case "document":
				payload, err := io.ReadAll(io.LimitReader(os.Stdin, decode.MaxPayloadBytes+1))
				if err != nil {
					fmt.Fprintf(os.Stderr, "WARN: stdin read error: %v\n", err)
					return
				}
				if len(payload) > decode.MaxPayloadBytes {
					fmt.Fprintf(os.Stderr, "WARN: stdin document exceeds %d bytes\n", decode.MaxPayloadBytes)
					return
				}
				submit(payload)
			case "line", "ndjson":
				scanner := bufio.NewScanner(os.Stdin)
				scanner.Buffer(make([]byte, 64*1024), decode.MaxPayloadBytes)
				for scanner.Scan() {
					if len(scanner.Bytes()) > 0 {
						submit(scanner.Bytes())
					}
				}
				if err := scanner.Err(); err != nil && err != io.EOF {
					fmt.Fprintf(os.Stderr, "WARN: stdin scanner error: %v\n", err)
				}
			default:
				fmt.Fprintf(os.Stderr, "WARN: invalid -stdin-framing %q (expected line, ndjson, document)\n", *stdinFraming)
			}
		}()
	}

	fmt.Fprintf(os.Stderr, "========================================================\n")
	fmt.Fprintf(os.Stderr, " Pipeline running with %d workers. Awaiting events...\n", *workers)

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	if !hasListeners && *inputFile != "" {
		select {
		case <-sigChan:
			fmt.Fprintf(os.Stderr, "\nReceived shutdown signal. Stopping pipeline...\n")
		case <-doneFile:
			// Allow ConnectIngest to transfer records from mgr.Channel into pipeline
			time.Sleep(50 * time.Millisecond)
			for i := 0; i < 100; i++ {
				if p.Stats().Pending == 0 {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			fmt.Fprintf(os.Stderr, "Finished processing file. Stopping pipeline...\n")
		}
	} else if isPipe || *readStdin {
		select {
		case <-sigChan:
			fmt.Fprintf(os.Stderr, "\nReceived shutdown signal. Stopping pipeline...\n")
		case <-doneReading:
			fmt.Fprintf(os.Stderr, "Finished reading from stdin. Draining pipeline...\n")
		}
	} else {
		<-sigChan
		fmt.Fprintf(os.Stderr, "\nReceived shutdown signal. Stopping pipeline...\n")
	}

	// Graceful shutdown sequence
	if uiServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = uiServer.Stop(shutdownCtx)
		shutdownCancel()
	}
	cancel()
	_ = mgr.Stop()
	p.Stop()
	_ = compositeSink.Flush()
	_ = compositeSink.Close()

	if os.Getenv("DEINOPIS_DAEMON") == "1" {
		_ = os.Remove(cli.DefaultPIDFile)
	}

	stats := p.Stats()
	valid, reason := stats.VerifyInvariant()

	fmt.Fprintf(os.Stderr, "\n--- Final Pipeline Loss Accounting ---\n")
	fmt.Fprintf(os.Stderr, "Accepted:    %d\n", stats.Accepted)
	fmt.Fprintf(os.Stderr, "Normalized:  %d\n", stats.Normalized)
	fmt.Fprintf(os.Stderr, "Quarantined: %d\n", stats.Quarantined)
	fmt.Fprintf(os.Stderr, "Pending:     %d\n", stats.Pending)
	fmt.Fprintf(os.Stderr, "Delivered:   %d\n", stats.Delivered)
	fmt.Fprintf(os.Stderr, "Loss Audit:  %s\n", reason)
	fmt.Fprintf(os.Stderr, "--------------------------------------\n")

	if !valid {
		os.Exit(2)
	}
}

func stripCommandOptions(args []string, stripJSON bool) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--packs-dir", "-packs-dir", "--ui", "--admin-token":
			i++
		case "--local":
		case "--json":
			if !stripJSON {
				out = append(out, args[i])
			}
		default:
			out = append(out, args[i])
		}
	}
	return out
}

func configureVaultWitness(store *rawstore.RawStore) (*witness.Client, string, error) {
	keys := []string{"DEINOPIS_VAULT_STREAM_ID", "DEINOPIS_VAULT_EPOCH", "DEINOPIS_WITNESS_URL", "DEINOPIS_WITNESS_TOKEN", "DEINOPIS_WITNESS_CA", "DEINOPIS_WITNESS_CLIENT_CERT", "DEINOPIS_WITNESS_CLIENT_KEY"}
	values := make(map[string]string, len(keys))
	configured := 0
	for _, key := range keys {
		values[key] = os.Getenv(key)
		if values[key] != "" {
			configured++
		}
	}
	if configured == 0 {
		return nil, "", nil
	}
	if configured != len(keys) {
		return nil, "", fmt.Errorf("vault witness requires all seven DEINOPIS_VAULT_* and DEINOPIS_WITNESS_* settings")
	}
	epoch, err := time.Parse(time.RFC3339, values["DEINOPIS_VAULT_EPOCH"])
	if err != nil {
		return nil, "", fmt.Errorf("invalid DEINOPIS_VAULT_EPOCH: %w", err)
	}
	if _, err = hex.DecodeString(values["DEINOPIS_VAULT_STREAM_ID"]); err != nil || len(values["DEINOPIS_VAULT_STREAM_ID"]) != 64 || strings.ToLower(values["DEINOPIS_VAULT_STREAM_ID"]) != values["DEINOPIS_VAULT_STREAM_ID"] {
		return nil, "", errors.New("DEINOPIS_VAULT_STREAM_ID must be 64 lowercase hexadecimal characters")
	}
	cert, err := tls.LoadX509KeyPair(values["DEINOPIS_WITNESS_CLIENT_CERT"], values["DEINOPIS_WITNESS_CLIENT_KEY"])
	if err != nil {
		return nil, "", fmt.Errorf("load witness client certificate: %w", err)
	}
	caPEM, err := os.ReadFile(values["DEINOPIS_WITNESS_CA"])
	if err != nil {
		return nil, "", fmt.Errorf("read witness CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, "", errors.New("witness CA file contains no certificates")
	}
	client, err := witness.NewClient(values["DEINOPIS_WITNESS_URL"], values["DEINOPIS_WITNESS_TOKEN"], cert, roots)
	if err != nil {
		return nil, "", err
	}
	state, err := store.VaultState(context.Background())
	if err != nil {
		return nil, "", err
	}
	if state.StreamID != values["DEINOPIS_VAULT_STREAM_ID"] || !state.EpochStart.Equal(epoch.UTC()) {
		if err = store.ConfigureVaultStream(context.Background(), values["DEINOPIS_VAULT_STREAM_ID"], epoch.UTC()); err != nil {
			return nil, "", fmt.Errorf("configure enrolled stream (existing evidence is never rewritten): %w", err)
		}
	}
	return client, values["DEINOPIS_VAULT_STREAM_ID"], nil
}

func runVaultHeartbeats(ctx context.Context, store *rawstore.RawStore, client *witness.Client, streamID string) {
	bootID := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UTC().UnixNano())
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	var counter uint64
	for {
		counter++
		queueDepth, err := store.PendingVaultUploads(ctx)
		if err == nil {
			err = client.SendHeartbeat(ctx, streamID, bootID, counter, queueDepth)
		}
		if err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "WARN: witness heartbeat failed: %v\n", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
