package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"deinopis/internal/logsim"
)

func main() {
	sourceFlag := flag.String("source", "all", "Source: firewall|asa|sshd|ids|webserver|app|database|all|mixed")
	scenarioFlag := flag.String("scenario", "all", "Scenario: normal|bruteforce|scan|malformed|mixed|all")
	formatFlag := flag.String("format", "stdout", "Output format: stdout|syslog-udp|syslog-tcp|syslog-tls|http-post|file")
	targetFlag := flag.String("target", "127.0.0.1:514", "Network target address or URL (e.g. 127.0.0.1:514 or http://127.0.0.1:8080/api/v1/ingest)")
	outputDir := flag.String("output-dir", "simulated", "Output directory when format=file")
	rateFlag := flag.Float64("rate", 10.0, "Events per second (0 = burst as fast as possible)")
	countFlag := flag.Int("count", 100, "Total events to generate (0 = infinite loop)")
	seedFlag := flag.Int64("seed", 42, "Random seed (0 = time-based)")

	flag.Parse()

	var rng *rand.Rand
	if *seedFlag == 0 {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	} else {
		rng = rand.New(rand.NewSource(*seedFlag))
	}

	allSimulators := []logsim.SourceSimulator{
		&logsim.FirewallSimulator{},
		&logsim.ASASimulator{},
		&logsim.SSHDSimulator{},
		&logsim.IDSSimulator{},
		&logsim.WebServerSimulator{},
		&logsim.AppSimulator{},
		&logsim.DatabaseSimulator{},
	}

	var activeSimulators []logsim.SourceSimulator
	if *sourceFlag == "all" || *sourceFlag == "mixed" {
		activeSimulators = allSimulators
	} else {
		for _, s := range allSimulators {
			if s.Name() == *sourceFlag {
				activeSimulators = append(activeSimulators, s)
			}
		}
	}

	if len(activeSimulators) == 0 {
		fmt.Fprintf(os.Stderr, "Unknown source: %s\n", *sourceFlag)
		os.Exit(1)
	}

	scenarios := []string{"normal", "bruteforce", "scan", "mixed", "malformed"}
	if *scenarioFlag != "all" && *scenarioFlag != "mixed" {
		scenarios = []string{*scenarioFlag}
	}

	// Prepare network sockets if applicable
	var udpConn net.Conn
	var tcpConn net.Conn
	var err error

	if *formatFlag == "syslog-udp" {
		udpConn, err = net.Dial("udp", *targetFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to UDP target %s: %v\n", *targetFlag, err)
			os.Exit(1)
		}
		defer udpConn.Close()
	} else if *formatFlag == "syslog-tcp" {
		tcpConn, err = net.Dial("tcp", *targetFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to TCP target %s: %v\n", *targetFlag, err)
			os.Exit(1)
		}
		defer tcpConn.Close()
	} else if *formatFlag == "syslog-tls" {
		tlsCfg := &tls.Config{InsecureSkipVerify: true}
		tlsConn, err := tls.Dial("tcp", *targetFlag, tlsCfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to TLS target %s: %v\n", *targetFlag, err)
			os.Exit(1)
		}
		defer tlsConn.Close()
		tcpConn = tlsConn
	} else if *formatFlag == "file" {
		_ = os.MkdirAll(*outputDir, 0755)
	}

	fmt.Fprintf(os.Stderr, "Starting logsim: sources=%s scenario=%s format=%s count=%d rate=%.1f eps\n",
		*sourceFlag, *scenarioFlag, *formatFlag, *countFlag, *rateFlag)

	var delay time.Duration
	if *rateFlag > 0 {
		delay = time.Duration(float64(time.Second) / *rateFlag)
	}

	var emitted, acked, errCount int
	for {
		if *countFlag > 0 && emitted >= *countFlag {
			break
		}

		sim := activeSimulators[rng.Intn(len(activeSimulators))]
		sc := scenarios[rng.Intn(len(scenarios))]
		if *scenarioFlag == "mixed" {
			// Mixed scenario: 70% normal, 15% bruteforce, 10% scan, 5% malformed
			roll := rng.Float32()
			if roll < 0.70 {
				sc = "normal"
			} else if roll < 0.85 {
				sc = "bruteforce"
			} else if roll < 0.95 {
				sc = "scan"
			} else {
				sc = "malformed"
			}
		}

		payload := sim.Generate(sc, time.Now().UTC(), rng)

		var sendErr error
		switch *formatFlag {
		case "stdout":
			_, sendErr = fmt.Println(string(payload))
		case "syslog-udp":
			_, sendErr = udpConn.Write(payload)
		case "syslog-tcp", "syslog-tls":
			_, sendErr = fmt.Fprintf(tcpConn, "%s\n", string(payload))
		case "http-post":
			resp, httpErr := http.Post(*targetFlag, "application/json", bytes.NewReader(payload))
			if httpErr != nil {
				sendErr = httpErr
			} else {
				if resp.StatusCode != http.StatusOK {
					sendErr = fmt.Errorf("http status %d", resp.StatusCode)
				}
				_ = resp.Body.Close()
			}
		case "file":
			if sim.Format() == "csv" {
				fileName := fmt.Sprintf("audit_%s_%d.csv", sim.Name(), time.Now().UnixNano())
				filePath := filepath.Join(*outputDir, fileName)
				sendErr = os.WriteFile(filePath, payload, 0644)
			} else {
				filePath := filepath.Join(*outputDir, fmt.Sprintf("%s.log", sim.Name()))
				f, fErr := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
				if fErr != nil {
					sendErr = fErr
				} else {
					_, sendErr = fmt.Fprintf(f, "%s\n", string(payload))
					_ = f.Close()
				}
			}
		}

		emitted++
		if sendErr != nil {
			errCount++
		} else {
			acked++
		}

		if delay > 0 {
			time.Sleep(delay)
		}
	}

	fmt.Fprintf(os.Stderr, "Finished logsim: emitted=%d acked=%d errors=%d\n", emitted, acked, errCount)
}
