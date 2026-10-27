package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"deinopis/internal/witness"
)

func main() {
	var listen, dbPath, manifestDir, streamID, certFingerprint, caPath, certPath, keyPath string
	var statusOnly bool
	flag.StringVar(&listen, "listen", ":9443", "mTLS listener address")
	flag.StringVar(&dbPath, "db", "data/witness.db", "private witness SQLite database")
	flag.StringVar(&manifestDir, "manifest-dir", "data/manifests", "private original manifest directory")
	flag.StringVar(&streamID, "stream-id", os.Getenv("DEINOPIS_WITNESS_STREAM_ID"), "enrolled 32-byte stream ID")
	flag.StringVar(&certFingerprint, "collector-cert-sha256", os.Getenv("DEINOPIS_COLLECTOR_CERT_SHA256"), "enrolled collector client certificate SHA-256")
	flag.StringVar(&caPath, "client-ca", os.Getenv("DEINOPIS_WITNESS_CLIENT_CA"), "CA PEM for collector mTLS certificates")
	flag.StringVar(&certPath, "tls-cert", os.Getenv("DEINOPIS_WITNESS_TLS_CERT"), "witness HTTPS certificate PEM")
	flag.StringVar(&keyPath, "tls-key", os.Getenv("DEINOPIS_WITNESS_TLS_KEY"), "witness HTTPS private key PEM")
	flag.BoolVar(&statusOnly, "status", false, "print preserved publication history and queue state, then exit")
	flag.Parse()
	token := os.Getenv("DEINOPIS_WITNESS_TOKEN")
	store, err := openWitnessStore(os.Environ(), dbPath, manifestDir)
	if err != nil {
		log.Fatalf("open witness store: %v", err)
	}
	defer store.Close()
	if statusOnly {
		status, statusErr := store.PublicationQueueStatus(context.Background())
		if statusErr != nil {
			log.Fatalf("read publication queue status: %v", statusErr)
		}
		if err = json.NewEncoder(os.Stdout).Encode(status); err != nil {
			log.Fatalf("write publication queue status: %v", err)
		}
		return
	}
	if streamID == "" || certFingerprint == "" || caPath == "" || certPath == "" || keyPath == "" || token == "" {
		log.Fatal("v2 witness requires stream, collector fingerprint, TLS files, and DEINOPIS_WITNESS_TOKEN")
	}
	if err = store.EnrollBatch(context.Background(), streamID, certFingerprint, token); err != nil {
		log.Fatalf("enroll witness batch stream: %v", err)
	}
	log.Print("external chain publishing is disabled; existing publication history is preserved")
	caBytes, err := os.ReadFile(caPath)
	if err != nil {
		log.Fatalf("read client CA: %v", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caBytes) {
		log.Fatal("client CA file contains no certificates")
	}
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		log.Fatalf("load witness TLS certificate: %v", err)
	}
	server := &http.Server{Addr: listen, Handler: witness.NewHandler(store), TLSConfig: witness.ServerTLSConfig(clientCAs), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 70 * time.Second, WriteTimeout: 70 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	server.TLSConfig.Certificates = []tls.Certificate{certificate}
	fmt.Printf("DEINOPIS witness listening with mTLS on %s\n", listen)
	if err = server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
		log.Fatalf("witness server: %v", err)
	}
}

func openWitnessStore(environ []string, dbPath, manifestDir string) (*witness.Store, error) {
	if err := rejectExternalPublisherEnvironment(environ); err != nil {
		return nil, err
	}
	return witness.NewStore(dbPath, manifestDir)
}

func rejectExternalPublisherEnvironment(environ []string) error {
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "DEINOPIS_CHAIN_") || strings.HasPrefix(name, "DEINOPIS_REGISTRY_") || name == "DEINOPIS_PUBLISHER_KEY_FILE" {
			return fmt.Errorf("external chain publishing is disabled; remove retired setting %s (no database or network action was taken)", name)
		}
	}
	return nil
}
