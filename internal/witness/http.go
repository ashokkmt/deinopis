package witness

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func ServerTLSConfig(clientCAs *x509.CertPool) *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs}
}

func NewHandler(store *Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /internal/v1/streams/{stream}/windows/{slot}", store.putManifest)
	mux.HandleFunc("POST /internal/v1/streams/{stream}/heartbeat", store.postHeartbeat)
	return mux
}

func (s *Store) putManifest(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/octet-stream" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported media type")
		return
	}
	identity, token, ok := requestIdentity(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "mTLS identity and bearer token required")
		return
	}
	slot, err := parseUint(r.PathValue("slot"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid slot")
		return
	}
	body, err := readBounded(r.Body, manifestLimit)
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "manifest too large")
		return
	}
	receipt, err := s.AcceptManifest(r.Context(), r.PathValue("stream"), slot, r.Header.Get("X-DEINOPIS-Manifest-SHA256"), identity, token, body, time.Now().UTC())
	if err != nil {
		switch {
		case errors.Is(err, ErrUnauthorized), errors.Is(err, ErrNotEnrolled):
			writeError(w, http.StatusUnauthorized, "collector is not authorized for this stream")
		case errors.Is(err, ErrConflict):
			writeError(w, http.StatusConflict, "original manifest conflict recorded")
		case strings.Contains(err.Error(), "future or open"):
			writeError(w, http.StatusUnprocessableEntity, "window is not closed")
		default:
			writeError(w, http.StatusUnprocessableEntity, "manifest rejected")
		}
		return
	}
	status := http.StatusCreated
	if receipt.Existing {
		status = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"stream_id": receipt.StreamID, "slot": fmt.Sprint(receipt.Slot), "manifest_digest": hex.EncodeToString(receipt.Digest[:]), "commitment": hex.EncodeToString(receipt.Commitment[:]), "received_at": receipt.ReceivedAt.Format(time.RFC3339Nano), "existing": receipt.Existing})
}

func (s *Store) postHeartbeat(w http.ResponseWriter, r *http.Request) {
	identity, token, ok := requestIdentity(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "mTLS identity and bearer token required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body Heartbeat
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid heartbeat")
		return
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid heartbeat")
		return
	}
	if err := s.RecordHeartbeat(r.Context(), r.PathValue("stream"), identity, token, body, time.Now().UTC()); err != nil {
		if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrNotEnrolled) {
			writeError(w, http.StatusUnauthorized, "collector is not authorized for this stream")
			return
		}
		writeError(w, http.StatusUnprocessableEntity, "heartbeat rejected")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func requestIdentity(r *http.Request) (fingerprint, token string, ok bool) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 || len(r.TLS.VerifiedChains) == 0 {
		return "", "", false
	}
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", "", false
	}
	sum := sha256.Sum256(r.TLS.PeerCertificates[0].Raw)
	return hex.EncodeToString(sum[:]), parts[1], true
}

func parseUint(v string) (uint64, error) {
	if v == "" || len(v) > 20 {
		return 0, fmt.Errorf("invalid uint64")
	}
	var n uint64
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid uint64")
		}
		d := uint64(c - '0')
		if n > (^uint64(0)-d)/10 {
			return 0, fmt.Errorf("uint64 overflow")
		}
		n = n*10 + d
	}
	return n, nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "WITNESS_REQUEST_REJECTED", "message": message}})
}
