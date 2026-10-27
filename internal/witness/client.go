package witness

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"deinopis/internal/integrity"
)

type Client struct {
	base  *url.URL
	http  *http.Client
	token string
}

func (c *Client) SendHeartbeat(ctx context.Context, streamID, bootID string, counter uint64, queueDepth uint32) error {
	if !validDigestText(streamID) || bootID == "" || len(bootID) > 128 {
		return errors.New("invalid witness heartbeat identity")
	}
	body, err := json.Marshal(Heartbeat{BootID: bootID, BootCounter: counter, QueueDepth: queueDepth})
	if err != nil {
		return err
	}
	u := *c.base
	u.Path = c.base.Path + "/internal/v1/streams/" + streamID + "/heartbeat"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.New("witness heartbeat request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("witness rejected heartbeat with HTTP %d", resp.StatusCode)
	}
	return nil
}

func NewClient(baseURL, token string, certificate tls.Certificate, roots *x509.CertPool) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("witness URL must be an HTTPS origin without credentials, query, or fragment")
	}
	if token == "" || len(certificate.Certificate) == 0 {
		return nil, errors.New("witness token and client certificate are required")
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{certificate}}}
	return &Client{base: u, http: &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, token: token}, nil
}

func (c *Client) UploadManifest(ctx context.Context, streamID string, slot uint64, digest integrity.Digest, body []byte) (string, error) {
	if len(body) == 0 || len(body) > 64*1024*1024 || !validDigestText(streamID) || integrity.ManifestDigest(body) != digest {
		return "", errors.New("invalid manifest upload payload")
	}
	u := *c.base
	u.Path = c.base.Path + fmt.Sprintf("/internal/v1/streams/%s/windows/%d", streamID, slot)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-DEINOPIS-Manifest-SHA256", hex.EncodeToString(digest[:]))
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", errors.New("witness upload request failed")
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(payload) > 1<<20 {
		return "", errors.New("invalid witness response")
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("witness rejected upload with HTTP %d", resp.StatusCode)
	}
	var receipt struct {
		StreamID       string `json:"stream_id"`
		Slot           string `json:"slot"`
		ManifestDigest string `json:"manifest_digest"`
		Commitment     string `json:"commitment"`
		ReceivedAt     string `json:"received_at"`
		Existing       bool   `json:"existing"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&receipt); err != nil {
		return "", errors.New("invalid witness receipt")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return "", errors.New("invalid witness receipt")
	}
	if receipt.StreamID != streamID || receipt.Slot != fmt.Sprint(slot) || receipt.ManifestDigest != hex.EncodeToString(digest[:]) || !validDigestText(receipt.Commitment) {
		return "", errors.New("witness receipt does not match upload")
	}
	if _, err = time.Parse(time.RFC3339Nano, receipt.ReceivedAt); err != nil {
		return "", errors.New("witness receipt has invalid timestamp")
	}
	encoded, err := json.Marshal(receipt)
	return string(encoded), err
}
