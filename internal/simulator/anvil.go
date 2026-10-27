package simulator

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"deinopis/internal/chain/registry"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

const startupTimeout = 10 * time.Second

// Runner owns one fresh, loopback-only Anvil process and its disposable run directory.
type Runner struct {
	RunDir       string
	RPCURL       string
	ChainID      *big.Int
	Registry     common.Address
	RegistryHash common.Hash
	PID          int

	client *ethclient.Client
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

// Start launches an empty Anvil chain, deploys the real registry binding and verifies its empty head.
func Start(ctx context.Context) (*Runner, error) {
	path, err := findAnvil()
	if err != nil {
		return nil, err
	}
	runDir, err := os.MkdirTemp("", "deinopis-sim-")
	if err != nil {
		return nil, fmt.Errorf("create simulator run directory: %w", err)
	}
	if err = os.Chmod(runDir, 0700); err != nil {
		_ = os.RemoveAll(runDir)
		return nil, fmt.Errorf("restrict simulator run directory: %w", err)
	}
	port, err := reserveLoopbackPort()
	if err != nil {
		_ = os.RemoveAll(runDir)
		return nil, err
	}
	chainID, err := randomChainID()
	if err != nil {
		_ = os.RemoveAll(runDir)
		return nil, err
	}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	if err = validateLoopbackEndpoint(endpoint); err != nil {
		_ = os.RemoveAll(runDir)
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(runCtx, path,
		"--host", "127.0.0.1", "--port", fmt.Sprint(port),
		"--chain-id", chainID.String(), "--accounts", "0", "--mnemonic-random", "12")
	cmd.Dir = runDir
	cmd.Env = []string{"HOME=" + runDir, "TMPDIR=" + runDir, "PATH=" + os.Getenv("PATH")}
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err = cmd.Start(); err != nil {
		cancel()
		_ = os.RemoveAll(runDir)
		return nil, fmt.Errorf("start local Anvil process: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	runner := &Runner{RunDir: runDir, RPCURL: endpoint, ChainID: chainID, PID: cmd.Process.Pid, cancel: cancel, done: done}
	client, err := waitForAnvil(runCtx, endpoint, done)
	if err != nil {
		runner.Stop()
		return nil, err
	}
	runner.client = client
	if err = runner.deployRegistry(runCtx); err != nil {
		runner.Stop()
		return nil, err
	}
	if err = runner.writeRunRecord(); err != nil {
		runner.Stop()
		return nil, err
	}
	return runner, nil
}

// Stop terminates only the Anvil child process started by this Runner. Run artifacts remain available.
func (r *Runner) Stop() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.client != nil {
			r.client.Close()
		}
		if r.cancel != nil {
			r.cancel()
		}
		if r.done != nil {
			select {
			case <-r.done:
			case <-time.After(3 * time.Second):
			}
		}
	})
}

func (r *Runner) deployRegistry(ctx context.Context) error {
	key, err := crypto.GenerateKey()
	if err != nil {
		return fmt.Errorf("generate ephemeral simulator key: %w", err)
	}
	defer func() { key.D.SetInt64(0) }()
	address := crypto.PubkeyToAddress(key.PublicKey)
	rpcClient, err := rpc.DialContext(ctx, r.RPCURL)
	if err != nil {
		return fmt.Errorf("connect to local simulator RPC: %w", err)
	}
	defer rpcClient.Close()
	var funded any
	balance := new(big.Int).Mul(big.NewInt(1000), big.NewInt(1e18))
	if err = rpcClient.CallContext(ctx, &funded, "anvil_setBalance", address, fmt.Sprintf("0x%x", balance)); err != nil {
		return fmt.Errorf("fund ephemeral simulator account: %w", err)
	}
	client := ethclient.NewClient(rpcClient)
	auth, err := bind.NewKeyedTransactorWithChainID(key, r.ChainID)
	if err != nil {
		return fmt.Errorf("create simulator transaction signer: %w", err)
	}
	auth.Context = ctx
	contractAddress, tx, contract, err := registry.DeployBatchRegistry(auth, client)
	if err != nil {
		return fmt.Errorf("deploy local batch registry: %w", err)
	}
	receipt, err := bind.WaitMined(ctx, client, tx)
	if err != nil {
		return fmt.Errorf("wait for local registry deployment: %w", err)
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return errors.New("local registry deployment reverted")
	}
	code, err := client.CodeAt(ctx, contractAddress, nil)
	if err != nil {
		return fmt.Errorf("read deployed local registry code: %w", err)
	}
	if len(code) == 0 {
		return errors.New("deployed local registry has no runtime code")
	}
	stream := [32]byte{}
	count, err := contract.BatchCount(&bind.CallOpts{Context: ctx}, stream)
	if err != nil {
		return fmt.Errorf("read empty local registry history: %w", err)
	}
	if count != 0 {
		return fmt.Errorf("new local registry has unexpected batch count %d", count)
	}
	head, err := contract.HeadCommitment(&bind.CallOpts{Context: ctx}, stream)
	if err != nil {
		return fmt.Errorf("verify empty local registry head: %w", err)
	}
	if head != ([32]byte{}) {
		return errors.New("new local registry has a nonzero head commitment")
	}
	r.Registry = contractAddress
	r.RegistryHash = crypto.Keccak256Hash(code)
	return nil
}

func (r *Runner) writeRunRecord() error {
	record := struct {
		Mode         string `json:"mode"`
		ChainID      string `json:"chain_id"`
		Registry     string `json:"registry_address"`
		RegistryHash string `json:"registry_code_hash"`
		ProcessID    int    `json:"anvil_process_id"`
		StartedAt    string `json:"started_at_utc"`
	}{"SIMULATED", r.ChainID.String(), r.Registry.Hex(), r.RegistryHash.Hex(), r.PID, time.Now().UTC().Format(time.RFC3339Nano)}
	body, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(r.RunDir, "run.json"), append(body, '\n'), 0600); err != nil {
		return fmt.Errorf("write simulator run record: %w", err)
	}
	return nil
}

func waitForAnvil(ctx context.Context, endpoint string, done chan error) (*ethclient.Client, error) {
	deadline := time.NewTimer(startupTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		rpcClient, err := rpc.DialContext(dialCtx, endpoint)
		if err == nil {
			client := ethclient.NewClient(rpcClient)
			var chainID *big.Int
			chainID, err = client.ChainID(dialCtx)
			if err == nil && chainID.Sign() > 0 {
				cancel()
				return client, nil
			}
			client.Close()
		}
		cancel()
		select {
		case err := <-done:
			done <- err
			if err == nil {
				return nil, errors.New("local Anvil exited before RPC readiness")
			}
			return nil, fmt.Errorf("local Anvil exited before RPC readiness: %w", err)
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, errors.New("timed out waiting for local Anvil RPC readiness")
		case <-tick.C:
		}
	}
}

func findAnvil() (string, error) {
	if path, err := exec.LookPath("anvil"); err == nil {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err == nil {
		path := filepath.Join(home, ".foundry", "bin", "anvil")
		if info, statErr := os.Stat(path); statErr == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
			return path, nil
		}
	}
	return "", errors.New("anvil binary not found on PATH or at ~/.foundry/bin/anvil")
}

func reserveLoopbackPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("reserve local simulator port: %w", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func randomChainID() (*big.Int, error) {
	limit := big.NewInt(1 << 30)
	n, err := crand.Int(crand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate simulator chain ID: %w", err)
	}
	return n.Add(n, big.NewInt(1<<30)), nil
}

func validateLoopbackEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("simulator RPC endpoint must be a plain loopback HTTP URL")
	}
	host, _, err := net.SplitHostPort(u.Host)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() {
		return errors.New("simulator RPC endpoint must use a loopback address")
	}
	return nil
}
