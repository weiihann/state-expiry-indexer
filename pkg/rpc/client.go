package rpc

import (
	"context"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
)

// ClientInterface defines the interface for RPC client operations
type ClientInterface interface {
	GetLatestBlockNumber(ctx context.Context) (*big.Int, error)
	GetCode(ctx context.Context, address string, blockNumber *big.Int) (string, error)
	GetStateDiff(ctx context.Context, blockNumber *big.Int) ([]TransactionResult, error)
}

type Client struct {
	eth    *rpc.Client
	logger *slog.Logger
}

// Ensure Client implements ClientInterface
var _ ClientInterface = (*Client)(nil)

func NewClient(ctx context.Context, url string) (*Client, error) {
	logger := logger.GetLogger("rpc-client")
	eth, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, err
	}
	return &Client{eth: eth, logger: logger}, nil
}

func (c *Client) GetLatestBlockNumber(ctx context.Context) (*big.Int, error) {
	var result hexutil.Big
	err := c.eth.CallContext(ctx, &result, "eth_blockNumber")
	if err != nil {
		return nil, err
	}
	return (*big.Int)(&result), nil
}

// GetCode returns the contract code at the given address and block number
func (c *Client) GetCode(ctx context.Context, address string, blockNumber *big.Int) (string, error) {
	var result string
	blockParam := "latest"
	if blockNumber != nil {
		blockParam = hexutil.EncodeBig(blockNumber)
	}

	err := c.eth.CallContext(ctx, &result, "eth_getCode", address, blockParam)
	if err != nil {
		return "", err
	}
	return result, nil
}

// DiffItem represents a single change in the state diff
type DiffItem struct {
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

// AccountDiff represents the changes to a single account
type AccountDiff struct {
	Balance any `json:"balance,omitempty"`
	Code    any `json:"code,omitempty"`
	Nonce   any `json:"nonce,omitempty"`
	Storage any `json:"storage,omitempty"`
}

// StateDiff represents the state diff for a single transaction
type TransactionResult struct {
	StateDiff map[string]AccountDiff `json:"stateDiff"`
	TxHash    string                 `json:"transactionHash"`
}

func (c *Client) GetStateDiff(ctx context.Context, blockNumber *big.Int) ([]TransactionResult, error) {
	var result []TransactionResult
	err := c.eth.CallContext(ctx, &result, "trace_replayBlockTransactions", hexutil.EncodeBig(blockNumber), []string{"stateDiff"})
	if err != nil {
		return nil, err
	}
	return result, nil
}
