package rpc

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

type Client struct {
	eth *rpc.Client
}

func NewClient(ctx context.Context, url string) (*Client, error) {
	eth, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, err
	}
	return &Client{eth: eth}, nil
}

func (c *Client) GetLatestBlockNumber(ctx context.Context) (*big.Int, error) {
	var result hexutil.Big
	err := c.eth.CallContext(ctx, &result, "eth_blockNumber")
	if err != nil {
		return nil, err
	}
	return (*big.Int)(&result), nil
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
