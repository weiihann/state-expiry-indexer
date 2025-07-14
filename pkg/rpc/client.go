package rpc

import (
	"context"
	"encoding/json"
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

// ClientInterface defines the interface for RPC client operations
type ClientInterface interface {
	GetLatestBlockNumber(ctx context.Context) (*big.Int, error)
	GetCode(ctx context.Context, address string, blockNumber *big.Int) (string, error)
	GetStateDiff(ctx context.Context, blockNumber *big.Int) ([]TransactionResult, error)
}

type Client struct {
	eth *rpc.Client
}

// Ensure Client implements ClientInterface
var _ ClientInterface = (*Client)(nil)

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
	Balance        any  `json:"balance,omitempty"`
	Code           any  `json:"code,omitempty"`
	Nonce          any  `json:"nonce,omitempty"`
	Storage        any  `json:"storage,omitempty"`
	AccountChanged bool `json:"-"`
	StorageChanged bool `json:"-"`
	IsContract     bool `json:"-"`
}

// UnmarshalJSON custom unmarshaller for AccountDiff to set change flags
func (ad *AccountDiff) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Helper function to process balance/code/nonce
	processField := func(field json.RawMessage, target *any) bool {
		var s string
		if json.Unmarshal(field, &s) == nil && s == "=" {
			*target = s
			return false
		}
		var m map[string]DiffItem
		if json.Unmarshal(field, &m) == nil {
			*target = m
			return true
		}
		// If neither, set to raw message
		*target = field
		return false
	}

	// Process balance
	if bal, ok := raw["balance"]; ok {
		if processField(bal, &ad.Balance) {
			ad.AccountChanged = true
		}
	}

	// Process code
	if code, ok := raw["code"]; ok {
		if processField(code, &ad.Code) {
			ad.AccountChanged = true
			ad.IsContract = true
		}
	}

	// Process nonce
	if nonce, ok := raw["nonce"]; ok {
		if processField(nonce, &ad.Nonce) {
			ad.AccountChanged = true
		}
	}

	// Process storage
	if stor, ok := raw["storage"]; ok {
		var m map[string]map[string]DiffItem
		if json.Unmarshal(stor, &m) == nil {
			ad.Storage = m
			if len(m) > 0 {
				ad.StorageChanged = true
				ad.AccountChanged = true
				ad.IsContract = true
			}
		} else {
			// If not map, set to raw
			ad.Storage = stor
		}
	}

	return nil
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
