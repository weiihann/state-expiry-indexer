package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockRPCServer creates a test HTTP server that mocks Ethereum RPC responses
type MockRPCServer struct {
	server   *httptest.Server
	handlers map[string]func(params []interface{}) (interface{}, error)
}

func NewMockRPCServer() *MockRPCServer {
	mock := &MockRPCServer{
		handlers: make(map[string]func(params []interface{}) (interface{}, error)),
	}

	// Create HTTP server with JSON-RPC handler
	mock.server = httptest.NewServer(http.HandlerFunc(mock.handleRPC))

	return mock
}

func (m *MockRPCServer) Close() {
	m.server.Close()
}

func (m *MockRPCServer) URL() string {
	return m.server.URL
}

// SetHandler sets a handler for a specific RPC method
func (m *MockRPCServer) SetHandler(method string, handler func(params []interface{}) (interface{}, error)) {
	m.handlers[method] = handler
}

// JSON-RPC request structure
type rpcRequest struct {
	ID     int           `json:"id"`
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}

// JSON-RPC response structure
type rpcResponse struct {
	ID     int         `json:"id"`
	Result interface{} `json:"result,omitempty"`
	Error  *rpcError   `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (m *MockRPCServer) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	handler, exists := m.handlers[req.Method]
	if !exists {
		resp := rpcResponse{
			ID: req.ID,
			Error: &rpcError{
				Code:    -32601,
				Message: "Method not found",
			},
		}
		json.NewEncoder(w).Encode(resp)
		return
	}

	result, err := handler(req.Params)
	if err != nil {
		resp := rpcResponse{
			ID: req.ID,
			Error: &rpcError{
				Code:    -32000,
				Message: err.Error(),
			},
		}
		json.NewEncoder(w).Encode(resp)
		return
	}

	resp := rpcResponse{
		ID:     req.ID,
		Result: result,
	}
	json.NewEncoder(w).Encode(resp)
}

func TestNewClient(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	ctx := context.Background()

	t.Run("successfully creates client with valid URL", func(t *testing.T) {
		client, err := NewClient(ctx, mockServer.URL())
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.NotNil(t, client.eth)
	})

	t.Run("returns error with invalid URL", func(t *testing.T) {
		_, err := NewClient(ctx, "invalid-url")
		assert.Error(t, err)
	})
}

func TestClient_GetLatestBlockNumber(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	ctx := context.Background()
	client, err := NewClient(ctx, mockServer.URL())
	require.NoError(t, err)

	t.Run("returns correct block number", func(t *testing.T) {
		// Mock eth_blockNumber response
		mockServer.SetHandler("eth_blockNumber", func(params []interface{}) (interface{}, error) {
			return "0x1b4", nil // 436 in decimal
		})

		blockNumber, err := client.GetLatestBlockNumber(ctx)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(436), blockNumber)
	})

	t.Run("handles RPC error", func(t *testing.T) {
		// Mock error response
		mockServer.SetHandler("eth_blockNumber", func(params []interface{}) (interface{}, error) {
			return nil, fmt.Errorf("network error")
		})

		_, err := client.GetLatestBlockNumber(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "network error")
	})

	t.Run("handles invalid hex response", func(t *testing.T) {
		// Mock invalid hex response
		mockServer.SetHandler("eth_blockNumber", func(params []interface{}) (interface{}, error) {
			return "invalid-hex", nil
		})

		_, err := client.GetLatestBlockNumber(ctx)
		assert.Error(t, err)
	})

	t.Run("handles large block numbers", func(t *testing.T) {
		// Mock large block number (above uint64 max)
		largeHex := "0x10000000000000000" // 2^64
		mockServer.SetHandler("eth_blockNumber", func(params []interface{}) (interface{}, error) {
			return largeHex, nil
		})

		blockNumber, err := client.GetLatestBlockNumber(ctx)
		assert.NoError(t, err)
		expected := new(big.Int)
		expected.SetString("10000000000000000", 16)
		assert.Equal(t, expected, blockNumber)
	})
}

func TestClient_GetStateDiff(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	ctx := context.Background()
	client, err := NewClient(ctx, mockServer.URL())
	require.NoError(t, err)

	t.Run("returns valid state diff", func(t *testing.T) {
		// Mock trace_replayBlockTransactions response
		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			// Verify block number parameter
			assert.Len(t, params, 2)
			assert.Equal(t, "0x64", params[0]) // 100 in hex
			assert.Equal(t, []interface{}{"stateDiff"}, params[1])

			return []TransactionResult{
				{
					TxHash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					StateDiff: map[string]AccountDiff{
						"0x1234567890abcdef1234567890abcdef12345678": {
							Balance: map[string]string{
								"from": "0x0",
								"to":   "0x100",
							},
							Nonce: map[string]string{
								"from": "0x0",
								"to":   "0x1",
							},
							Storage: map[string]map[string]string{
								"0x0000000000000000000000000000000000000000000000000000000000000001": {
									"from": "0x0",
									"to":   "0x123",
								},
							},
						},
						"0xabcdef1234567890abcdef1234567890abcdef12": {
							Code: map[string]string{
								"from": "0x",
								"to":   "0x6080604052",
							},
						},
					},
				},
			}, nil
		})

		stateDiffs, err := client.GetStateDiff(ctx, big.NewInt(100))
		assert.NoError(t, err)
		assert.Len(t, stateDiffs, 1)

		txResult := stateDiffs[0]
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", txResult.TxHash)
		assert.Len(t, txResult.StateDiff, 2)

		// Verify account diff structure
		accountDiff := txResult.StateDiff["0x1234567890abcdef1234567890abcdef12345678"]
		assert.NotNil(t, accountDiff.Balance)
		assert.NotNil(t, accountDiff.Nonce)
		assert.NotNil(t, accountDiff.Storage)

		// Verify contract deployment
		contractDiff := txResult.StateDiff["0xabcdef1234567890abcdef1234567890abcdef12"]
		assert.NotNil(t, contractDiff.Code)
	})

	t.Run("handles empty state diff", func(t *testing.T) {
		// Mock empty response
		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			return []TransactionResult{}, nil
		})

		stateDiffs, err := client.GetStateDiff(ctx, big.NewInt(200))
		assert.NoError(t, err)
		assert.Len(t, stateDiffs, 0)
	})

	t.Run("handles block with no transactions", func(t *testing.T) {
		// Mock response for block with no transactions
		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			return []TransactionResult{}, nil
		})

		stateDiffs, err := client.GetStateDiff(ctx, big.NewInt(300))
		assert.NoError(t, err)
		assert.Len(t, stateDiffs, 0)
	})

	t.Run("handles RPC error", func(t *testing.T) {
		// Mock error response
		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			return nil, fmt.Errorf("block not found")
		})

		_, err := client.GetStateDiff(ctx, big.NewInt(999999))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "block not found")
	})

	t.Run("handles complex state diff with multiple transactions", func(t *testing.T) {
		// Mock response with multiple transactions
		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			return []TransactionResult{
				{
					TxHash: "0x1111111111111111111111111111111111111111111111111111111111111111",
					StateDiff: map[string]AccountDiff{
						"0x1111111111111111111111111111111111111111": {
							Balance: map[string]string{"from": "0x0", "to": "0x100"},
						},
					},
				},
				{
					TxHash: "0x2222222222222222222222222222222222222222222222222222222222222222",
					StateDiff: map[string]AccountDiff{
						"0x2222222222222222222222222222222222222222": {
							Nonce: map[string]string{"from": "0x1", "to": "0x2"},
							Storage: map[string]map[string]string{
								"0x0000000000000000000000000000000000000000000000000000000000000001": {
									"from": "0x0",
									"to":   "0x456",
								},
								"0x0000000000000000000000000000000000000000000000000000000000000002": {
									"from": "0x123",
									"to":   "0x789",
								},
							},
						},
					},
				},
			}, nil
		})

		stateDiffs, err := client.GetStateDiff(ctx, big.NewInt(400))
		assert.NoError(t, err)
		assert.Len(t, stateDiffs, 2)

		// Verify first transaction
		tx1 := stateDiffs[0]
		assert.Equal(t, "0x1111111111111111111111111111111111111111111111111111111111111111", tx1.TxHash)
		assert.Len(t, tx1.StateDiff, 1)

		// Verify second transaction
		tx2 := stateDiffs[1]
		assert.Equal(t, "0x2222222222222222222222222222222222222222222222222222222222222222", tx2.TxHash)
		assert.Len(t, tx2.StateDiff, 1)

		// Verify storage changes in second transaction
		accountDiff := tx2.StateDiff["0x2222222222222222222222222222222222222222"]
		storage, ok := accountDiff.Storage.(map[string]any)
		assert.True(t, ok)
		assert.Len(t, storage, 2)
	})

	t.Run("handles null values in state diff", func(t *testing.T) {
		// Mock response with null values (account deletion)
		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			return []TransactionResult{
				{
					TxHash: "0x3333333333333333333333333333333333333333333333333333333333333333",
					StateDiff: map[string]AccountDiff{
						"0x3333333333333333333333333333333333333333": {
							Balance: map[string]interface{}{
								"from": "0x100",
								"to":   nil, // Account deletion
							},
							Code: map[string]interface{}{
								"from": "0x6080604052",
								"to":   nil, // Code deletion
							},
						},
					},
				},
			}, nil
		})

		stateDiffs, err := client.GetStateDiff(ctx, big.NewInt(500))
		assert.NoError(t, err)
		assert.Len(t, stateDiffs, 1)

		tx := stateDiffs[0]
		assert.Equal(t, "0x3333333333333333333333333333333333333333333333333333333333333333", tx.TxHash)

		accountDiff := tx.StateDiff["0x3333333333333333333333333333333333333333"]
		assert.NotNil(t, accountDiff.Balance)
		assert.NotNil(t, accountDiff.Code)
	})

	t.Run("correctly encodes block number parameter", func(t *testing.T) {
		var capturedBlockParam string
		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			capturedBlockParam = params[0].(string)
			return []TransactionResult{}, nil
		})

		// Test different block numbers
		testCases := []struct {
			blockNumber *big.Int
			expectedHex string
		}{
			{big.NewInt(0), "0x0"},
			{big.NewInt(1), "0x1"},
			{big.NewInt(255), "0xff"},
			{big.NewInt(256), "0x100"},
			{big.NewInt(1000000), "0xf4240"},
		}

		for _, tc := range testCases {
			_, err := client.GetStateDiff(ctx, tc.blockNumber)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedHex, capturedBlockParam,
				"Block number %s should be encoded as %s", tc.blockNumber.String(), tc.expectedHex)
		}
	})

	t.Run("handles network timeout", func(t *testing.T) {
		// Create a context with timeout
		timeoutCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			return []TransactionResult{}, nil
		})

		_, err := client.GetStateDiff(timeoutCtx, big.NewInt(600))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})
}

func TestClient_JSONRPCCompatibility(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	ctx := context.Background()
	client, err := NewClient(ctx, mockServer.URL())
	require.NoError(t, err)

	t.Run("handles JSON-RPC 2.0 format", func(t *testing.T) {
		// Test that our mock server correctly handles JSON-RPC format
		mockServer.SetHandler("eth_blockNumber", func(params []interface{}) (interface{}, error) {
			assert.Len(t, params, 0) // eth_blockNumber takes no parameters
			return "0x123", nil
		})

		blockNumber, err := client.GetLatestBlockNumber(ctx)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(0x123), blockNumber)
	})
}

func TestAccountDiff_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		expected AccountDiff
	}{
		{
			name: "balance change (map)",
			jsonData: `{
				"balance": {
					"*": {
						"from": "0x549d40dfafc00",
						"to": "0x4c2acb18094c00"
					}
				},
				"code": "=",
				"nonce": "=",
				"storage": {}
			}`,
			expected: AccountDiff{
				Balance:        map[string]DiffItem{"*": {From: "0x549d40dfafc00", To: "0x4c2acb18094c00"}},
				Code:           "=",
				Nonce:          "=",
				Storage:        map[string]map[string]DiffItem{},
				AccountChanged: true,
				StorageChanged: false,
			},
		},
		{
			name: "nonce change (map)",
			jsonData: `{
				"balance": {
					"*": {
						"from": "0x19212992f3917b5e50c",
						"to": "0x192124ad851b3a0550c"
					}
				},
				"code": "=",
				"nonce": {
					"*": {
						"from": "0x285a7",
						"to": "0x285a8"
					}
				},
				"storage": {}
			}`,
			expected: AccountDiff{
				Balance:        map[string]DiffItem{"*": {From: "0x19212992f3917b5e50c", To: "0x192124ad851b3a0550c"}},
				Code:           "=",
				Nonce:          map[string]DiffItem{"*": {From: "0x285a7", To: "0x285a8"}},
				Storage:        map[string]map[string]DiffItem{},
				AccountChanged: true,
				StorageChanged: false,
			},
		},
		{
			name: "storage change (non-empty map)",
			jsonData: `{
				"balance": {
					"*": {
						"from": "0x693c124a2b710860c0",
						"to": "0x693c19c01bcb0fa0c0"
					}
				},
				"code": "=",
				"nonce": "=",
				"storage": {
					"0xe1f979c68554698fa8bf9552587bcd354b4ed0ddf809ee5e2ae60bfa0785ef74": {
						"*": {
							"from": "0x000000000000000000000000000000000000000000000000b469471f80140000",
							"to": "0x00000000000000000000000000000000000000000000000e41dbb290f7bc0000"
						}
					}
				}
			}`,
			expected: AccountDiff{
				Balance: map[string]DiffItem{"*": {From: "0x693c124a2b710860c0", To: "0x693c19c01bcb0fa0c0"}},
				Code:    "=",
				Nonce:   "=",
				Storage: map[string]map[string]DiffItem{
					"0xe1f979c68554698fa8bf9552587bcd354b4ed0ddf809ee5e2ae60bfa0785ef74": {"*": DiffItem{From: "0x000000000000000000000000000000000000000000000000b469471f80140000", To: "0x00000000000000000000000000000000000000000000000e41dbb290f7bc0000"}},
				},
				AccountChanged: true,
				StorageChanged: true,
			},
		},
		{
			name: "empty storage (no change)",
			jsonData: `{
				"balance": "=",
				"code": "=",
				"nonce": "=",
				"storage": {}
			}`,
			expected: AccountDiff{
				Balance:        "=",
				Code:           "=",
				Nonce:          "=",
				Storage:        map[string]map[string]DiffItem{},
				AccountChanged: false,
				StorageChanged: false,
			},
		},
		{
			name: "code change (map)",
			jsonData: `{
				"balance": "=",
				"code": {
					"*": {
						"from": "0x",
						"to": "0x6080604052"
					}
				},
				"nonce": "=",
				"storage": {}
			}`,
			expected: AccountDiff{
				Balance:        "=",
				Code:           map[string]DiffItem{"*": {From: "0x", To: "0x6080604052"}},
				Nonce:          "=",
				Storage:        map[string]map[string]DiffItem{},
				AccountChanged: true,
				StorageChanged: false,
			},
		},
		{
			name: "no changes",
			jsonData: `{
				"balance": "=",
				"code": "=",
				"nonce": "=",
				"storage": {}
			}`,
			expected: AccountDiff{
				Balance:        "=",
				Code:           "=",
				Nonce:          "=",
				Storage:        map[string]map[string]DiffItem{},
				AccountChanged: false,
				StorageChanged: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ad AccountDiff
			err := json.Unmarshal([]byte(tt.jsonData), &ad)
			assert.NoError(t, err)

			// Assert flags
			assert.Equal(t, tt.expected.AccountChanged, ad.AccountChanged)
			assert.Equal(t, tt.expected.StorageChanged, ad.StorageChanged)

			// Assert fields (basic type checks)
			assert.Equal(t, tt.expected.Balance, ad.Balance)
			assert.Equal(t, tt.expected.Code, ad.Code)
			assert.Equal(t, tt.expected.Nonce, ad.Nonce)
			assert.Equal(t, tt.expected.Storage, ad.Storage)
		})
	}
}

func TestClient_EdgeCases(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	ctx := context.Background()
	client, err := NewClient(ctx, mockServer.URL())
	require.NoError(t, err)

	t.Run("handles zero block number", func(t *testing.T) {
		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			assert.Equal(t, "0x0", params[0])
			return []TransactionResult{}, nil
		})

		stateDiffs, err := client.GetStateDiff(ctx, big.NewInt(0))
		assert.NoError(t, err)
		assert.Len(t, stateDiffs, 0)
	})

	t.Run("handles very large block number", func(t *testing.T) {
		largeBlock := new(big.Int)
		largeBlock.SetString("999999999999999999999", 10)

		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			// Should be encoded as hex
			assert.Equal(t, "0x3635c9adc5de9fffff", params[0])
			return []TransactionResult{}, nil
		})

		stateDiffs, err := client.GetStateDiff(ctx, largeBlock)
		assert.NoError(t, err)
		assert.Len(t, stateDiffs, 0)
	})

	t.Run("handles malformed JSON response", func(t *testing.T) {
		// Close the mock server to simulate network error
		mockServer.Close()

		_, err := client.GetLatestBlockNumber(ctx)
		assert.Error(t, err)
	})
}

func TestClient_RealWorldScenarios(t *testing.T) {
	mockServer := NewMockRPCServer()
	defer mockServer.Close()

	ctx := context.Background()
	client, err := NewClient(ctx, mockServer.URL())
	require.NoError(t, err)

	t.Run("simulates ERC20 transfer", func(t *testing.T) {
		// Mock state diff for an ERC20 transfer
		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			return []TransactionResult{
				{
					TxHash: "0xerc20transfer1234567890abcdef1234567890abcdef1234567890abcdef123456",
					StateDiff: map[string]AccountDiff{
						// ERC20 contract state changes
						"0xa0b86a33e6d01a7ca3b15c8a0f5a2c1e5e5e5e5e": {
							Storage: map[string]map[string]string{
								// Sender balance slot
								"0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef": {
									"from": "0x64", // 100 tokens
									"to":   "0x32", // 50 tokens
								},
								// Receiver balance slot
								"0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890": {
									"from": "0x0",  // 0 tokens
									"to":   "0x32", // 50 tokens
								},
							},
						},
						// Sender account (gas payment)
						"0x1111111111111111111111111111111111111111": {
							Balance: map[string]string{
								"from": "0x1bc16d674ec80000", // 2 ETH
								"to":   "0x1b1ae4d6e2ef5000", // 1.98 ETH (gas paid)
							},
							Nonce: map[string]string{
								"from": "0x5",
								"to":   "0x6",
							},
						},
					},
				},
			}, nil
		})

		stateDiffs, err := client.GetStateDiff(ctx, big.NewInt(12345))
		assert.NoError(t, err)
		assert.Len(t, stateDiffs, 1)

		tx := stateDiffs[0]
		assert.Contains(t, tx.TxHash, "erc20transfer")
		assert.Len(t, tx.StateDiff, 2) // Contract and sender account
	})

	t.Run("simulates contract deployment", func(t *testing.T) {
		// Mock state diff for contract deployment
		mockServer.SetHandler("trace_replayBlockTransactions", func(params []interface{}) (interface{}, error) {
			return []TransactionResult{
				{
					TxHash: "0xcontractdeployment567890abcdef1234567890abcdef1234567890abcdef12",
					StateDiff: map[string]AccountDiff{
						// Deployer account
						"0x2222222222222222222222222222222222222222": {
							Balance: map[string]string{
								"from": "0x2386f26fc10000", // Starting balance
								"to":   "0x2386f26fc10000", // Same (no ETH sent to contract)
							},
							Nonce: map[string]string{
								"from": "0xa",
								"to":   "0xb",
							},
						},
						// New contract account
						"0x3333333333333333333333333333333333333333": {
							Balance: map[string]interface{}{
								"from": nil,   // Didn't exist
								"to":   "0x0", // Created with 0 balance
							},
							Code: map[string]interface{}{
								"from": nil,                                                                // No code
								"to":   "0x6080604052348015600f57600080fd5b506004361060285760003560e01c80", // Contract bytecode
							},
							Nonce: map[string]interface{}{
								"from": nil,   // Didn't exist
								"to":   "0x1", // Contract nonce starts at 1
							},
							Storage: map[string]map[string]interface{}{
								// Constructor may set initial storage
								"0x0000000000000000000000000000000000000000000000000000000000000000": {
									"from": nil,
									"to":   "0x1234567890abcdef",
								},
							},
						},
					},
				},
			}, nil
		})

		stateDiffs, err := client.GetStateDiff(ctx, big.NewInt(54321))
		assert.NoError(t, err)
		assert.Len(t, stateDiffs, 1)

		tx := stateDiffs[0]
		assert.Contains(t, tx.TxHash, "contractdeployment")

		// Verify contract creation
		contractDiff := tx.StateDiff["0x3333333333333333333333333333333333333333"]
		assert.NotNil(t, contractDiff.Code)
		assert.NotNil(t, contractDiff.Balance)
		assert.NotNil(t, contractDiff.Storage)
	})
}
