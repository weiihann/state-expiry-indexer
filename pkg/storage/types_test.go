package storage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiff_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		want     Diff
	}{
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
			want: Diff{
				Storage: []string{
					"0xe1f979c68554698fa8bf9552587bcd354b4ed0ddf809ee5e2ae60bfa0785ef74",
				},
				IsContract: true,
			},
		},
		{
			name: "multiple storage change",
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
					"0x1": {
						"*": {
							"from": "0x000000000000000000000000000000000000000000000000b469471f80140000",
							"to": "0x00000000000000000000000000000000000000000000000e41dbb290f7bc0000"
						}
					},
					"0x2": {
						"*": {
							"from": "0x000000000000000000000000000000000000000000000000b469471f80140000",
							"to": "0x00000000000000000000000000000000000000000000000e41dbb290f7bc0000"
						}
					},
					"0x3": {
						"*": {
							"from": "0x000000000000000000000000000000000000000000000000b469471f80140000",
							"to": "0x00000000000000000000000000000000000000000000000e41dbb290f7bc0000"
						}
					}
				}
			}`,
			want: Diff{
				Storage: []string{
					"0x1",
					"0x2",
					"0x3",
				},
				IsContract: true,
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
			want: Diff{
				Storage:    nil,
				IsContract: false,
			},
		},
		{
			name: "code change (non-empty map)",
			jsonData: `{
				"balance": "=",
				"code": {
					"*": {
						"from": "0x",
						"to": "0x60806040"
					}
				},
				"nonce": "=",
				"storage": {}
			}`,
			want: Diff{
				Storage:    nil,
				IsContract: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d Diff
			err := json.Unmarshal([]byte(tt.jsonData), &d)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, d)
		})
	}
}

func TestReadRangeDiffs_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		json string
		want ReadRangeDiffs
	}{
		{
			name: "empty",
			json: `{}`,
			want: ReadRangeDiffs{},
		},
		{
			name: "single diff empty",
			json: `{
			"blockNum": 1000,
			"diffs": [
				{
					"stateDiff": {}
				}
			]}`,
			want: ReadRangeDiffs{
				BlockNum: 1000,
				Diffs: []ReadDiffs{
					{
						StateDiff: map[string]Diff{},
					},
				},
			},
		},
		{
			name: "multiple diffs empty",
			json: `{
			"blockNum": 1000,
			"diffs": [
				{
					"stateDiff": {}
				},
				{
					"stateDiff": {}
				}
			]}`,
			want: ReadRangeDiffs{
				BlockNum: 1000,
				Diffs: []ReadDiffs{
					{
						StateDiff: map[string]Diff{},
					},
					{
						StateDiff: map[string]Diff{},
					},
				},
			},
		},
		{
			name: "single diff with balance and nonce change only",
			json: `{
			"blockNum": 1000,
			"diffs": [
				{
					"stateDiff": {
						"0x1234567890abcdef1234567890abcdef12345678": {
							"balance": {"*": {"from": "0x0", "to": "0x100"}},
							"nonce": {"*": {"from": "0x0", "to": "0x1"}},
							"storage": {}
						}
					}
				}
			]}`,
			want: ReadRangeDiffs{
				BlockNum: 1000,
				Diffs: []ReadDiffs{
					{
						StateDiff: map[string]Diff{
							"0x1234567890abcdef1234567890abcdef12345678": {
								Storage:    nil,
								IsContract: false,
							},
						},
					},
				},
			},
		},
		{
			name: "multiple diffs with balance and nonce change only",
			json: `{
			"blockNum": 1000,
			"diffs": [
				{
					"stateDiff": {
						"0x1": {
							"balance": {"*": {"from": "0x0", "to": "0x100"}},
							"nonce": {"*": {"from": "0x0", "to": "0x1"}},
							"storage": {}
						}
					}
				},
				{
					"stateDiff": {
						"0x2": {
							"balance": {"*": {"from": "0x0", "to": "0x100"}},
							"nonce": {"*": {"from": "0x0", "to": "0x1"}},
							"storage": {}
						}
					}
				}
			]}`,
			want: ReadRangeDiffs{
				BlockNum: 1000,
				Diffs: []ReadDiffs{
					{
						StateDiff: map[string]Diff{
							"0x1": {
								Storage:    nil,
								IsContract: false,
							},
						},
					},
					{
						StateDiff: map[string]Diff{
							"0x2": {
								Storage:    nil,
								IsContract: false,
							},
						},
					},
				},
			},
		},
		{
			name: "multiple diffs with code change only",
			json: `{
			"blockNum": 1000,
			"diffs": [
				{
					"stateDiff": {
						"0x1": {
							"code": {"*": {"from": "0x0", "to": "0x100"}},
							"balance": "=",
							"nonce": "=",
							"storage": {}
						}
					}
				},
				{
					"stateDiff": {
						"0x2": {
							"code": {"*": {"from": "0x0", "to": "0x100"}},
							"balance": "=",
							"nonce": "=",
							"storage": {}
						}
					}
				}
			]}`,
			want: ReadRangeDiffs{
				BlockNum: 1000,
				Diffs: []ReadDiffs{
					{
						StateDiff: map[string]Diff{
							"0x1": {
								Storage:    nil,
								IsContract: true,
							},
						},
					},
					{
						StateDiff: map[string]Diff{
							"0x2": {
								Storage:    nil,
								IsContract: true,
							},
						},
					},
				},
			},
		},
		{
			name: "multiple diffs with storage change only",
			json: `{
			"blockNum": 1000,
			"diffs": [
				{
					"stateDiff": {
						"0x1": {
							"storage": {
								"0x1": {
									"*": {"from": "0x0", "to": "0x100"}
								}
							}
						}
					}
				},
				{
					"stateDiff": {
						"0x2": {
							"storage": {
								"0x2": {
									"*": {"from": "0x0", "to": "0x100"}
								}
							}
						}
					}
				}
			]}`,
			want: ReadRangeDiffs{
				BlockNum: 1000,
				Diffs: []ReadDiffs{
					{
						StateDiff: map[string]Diff{
							"0x1": {
								Storage:    []string{"0x1"},
								IsContract: true,
							},
						},
					},
					{
						StateDiff: map[string]Diff{
							"0x2": {
								Storage:    []string{"0x2"},
								IsContract: true,
							},
						},
					},
				},
			},
		},
		{
			name: "multiple diffs with multiple changes",
			json: `{
				"blockNum": 1000,
				"diffs": [
					{
						"stateDiff": {
							"0x1": {
								"storage": {
									"0x1": {
										"*": {"from": "0x0", "to": "0x100"}
									},
									"0x2": {
										"*": {"from": "0x0", "to": "0x100"}
									},
									"0x3": {
										"*": {"from": "0x0", "to": "0x100"}
									}
								}
							}
						}
					},
					{
						"stateDiff": {
							"0x2": {
								"storage": {
									"0x1": {
										"*": {"from": "0x0", "to": "0x100"}
									},
									"0x2": {
										"*": {"from": "0x0", "to": "0x100"}
									},
									"0x3": {
										"*": {"from": "0x0", "to": "0x100"}
									}
								}
							}
						}
					}
				]}`,
			want: ReadRangeDiffs{
				BlockNum: 1000,
				Diffs: []ReadDiffs{
					{
						StateDiff: map[string]Diff{
							"0x1": {
								Storage:    []string{"0x1", "0x2", "0x3"},
								IsContract: true,
							},
						},
					},
					{
						StateDiff: map[string]Diff{
							"0x2": {
								Storage:    []string{"0x1", "0x2", "0x3"},
								IsContract: true,
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got ReadRangeDiffs
			err := json.Unmarshal([]byte(tt.json), &got)
			assert.NoError(t, err)

			assert.Equal(t, tt.want.BlockNum, got.BlockNum)
			assert.Equal(t, len(tt.want.Diffs), len(got.Diffs), "Diffs length mismatch")
			for i, wantDiff := range tt.want.Diffs {
				gotDiff := got.Diffs[i]
				assert.Equal(t, len(wantDiff.StateDiff), len(gotDiff.StateDiff), "StateDiff length mismatch at Diffs[%d]", i)
				for addr, wantState := range wantDiff.StateDiff {
					gotState, ok := gotDiff.StateDiff[addr]
					assert.True(t, ok, "Missing address %s in got.StateDiff at Diffs[%d]", addr, i)
					assert.Equal(t, wantState.IsContract, gotState.IsContract, "IsContract mismatch for %s at Diffs[%d]", addr, i)
					assert.ElementsMatch(t, wantState.Storage, gotState.Storage, "Storage mismatch for %s at Diffs[%d]", addr, i)
				}
			}
		})
	}
}

func TestReadRangeDiffsSlice_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		json string
		want []ReadRangeDiffs
	}{
		{
			name: "empty",
			json: `[]`,
			want: []ReadRangeDiffs{},
		},
		{
			name: "multiple diffs empty",
			json: `[
				{
					"blockNum": 1000,
					"diffs": [
						{
							"stateDiff": {}
						}
					]
				},
				{
					"blockNum": 1001,
					"diffs": [
						{
							"stateDiff": {}
						}
					]
				}
			]`,
			want: []ReadRangeDiffs{
				{
					BlockNum: 1000,
					Diffs: []ReadDiffs{
						{
							StateDiff: map[string]Diff{},
						},
					},
				},
				{
					BlockNum: 1001,
					Diffs: []ReadDiffs{
						{
							StateDiff: map[string]Diff{},
						},
					},
				},
			},
		},
		{
			name: "multiple diffs",
			json: `[
				{
					"blockNum": 1000,
					"diffs": [
						{
							"stateDiff": {
								"0x1": {
									"balance": {"*": {"from": "0x0", "to": "0x100"}},
									"nonce": {"*": {"from": "0x0", "to": "0x1"}},
									"storage": {}
								}
							}
						}
					]
				},
				{
					"blockNum": 1001,
					"diffs": [
						{
							"stateDiff": {
								"0x1": {
									"storage": {
										"0x1": {"*": {"from": "0x0", "to": "0x100"}},
										"0x2": {"*": {"from": "0x0", "to": "0x100"}},
										"0x3": {"*": {"from": "0x0", "to": "0x100"}}
									}
								}
							}
						}
					]
				}
			]`,
			want: []ReadRangeDiffs{
				{
					BlockNum: 1000,
					Diffs: []ReadDiffs{
						{
							StateDiff: map[string]Diff{
								"0x1": {
									Storage:    nil,
									IsContract: false,
								},
							},
						},
					},
				},
				{
					BlockNum: 1001,
					Diffs: []ReadDiffs{
						{
							StateDiff: map[string]Diff{
								"0x1": {
									Storage:    []string{"0x1", "0x2", "0x3"},
									IsContract: true,
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []ReadRangeDiffs
			err := json.Unmarshal([]byte(tt.json), &got)

			assert.NoError(t, err)
			assert.Equal(t, len(tt.want), len(got), "Diffs length mismatch")

			for i, wantDiff := range tt.want {
				gotDiff := got[i]
				assert.Equal(t, wantDiff.BlockNum, gotDiff.BlockNum)
				assert.Equal(t, len(wantDiff.Diffs), len(gotDiff.Diffs), "Diffs length mismatch")
				for j, wantDiff := range wantDiff.Diffs {
					gotDiff := gotDiff.Diffs[j]
					assert.Equal(t, len(wantDiff.StateDiff), len(gotDiff.StateDiff), "StateDiff length mismatch at Diffs[%d]", i)
					for addr, wantState := range wantDiff.StateDiff {
						gotState, ok := gotDiff.StateDiff[addr]
						assert.True(t, ok, "Missing address %s in got.StateDiff at Diffs[%d]", addr, i)
						assert.Equal(t, wantState.IsContract, gotState.IsContract, "IsContract mismatch for %s at Diffs[%d]", addr, i)
						assert.ElementsMatch(t, wantState.Storage, gotState.Storage, "Storage mismatch for %s at Diffs[%d]", addr, i)
					}
				}
			}
		})
	}
}
