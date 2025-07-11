package repository

// Optimized data structures for efficient ClickHouse queries
// Focused on answering the 15 target questions with minimal database operations

// ==============================================================================
// ACCOUNT ANALYTICS STRUCTURES (Questions 1, 2, 5a)
// ==============================================================================

type AccountAnalytics struct {
	Total           AccountTotals           `json:"total"`
	Expiry          AccountExpiryData       `json:"expiry"`
	SingleAccess    AccountSingleAccessData `json:"single_access"`
	Distribution    AccountDistribution     `json:"distribution"`
}

type AccountTotals struct {
	EOAs      int `json:"eoas"`
	Contracts int `json:"contracts"`
	Total     int `json:"total"`
}

type AccountExpiryData struct {
	ExpiredEOAs      int     `json:"expired_eoas"`
	ExpiredContracts int     `json:"expired_contracts"`
	TotalExpired     int     `json:"total_expired"`
	ExpiryRate       float64 `json:"expiry_rate"`
}

type AccountSingleAccessData struct {
	SingleAccessEOAs      int     `json:"single_access_eoas"`
	SingleAccessContracts int     `json:"single_access_contracts"`
	TotalSingleAccess     int     `json:"total_single_access"`
	SingleAccessRate      float64 `json:"single_access_rate"`
}

type AccountDistribution struct {
	EOAPercentage      float64 `json:"eoa_percentage"`
	ContractPercentage float64 `json:"contract_percentage"`
}

// ==============================================================================
// STORAGE ANALYTICS STRUCTURES (Questions 3, 4, 5b)
// ==============================================================================

type StorageAnalytics struct {
	Total        StorageTotals           `json:"total"`
	Expiry       StorageExpiryData       `json:"expiry"`
	SingleAccess StorageSingleAccessData `json:"single_access"`
}

type StorageTotals struct {
	TotalSlots int `json:"total_slots"`
}

type StorageExpiryData struct {
	ExpiredSlots int     `json:"expired_slots"`
	ActiveSlots  int     `json:"active_slots"`
	ExpiryRate   float64 `json:"expiry_rate"`
}

type StorageSingleAccessData struct {
	SingleAccessSlots int     `json:"single_access_slots"`
	SingleAccessRate  float64 `json:"single_access_rate"`
}

// ==============================================================================
// CONTRACT ANALYTICS STRUCTURES (Questions 7, 8, 9, 10, 11, 15)
// ==============================================================================

type ContractAnalytics struct {
	Rankings        ContractRankings        `json:"rankings"`
	ExpiryAnalysis  ContractExpiryAnalysis  `json:"expiry_analysis"`
	VolumeAnalysis  ContractVolumeAnalysis  `json:"volume_analysis"`
	StatusAnalysis  ContractStatusAnalysis  `json:"status_analysis"`
}

type ContractRankings struct {
	TopByExpiredSlots []ContractRankingItem `json:"top_by_expired_slots"`
	TopByTotalSlots   []ContractRankingItem `json:"top_by_total_slots"`
}

type ContractRankingItem struct {
	Address            string  `json:"address"`
	TotalSlots         int     `json:"total_slots"`
	ExpiredSlots       int     `json:"expired_slots"`
	ActiveSlots        int     `json:"active_slots"`
	ExpiryPercentage   float64 `json:"expiry_percentage"`
	LastAccess         uint64  `json:"last_access"`
	IsAccountActive    bool    `json:"is_account_active"`
}

type ContractExpiryAnalysis struct {
	AverageExpiryPercentage   float64                    `json:"average_expiry_percentage"`
	MedianExpiryPercentage    float64                    `json:"median_expiry_percentage"`
	ExpiryDistribution        []ExpiryDistributionBucket `json:"expiry_distribution"`
	ContractsAnalyzed         int                        `json:"contracts_analyzed"`
}

type ExpiryDistributionBucket struct {
	RangeStart int `json:"range_start"`
	RangeEnd   int `json:"range_end"`
	Count      int `json:"count"`
}

type ContractVolumeAnalysis struct {
	AverageStoragePerContract float64 `json:"average_storage_per_contract"`
	MedianStoragePerContract  float64 `json:"median_storage_per_contract"`
	MaxStoragePerContract     int     `json:"max_storage_per_contract"`
	MinStoragePerContract     int     `json:"min_storage_per_contract"`
	TotalContracts            int     `json:"total_contracts"`
}

type ContractStatusAnalysis struct {
	AllExpiredContracts    int     `json:"all_expired_contracts"`
	AllActiveContracts     int     `json:"all_active_contracts"`
	MixedStateContracts    int     `json:"mixed_state_contracts"`
	ActiveWithExpiredStorage int   `json:"active_with_expired_storage"`
	AllExpiredRate         float64 `json:"all_expired_rate"`
	AllActiveRate          float64 `json:"all_active_rate"`
}

// ==============================================================================
// BLOCK ACTIVITY ANALYTICS STRUCTURES (Questions 6, 12, 13, 14)
// ==============================================================================

type BlockActivityAnalytics struct {
	TopBlocks       []BlockActivity       `json:"top_blocks"`
	TimeSeriesData  []TimeSeriesPoint     `json:"time_series_data"`
	AccessRates     AccessRateAnalysis    `json:"access_rates"`
	FrequencyData   FrequencyAnalysis     `json:"frequency_data"`
	TrendData       TrendAnalysis         `json:"trend_data"`
}

type BlockActivity struct {
	BlockNumber     uint64 `json:"block_number"`
	AccountAccesses int    `json:"account_accesses"`
	StorageAccesses int    `json:"storage_accesses"`
	TotalAccesses   int    `json:"total_accesses"`
	EOAAccesses     int    `json:"eoa_accesses"`
	ContractAccesses int   `json:"contract_accesses"`
}

type TimeSeriesPoint struct {
	WindowStart      uint64  `json:"window_start"`
	WindowEnd        uint64  `json:"window_end"`
	AccountAccesses  int     `json:"account_accesses"`
	StorageAccesses  int     `json:"storage_accesses"`
	TotalAccesses    int     `json:"total_accesses"`
	AccessesPerBlock float64 `json:"accesses_per_block"`
}

type AccessRateAnalysis struct {
	AccountsPerBlock     float64 `json:"accounts_per_block"`
	StoragePerBlock      float64 `json:"storage_per_block"`
	TotalAccessesPerBlock float64 `json:"total_accesses_per_block"`
	BlocksAnalyzed       int     `json:"blocks_analyzed"`
}

type FrequencyAnalysis struct {
	AccountFrequency AccountFrequencyData `json:"account_frequency"`
	StorageFrequency StorageFrequencyData `json:"storage_frequency"`
}

type AccountFrequencyData struct {
	AverageFrequency   float64 `json:"average_frequency"`
	MedianFrequency    float64 `json:"median_frequency"`
	MostFrequentAccounts []FrequentAccount `json:"most_frequent_accounts"`
}

type StorageFrequencyData struct {
	AverageFrequency  float64 `json:"average_frequency"`
	MedianFrequency   float64 `json:"median_frequency"`
	MostFrequentSlots []FrequentStorage `json:"most_frequent_slots"`
}

type FrequentAccount struct {
	Address     string `json:"address"`
	AccessCount int    `json:"access_count"`
	IsContract  bool   `json:"is_contract"`
}

type FrequentStorage struct {
	Address     string `json:"address"`
	StorageSlot string `json:"storage_slot"`
	AccessCount int    `json:"access_count"`
}

type TrendAnalysis struct {
	TrendDirection   string  `json:"trend_direction"`   // "increasing", "decreasing", "stable"
	GrowthRate       float64 `json:"growth_rate"`
	PeakActivityBlock uint64 `json:"peak_activity_block"`
	LowActivityBlock  uint64 `json:"low_activity_block"`
}

// ==============================================================================
// UNIFIED ANALYTICS STRUCTURE (All Questions)
// ==============================================================================

type UnifiedAnalytics struct {
	Accounts      AccountAnalytics       `json:"accounts"`
	Storage       StorageAnalytics       `json:"storage"`
	Contracts     ContractAnalytics      `json:"contracts"`
	BlockActivity BlockActivityAnalytics `json:"block_activity"`
	Metadata      AnalyticsMetadata      `json:"metadata"`
}

type AnalyticsMetadata struct {
	ExpiryBlock     uint64 `json:"expiry_block"`
	CurrentBlock    uint64 `json:"current_block"`
	AnalysisRange   uint64 `json:"analysis_range"`
	GeneratedAt     int64  `json:"generated_at"`
	QueryDuration   int64  `json:"query_duration_ms"`
}

// ==============================================================================
// BASIC STATISTICS STRUCTURE (Quick Overview)
// ==============================================================================

type BasicStats struct {
	Accounts BasicAccountStats `json:"accounts"`
	Storage  BasicStorageStats `json:"storage"`
	Metadata BasicMetadata     `json:"metadata"`
}

type BasicAccountStats struct {
	TotalEOAs        int `json:"total_eoas"`
	TotalContracts   int `json:"total_contracts"`
	ExpiredEOAs      int `json:"expired_eoas"`
	ExpiredContracts int `json:"expired_contracts"`
}

type BasicStorageStats struct {
	TotalSlots   int `json:"total_slots"`
	ExpiredSlots int `json:"expired_slots"`
}

type BasicMetadata struct {
	ExpiryBlock  uint64 `json:"expiry_block"`
	CurrentBlock uint64 `json:"current_block"`
	GeneratedAt  int64  `json:"generated_at"`
}

// ==============================================================================
// QUERY PARAMETERS FOR EFFICIENT FILTERING
// ==============================================================================

type QueryParams struct {
	ExpiryBlock   uint64 `json:"expiry_block"`
	CurrentBlock  uint64 `json:"current_block"`
	StartBlock    uint64 `json:"start_block"`
	EndBlock      uint64 `json:"end_block"`
	WindowSize    int    `json:"window_size"`
	TopN          int    `json:"top_n"`
	MinFrequency  int    `json:"min_frequency"`
}

// Default query parameters
func DefaultQueryParams() QueryParams {
	return QueryParams{
		WindowSize:   1000,
		TopN:         10,
		MinFrequency: 1,
	}
}

// ==============================================================================
// LEGACY STRUCTURES (for backward compatibility)
// ==============================================================================

type SyncStatus struct {
	IsSynced         bool   `json:"is_synced"`
	LastIndexedRange uint64 `json:"last_indexed_range"`
	EndBlock         uint64 `json:"end_block"`
}

type Contract struct {
	Address   string `json:"address"`
	SlotCount int    `json:"slot_count"`
}

type Account struct {
	Address         string `json:"address"`
	LastAccessBlock uint64 `json:"last_access_block"`
	IsContract      *bool  `json:"is_contract,omitempty"`
}