package repository

type Contract struct {
	Address   string `json:"address"`
	SlotCount int    `json:"slot_count"`
}

type Account struct {
	Address         string `json:"address"`
	LastAccessBlock uint64 `json:"last_access_block"`
	IsContract      *bool  `json:"is_contract,omitempty"`
}

// Analytics data structures for comprehensive state expiry analysis
type AnalyticsData struct {
	AccountExpiry                 AccountExpiryAnalysis                 `json:"account_expiry"`
	AccountDistribution           AccountDistributionAnalysis           `json:"account_distribution"`
	StorageSlotExpiry             StorageSlotExpiryAnalysis             `json:"storage_slot_expiry"`
	ContractStorage               ContractStorageAnalysis               `json:"contract_storage"`
	StorageExpiry                 StorageExpiryAnalysis                 `json:"storage_expiry"`
	FullyExpiredContracts         FullyExpiredContractsAnalysis         `json:"fully_expired_contracts"`
	ActiveContractsExpiredStorage ActiveContractsExpiredStorageAnalysis `json:"active_contracts_expired_storage"`
	CompleteExpiry                CompleteExpiryAnalysis                `json:"complete_expiry"`
}

// Extended analytics data structures for comprehensive state analysis
// This structure supports all 15 questions and groups related analytics
type ExtendedAnalyticsData struct {
	AccountExpiry                 AccountExpiryAnalysis                 `json:"account_expiry"`
	AccountDistribution           AccountDistributionAnalysis           `json:"account_distribution"`
	StorageSlotExpiry             StorageSlotExpiryAnalysis             `json:"storage_slot_expiry"`
	ContractStorage               ContractStorageAnalysis               `json:"contract_storage"`
	StorageExpiry                 StorageExpiryAnalysis                 `json:"storage_expiry"`
	FullyExpiredContracts         FullyExpiredContractsAnalysis         `json:"fully_expired_contracts"`
	ActiveContractsExpiredStorage ActiveContractsExpiredStorageAnalysis `json:"active_contracts_expired_storage"`
	CompleteExpiry                CompleteExpiryAnalysis                `json:"complete_expiry"`

	// Advanced analytics (new categories)
	SingleAccess  SingleAccessAnalysis  `json:"single_access"`
	BlockActivity BlockActivityAnalysis `json:"block_activity"`
	TimeSeries    TimeSeriesAnalysis    `json:"time_series"`
	StorageVolume StorageVolumeAnalysis `json:"storage_volume"`
}

// Question 5: How many accounts/storage slots are only accessed once?
type SingleAccessAnalysis struct {
	AccountsSingleAccess SingleAccessAccountsAnalysis `json:"accounts_single_access"`
	StorageSingleAccess  SingleAccessStorageAnalysis  `json:"storage_single_access"`
}

type SingleAccessAccountsAnalysis struct {
	SingleAccessEOAs         int     `json:"single_access_eoas"`
	SingleAccessContracts    int     `json:"single_access_contracts"`
	TotalSingleAccess        int     `json:"total_single_access"`
	TotalEOAs                int     `json:"total_eoas"`
	TotalContracts           int     `json:"total_contracts"`
	TotalAccounts            int     `json:"total_accounts"`
	SingleAccessEOARate      float64 `json:"single_access_eoa_rate"`
	SingleAccessContractRate float64 `json:"single_access_contract_rate"`
	SingleAccessRate         float64 `json:"single_access_rate"`
}

type SingleAccessStorageAnalysis struct {
	SingleAccessSlots int     `json:"single_access_slots"`
	TotalSlots        int     `json:"total_slots"`
	SingleAccessRate  float64 `json:"single_access_rate"`
}

// Questions 6, 13: Block activity analysis
type BlockActivityAnalysis struct {
	TopActivityBlocks  []BlockActivityInfo `json:"top_activity_blocks"`
	BlockAccessRates   BlockAccessRates    `json:"block_access_rates"`
	ActivityStatistics ActivityStatistics  `json:"activity_statistics"`
	BlockRangeAnalysis []BlockRangeInfo    `json:"block_range_analysis"`
}

type BlockActivityInfo struct {
	BlockNumber        uint64 `json:"block_number"`
	AccountAccesses    int    `json:"account_accesses"`
	StorageAccesses    int    `json:"storage_accesses"`
	TotalAccesses      int    `json:"total_accesses"`
	EOAAccesses        int    `json:"eoa_accesses"`
	ContractAccesses   int    `json:"contract_accesses"`
	UniqueAccounts     int    `json:"unique_accounts"`
	UniqueStorageSlots int    `json:"unique_storage_slots"`
}

type BlockAccessRates struct {
	AccountsPerBlock      float64 `json:"accounts_per_block"`
	StoragePerBlock       float64 `json:"storage_per_block"`
	TotalAccessesPerBlock float64 `json:"total_accesses_per_block"`
	BlocksAnalyzed        int     `json:"blocks_analyzed"`
	AverageBlockInterval  float64 `json:"average_block_interval"`
}

type ActivityStatistics struct {
	MostActiveBlock   uint64  `json:"most_active_block"`
	LeastActiveBlock  uint64  `json:"least_active_block"`
	MaxAccesses       int     `json:"max_accesses"`
	MinAccesses       int     `json:"min_accesses"`
	AverageAccesses   float64 `json:"average_accesses"`
	MedianAccesses    float64 `json:"median_accesses"`
	StandardDeviation float64 `json:"standard_deviation"`
}

type BlockRangeInfo struct {
	RangeStart       uint64  `json:"range_start"`
	RangeEnd         uint64  `json:"range_end"`
	BlockCount       int     `json:"block_count"`
	AccountAccesses  int     `json:"account_accesses"`
	StorageAccesses  int     `json:"storage_accesses"`
	TotalAccesses    int     `json:"total_accesses"`
	AccessesPerBlock float64 `json:"accesses_per_block"`
}

// Questions 12, 14: Time series analysis
type TimeSeriesAnalysis struct {
	AccessTrends      AccessTrendsAnalysis    `json:"access_trends"`
	FrequencyAnalysis FrequencyAnalysisData   `json:"frequency_analysis"`
	TrendStatistics   TrendStatisticsAnalysis `json:"trend_statistics"`
	TimeWindows       []TimeWindowAnalysis    `json:"time_windows"`
}

type AccessTrendsAnalysis struct {
	EOATrend       []TimePoint    `json:"eoa_trend"`
	ContractTrend  []TimePoint    `json:"contract_trend"`
	StorageTrend   []TimePoint    `json:"storage_trend"`
	TotalTrend     []TimePoint    `json:"total_trend"`
	TrendDirection TrendDirection `json:"trend_direction"`
}

type TimePoint struct {
	BlockNumber     uint64  `json:"block_number"`
	Timestamp       uint64  `json:"timestamp,omitempty"`
	AccessCount     int     `json:"access_count"`
	CumulativeCount int     `json:"cumulative_count"`
	GrowthRate      float64 `json:"growth_rate"`
}

type TrendDirection struct {
	EOATrend      string  `json:"eoa_trend"` // "increasing", "decreasing", "stable"
	ContractTrend string  `json:"contract_trend"`
	StorageTrend  string  `json:"storage_trend"`
	TotalTrend    string  `json:"total_trend"`
	OverallGrowth float64 `json:"overall_growth"`
}

type FrequencyAnalysisData struct {
	AccountFrequency AccountFrequencyAnalysis `json:"account_frequency"`
	StorageFrequency StorageFrequencyAnalysis `json:"storage_frequency"`
	OverallFrequency OverallFrequencyAnalysis `json:"overall_frequency"`
}

type AccountFrequencyAnalysis struct {
	AverageAccessFrequency float64                       `json:"average_access_frequency"`
	MedianAccessFrequency  float64                       `json:"median_access_frequency"`
	FrequencyDistribution  []FrequencyDistributionBucket `json:"frequency_distribution"`
	MostFrequentAccounts   []FrequentAccountInfo         `json:"most_frequent_accounts"`
}

type StorageFrequencyAnalysis struct {
	AverageAccessFrequency float64                       `json:"average_access_frequency"`
	MedianAccessFrequency  float64                       `json:"median_access_frequency"`
	FrequencyDistribution  []FrequencyDistributionBucket `json:"frequency_distribution"`
	MostFrequentSlots      []FrequentStorageInfo         `json:"most_frequent_slots"`
}

type OverallFrequencyAnalysis struct {
	TotalUniqueAccounts int     `json:"total_unique_accounts"`
	TotalUniqueSlots    int     `json:"total_unique_slots"`
	TotalAccessEvents   int     `json:"total_access_events"`
	AverageAccountReuse float64 `json:"average_account_reuse"`
	AverageStorageReuse float64 `json:"average_storage_reuse"`
	SystemUtilization   float64 `json:"system_utilization"`
}

type FrequencyDistributionBucket struct {
	FrequencyRange string  `json:"frequency_range"`
	Count          int     `json:"count"`
	Percentage     float64 `json:"percentage"`
}

type FrequentAccountInfo struct {
	Address     string `json:"address"`
	AccessCount int    `json:"access_count"`
	IsContract  bool   `json:"is_contract"`
	FirstAccess uint64 `json:"first_access"`
	LastAccess  uint64 `json:"last_access"`
	AccessSpan  uint64 `json:"access_span"`
}

type FrequentStorageInfo struct {
	Address     string `json:"address"`
	StorageSlot string `json:"storage_slot"`
	AccessCount int    `json:"access_count"`
	FirstAccess uint64 `json:"first_access"`
	LastAccess  uint64 `json:"last_access"`
	AccessSpan  uint64 `json:"access_span"`
}

type TrendStatisticsAnalysis struct {
	GrowthRate       float64 `json:"growth_rate"`
	Volatility       float64 `json:"volatility"`
	SeasonalityIndex float64 `json:"seasonality_index"`
	PeakActivity     uint64  `json:"peak_activity"`
	LowActivity      uint64  `json:"low_activity"`
	TrendStartBlock  uint64  `json:"trend_start_block"`
	TrendEndBlock    uint64  `json:"trend_end_block"`
}

type TimeWindowAnalysis struct {
	WindowStart     uint64  `json:"window_start"`
	WindowEnd       uint64  `json:"window_end"`
	WindowSize      int     `json:"window_size"`
	AccountAccesses int     `json:"account_accesses"`
	StorageAccesses int     `json:"storage_accesses"`
	TotalAccesses   int     `json:"total_accesses"`
	AccessRate      float64 `json:"access_rate"`
	GrowthFromPrev  float64 `json:"growth_from_prev"`
}

// Questions 10, 15: Storage volume analysis
type StorageVolumeAnalysis struct {
	StorageDistribution StorageDistributionAnalysis `json:"storage_distribution"`
	ContractRankings    ContractRankingsAnalysis    `json:"contract_rankings"`
	VolumeStatistics    VolumeStatisticsAnalysis    `json:"volume_statistics"`
	ActivityAnalysis    StorageActivityAnalysis     `json:"activity_analysis"`
}

type StorageDistributionAnalysis struct {
	TotalStorageSlots    int                        `json:"total_storage_slots"`
	ActiveStorageSlots   int                        `json:"active_storage_slots"`
	ExpiredStorageSlots  int                        `json:"expired_storage_slots"`
	VolumeDistribution   []VolumeDistributionBucket `json:"volume_distribution"`
	ContractsWithStorage int                        `json:"contracts_with_storage"`
	ContractsAllActive   int                        `json:"contracts_all_active"`
	ContractsAllExpired  int                        `json:"contracts_all_expired"`
	ContractsMixed       int                        `json:"contracts_mixed"`
}

type VolumeDistributionBucket struct {
	StorageRange string  `json:"storage_range"`
	Count        int     `json:"count"`
	Percentage   float64 `json:"percentage"`
}

type ContractRankingsAnalysis struct {
	TopContractsByTotalSlots   []ContractVolumeInfo `json:"top_contracts_by_total_slots"`
	TopContractsByActiveSlots  []ContractVolumeInfo `json:"top_contracts_by_active_slots"`
	TopContractsByExpiredSlots []ContractVolumeInfo `json:"top_contracts_by_expired_slots"`
	ContractsAllActiveStorage  []ContractVolumeInfo `json:"contracts_all_active_storage"`
}

type ContractVolumeInfo struct {
	Address            string  `json:"address"`
	TotalSlots         int     `json:"total_slots"`
	ActiveSlots        int     `json:"active_slots"`
	ExpiredSlots       int     `json:"expired_slots"`
	ActivityPercentage float64 `json:"activity_percentage"`
	FirstAccess        uint64  `json:"first_access"`
	LastAccess         uint64  `json:"last_access"`
	IsActive           bool    `json:"is_active"`
}

type VolumeStatisticsAnalysis struct {
	AverageStoragePerContract float64 `json:"average_storage_per_contract"`
	MedianStoragePerContract  float64 `json:"median_storage_per_contract"`
	MaxStoragePerContract     int     `json:"max_storage_per_contract"`
	MinStoragePerContract     int     `json:"min_storage_per_contract"`
	StorageConcentration      float64 `json:"storage_concentration"`
	StorageUtilization        float64 `json:"storage_utilization"`
}

type StorageActivityAnalysis struct {
	ActiveStorageRate    float64              `json:"active_storage_rate"`
	StorageReuseRate     float64              `json:"storage_reuse_rate"`
	StorageChurnRate     float64              `json:"storage_churn_rate"`
	AverageStorageAge    float64              `json:"average_storage_age"`
	StorageLifespanStats StorageLifespanStats `json:"storage_lifespan_stats"`
}

type StorageLifespanStats struct {
	AverageLifespan uint64 `json:"average_lifespan"`
	MedianLifespan  uint64 `json:"median_lifespan"`
	MaxLifespan     uint64 `json:"max_lifespan"`
	MinLifespan     uint64 `json:"min_lifespan"`
}

// Question 1: How many accounts are expired (separated by EOA and contract)?
type AccountExpiryAnalysis struct {
	ExpiredEOAs               int     `json:"expired_eoas"`
	ExpiredContracts          int     `json:"expired_contracts"`
	TotalExpiredAccounts      int     `json:"total_expired_accounts"`
	TotalEOAs                 int     `json:"total_eoas"`
	TotalContracts            int     `json:"total_contracts"`
	TotalAccounts             int     `json:"total_accounts"`
	ExpiredEOAPercentage      float64 `json:"expired_eoa_percentage"`
	ExpiredContractPercentage float64 `json:"expired_contract_percentage"`
	TotalExpiredPercentage    float64 `json:"total_expired_percentage"`
}

// Question 2: What percentage of expired accounts are contracts vs EOAs?
type AccountDistributionAnalysis struct {
	ContractPercentage   float64 `json:"contract_percentage"`
	EOAPercentage        float64 `json:"eoa_percentage"`
	TotalExpiredAccounts int     `json:"total_expired_accounts"`
}

// New Question: What percentage of storage slots are expired?
type StorageSlotExpiryAnalysis struct {
	ExpiredSlots          int     `json:"expired_slots"`
	TotalSlots            int     `json:"total_slots"`
	ExpiredSlotPercentage float64 `json:"expired_slot_percentage"`
}

// Question 4: What are the top 10 contracts with the largest expired state footprint?
type ContractStorageAnalysis struct {
	TopExpiredContracts []ExpiredContract `json:"top_expired_contracts"`
}

type ExpiredContract struct {
	Address          string  `json:"address"`
	ExpiredSlotCount int     `json:"expired_slot_count"`
	TotalSlotCount   int     `json:"total_slot_count"`
	ExpiryPercentage float64 `json:"expiry_percentage"`
}

// Question 5: What percentage of a contract's total storage is expired?
// Question 6: How many contracts where all slots are expired?
type StorageExpiryAnalysis struct {
	AverageExpiryPercentage float64                  `json:"average_expiry_percentage"`
	MedianExpiryPercentage  float64                  `json:"median_expiry_percentage"`
	ExpiryDistribution      []ExpiryPercentageBucket `json:"expiry_distribution"`
	ContractsAnalyzed       int                      `json:"contracts_analyzed"`
}

type FullyExpiredContractsAnalysis struct {
	FullyExpiredContractCount int     `json:"fully_expired_contract_count"`
	TotalContractsWithStorage int     `json:"total_contracts_with_storage"`
	FullyExpiredPercentage    float64 `json:"fully_expired_percentage"`
}

type ExpiryPercentageBucket struct {
	RangeStart int `json:"range_start"`
	RangeEnd   int `json:"range_end"`
	Count      int `json:"count"`
}

// Question 8: How many contracts are still active but have expired storage? (Detailed threshold analysis)
type ActiveContractsExpiredStorageAnalysis struct {
	ThresholdAnalysis    []ExpiredStorageThreshold `json:"threshold_analysis"`
	TotalActiveContracts int                       `json:"total_active_contracts"`
}

type ExpiredStorageThreshold struct {
	ThresholdRange     string  `json:"threshold_range"`
	ContractCount      int     `json:"contract_count"`
	PercentageOfActive float64 `json:"percentage_of_active"`
}

// Question 9: How many contracts are fully expired at both account and storage levels?
type CompleteExpiryAnalysis struct {
	FullyExpiredContractCount int     `json:"fully_expired_contract_count"`
	TotalContractsWithStorage int     `json:"total_contracts_with_storage"`
	FullyExpiredPercentage    float64 `json:"fully_expired_percentage"`
}

type SyncStatus struct {
	IsSynced         bool   `json:"is_synced"`
	LastIndexedRange uint64 `json:"last_indexed_range"`
	EndBlock         uint64 `json:"end_block"`
}

// BaseStatistics holds all basic counts that can be derived from a single query
type BaseStatistics struct {
	// Account statistics (derived totals calculated via methods)
	TotalEOAs        int
	TotalContracts   int
	ExpiredEOAs      int
	ExpiredContracts int

	// Storage statistics
	TotalSlots   int
	ExpiredSlots int
}

func (b *BaseStatistics) TotalAccounts() int {
	return b.TotalEOAs + b.TotalContracts
}

func (b *BaseStatistics) ExpiredAccounts() int {
	return b.ExpiredEOAs + b.ExpiredContracts
}
