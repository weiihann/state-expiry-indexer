package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/weiihann/state-expiry-indexer/internal/logger"
	"github.com/weiihann/state-expiry-indexer/internal/repository"
	"github.com/weiihann/state-expiry-indexer/pkg/rpc"
)

type Server struct {
	repo      repository.StateRepositoryInterface
	rpcClient *rpc.Client
	rangeSize uint64
	log       *slog.Logger
	server    *http.Server
}

func NewServer(repo repository.StateRepositoryInterface, rpcClient *rpc.Client, rangeSize uint64) *Server {
	return &Server{
		repo:      repo,
		rpcClient: rpcClient,
		rangeSize: rangeSize,
		log:       logger.GetLogger("api-server"),
	}
}

func (s *Server) Run(ctx context.Context, host string, port int) error {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("State Expiry API"))
	})

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/sync", s.handleGetSyncStatus)

		// Optimized analytics endpoints grouped by question categories
		r.Route("/accounts", func(r chi.Router) {
			r.Get("/", s.handleGetAccountAnalytics) // Questions 1, 2, 5a
		})

		r.Route("/storage", func(r chi.Router) {
			r.Get("/", s.handleGetStorageAnalytics) // Questions 3, 4, 5b
		})

		r.Route("/contracts", func(r chi.Router) {
			r.Get("/", s.handleGetContractAnalytics)              // Questions 7, 8, 9, 10, 11, 15
			r.Get("/top-expired", s.handleGetTopExpiredContracts) // Question 7
			r.Get("/top-volume", s.handleGetTopVolumeContracts)   // Question 15
		})

		r.Route("/activity", func(r chi.Router) {
			r.Get("/", s.handleGetBlockActivityAnalytics)  // Questions 6, 12, 13, 14
			r.Get("/blocks", s.handleGetTopActivityBlocks) // Question 6
			r.Get("/trends", s.handleGetTrendAnalysis)     // Questions 12, 14
		})

		// Unified endpoint returning all analytics
		r.Get("/stats", s.handleGetUnifiedAnalytics) // All Questions 1-15

		// Quick overview endpoint
		r.Get("/overview", s.handleGetBasicStats) // Basic statistics

		// Legacy endpoints (for backward compatibility)
		r.Route("/analytics", func(r chi.Router) {
			r.Get("/extended", s.handleGetExtendedAnalytics)
			r.Get("/single-access", s.handleGetSingleAccessAnalytics)
			r.Get("/block-activity", s.handleGetBlockActivityAnalytics)
			r.Get("/time-series", s.handleGetTimeSeriesAnalytics)
			r.Get("/storage-volume", s.handleGetStorageVolumeAnalytics)
		})
	})

	s.server = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", host, port),
		Handler: r,
	}

	s.log.Info("Starting API server", "host", host, "port", port, "address", s.server.Addr)

	// Start server in a goroutine
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("API server listen error", "error", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown with timeout
	s.log.Info("Shutting down API server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.server.Shutdown(shutdownCtx); err != nil {
		s.log.Error("API server shutdown error", "error", err)
		return err
	}

	s.log.Info("API server stopped gracefully")
	return nil
}

func getUint64QueryParam(r *http.Request, key string) (uint64, error) {
	valStr := r.URL.Query().Get(key)
	if valStr == "" {
		return 0, fmt.Errorf("missing query parameter: %s", key)
	}
	return strconv.ParseUint(valStr, 10, 64)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload any) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func (s *Server) handleGetSyncStatus(w http.ResponseWriter, r *http.Request) {
	// Get the latest block number from the RPC client
	latestBlockBig, err := s.rpcClient.GetLatestBlockNumber(r.Context())
	if err != nil {
		s.log.Error("Failed to get latest block number from RPC",
			"error", err,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get latest block number")
		return
	}

	latestBlock := latestBlockBig.Uint64()

	// Calculate the latest range number
	// Range calculation: for block 0 (genesis) = range 0, for others = (blockNumber - 1) / rangeSize
	var latestRange uint64
	if latestBlock == 0 {
		latestRange = 0
	} else {
		latestRange = (latestBlock-1)/s.rangeSize + 1
	}

	// Get sync status from repository
	syncStatus, err := s.repo.GetSyncStatus(r.Context(), latestRange, s.rangeSize)
	if err != nil {
		s.log.Error("Failed to get sync status",
			"error", err,
			"latest_range", latestRange,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get sync status")
		return
	}

	s.log.Debug("Served sync status",
		"latest_block", latestBlock,
		"latest_range", latestRange,
		"last_indexed_range", syncStatus.LastIndexedRange,
		"is_synced", syncStatus.IsSynced,
		"end_block", syncStatus.EndBlock,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, syncStatus)
}

// Advanced analytics handlers

func (s *Server) handleGetExtendedAnalytics(w http.ResponseWriter, r *http.Request) {
	expiryBlock, err := getUint64QueryParam(r, "expiry_block")
	if err != nil {
		s.log.Warn("Invalid expiry_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'expiry_block' query parameter")
		return
	}

	// Get the latest block number from the RPC client
	latestBlockBig, err := s.rpcClient.GetLatestBlockNumber(r.Context())
	if err != nil {
		s.log.Error("Failed to get latest block number from RPC",
			"error", err,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get latest block number")
		return
	}

	currentBlock := latestBlockBig.Uint64()

	// Use new unified analytics method for backward compatibility
	params := repository.QueryParams{
		ExpiryBlock:  expiryBlock,
		CurrentBlock: currentBlock,
		TopN:         10,
		WindowSize:   1000,
		StartBlock:   expiryBlock - 10000,
		EndBlock:     currentBlock,
	}

	analytics, err := s.repo.GetUnifiedAnalytics(r.Context(), params)
	if err != nil {
		s.log.Error("Failed to get extended analytics data",
			"error", err,
			"expiry_block", expiryBlock,
			"current_block", currentBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get extended analytics data")
		return
	}

	s.log.Debug("Served extended analytics data",
		"expiry_block", expiryBlock,
		"current_block", currentBlock,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}

func (s *Server) handleGetSingleAccessAnalytics(w http.ResponseWriter, r *http.Request) {
	expiryBlock, err := getUint64QueryParam(r, "expiry_block")
	if err != nil {
		s.log.Warn("Invalid expiry_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'expiry_block' query parameter")
		return
	}

	// Get the latest block number from the RPC client
	latestBlockBig, err := s.rpcClient.GetLatestBlockNumber(r.Context())
	if err != nil {
		s.log.Error("Failed to get latest block number from RPC",
			"error", err,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get latest block number")
		return
	}

	currentBlock := latestBlockBig.Uint64()

	// Use new analytics methods for backward compatibility
	params := repository.QueryParams{
		ExpiryBlock:  expiryBlock,
		CurrentBlock: currentBlock,
		TopN:         10,
	}

	accountAnalytics, err := s.repo.GetAccountAnalytics(r.Context(), params)
	if err != nil {
		s.log.Error("Failed to get single access analytics",
			"error", err,
			"expiry_block", expiryBlock,
			"current_block", currentBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get single access analytics")
		return
	}

	storageAnalytics, err := s.repo.GetStorageAnalytics(r.Context(), params)
	if err != nil {
		s.log.Error("Failed to get single access analytics",
			"error", err,
			"expiry_block", expiryBlock,
			"current_block", currentBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get single access analytics")
		return
	}

	// Return single access data in legacy format
	response := map[string]interface{}{
		"accounts_single_access": accountAnalytics.SingleAccess,
		"storage_single_access":  storageAnalytics.SingleAccess,
	}

	s.log.Debug("Served single access analytics",
		"expiry_block", expiryBlock,
		"current_block", currentBlock,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, response)
}

// Legacy method removed - using new optimized version

func (s *Server) handleGetTimeSeriesAnalytics(w http.ResponseWriter, r *http.Request) {
	startBlock, err := getUint64QueryParam(r, "start_block")
	if err != nil {
		s.log.Warn("Invalid start_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'start_block' query parameter")
		return
	}

	endBlock, err := getUint64QueryParam(r, "end_block")
	if err != nil {
		s.log.Warn("Invalid end_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'end_block' query parameter")
		return
	}

	// Optional window_size parameter with default value
	windowSize := 1000
	if windowSizeStr := r.URL.Query().Get("window_size"); windowSizeStr != "" {
		if windowSizeVal, err := strconv.Atoi(windowSizeStr); err == nil && windowSizeVal > 0 {
			windowSize = windowSizeVal
		}
	}

	// Use new time series methods for backward compatibility
	timeSeries, err := s.repo.GetTimeSeriesData(r.Context(), startBlock, endBlock, windowSize)
	if err != nil {
		s.log.Error("Failed to get time series analytics",
			"error", err,
			"start_block", startBlock,
			"end_block", endBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get time series analytics")
		return
	}

	accessRates, err := s.repo.GetAccessRates(r.Context(), startBlock, endBlock)
	if err != nil {
		s.log.Error("Failed to get access rates",
			"error", err,
			"start_block", startBlock,
			"end_block", endBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get access rates")
		return
	}

	// Return time series data in legacy format
	response := map[string]interface{}{
		"time_series_data": timeSeries,
		"access_rates":     accessRates,
	}

	s.log.Debug("Served time series analytics",
		"start_block", startBlock,
		"end_block", endBlock,
		"window_size", windowSize,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetStorageVolumeAnalytics(w http.ResponseWriter, r *http.Request) {
	expiryBlock, err := getUint64QueryParam(r, "expiry_block")
	if err != nil {
		s.log.Warn("Invalid expiry_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'expiry_block' query parameter")
		return
	}

	// Get the latest block number from the RPC client
	latestBlockBig, err := s.rpcClient.GetLatestBlockNumber(r.Context())
	if err != nil {
		s.log.Error("Failed to get latest block number from RPC",
			"error", err,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get latest block number")
		return
	}

	currentBlock := latestBlockBig.Uint64()

	// Optional top_n parameter with default value
	topN := 10
	if topNStr := r.URL.Query().Get("top_n"); topNStr != "" {
		if topNVal, err := strconv.Atoi(topNStr); err == nil && topNVal > 0 {
			topN = topNVal
		}
	}

	// Use new contract analytics for backward compatibility
	params := repository.QueryParams{
		ExpiryBlock:  expiryBlock,
		CurrentBlock: currentBlock,
		TopN:         topN,
	}

	contractAnalytics, err := s.repo.GetContractAnalytics(r.Context(), params)
	if err != nil {
		s.log.Error("Failed to get storage volume analytics",
			"error", err,
			"expiry_block", expiryBlock,
			"current_block", currentBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get storage volume analytics")
		return
	}

	// Return storage volume data in legacy format
	response := map[string]interface{}{
		"contract_rankings": contractAnalytics.Rankings,
		"volume_analysis":   contractAnalytics.VolumeAnalysis,
		"status_analysis":   contractAnalytics.StatusAnalysis,
	}

	s.log.Debug("Served storage volume analytics",
		"expiry_block", expiryBlock,
		"current_block", currentBlock,
		"top_n", topN,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, response)
}

// ==============================================================================
// NEW OPTIMIZED API HANDLERS (Questions 1-15)
// ==============================================================================

// parseQueryParams extracts common query parameters and returns QueryParams
func (s *Server) parseQueryParams(r *http.Request) (repository.QueryParams, error) {
	params := repository.DefaultQueryParams()

	// Parse expiry_block (required for most endpoints)
	if expiryBlockStr := r.URL.Query().Get("expiry_block"); expiryBlockStr != "" {
		expiryBlock, err := strconv.ParseUint(expiryBlockStr, 10, 64)
		if err != nil {
			return params, fmt.Errorf("invalid expiry_block parameter: %w", err)
		}
		params.ExpiryBlock = expiryBlock
	}

	// Parse start_block and end_block for range queries
	if startBlockStr := r.URL.Query().Get("start_block"); startBlockStr != "" {
		startBlock, err := strconv.ParseUint(startBlockStr, 10, 64)
		if err != nil {
			return params, fmt.Errorf("invalid start_block parameter: %w", err)
		}
		params.StartBlock = startBlock
	}

	if endBlockStr := r.URL.Query().Get("end_block"); endBlockStr != "" {
		endBlock, err := strconv.ParseUint(endBlockStr, 10, 64)
		if err != nil {
			return params, fmt.Errorf("invalid end_block parameter: %w", err)
		}
		params.EndBlock = endBlock
	}

	// Parse optional parameters
	if topNStr := r.URL.Query().Get("top_n"); topNStr != "" {
		if topN, err := strconv.Atoi(topNStr); err == nil && topN > 0 {
			params.TopN = topN
		}
	}

	if windowSizeStr := r.URL.Query().Get("window_size"); windowSizeStr != "" {
		if windowSize, err := strconv.Atoi(windowSizeStr); err == nil && windowSize > 0 {
			params.WindowSize = windowSize
		}
	}

	// Get current block from RPC client
	if params.ExpiryBlock > 0 {
		latestBlockBig, err := s.rpcClient.GetLatestBlockNumber(r.Context())
		if err != nil {
			return params, fmt.Errorf("failed to get latest block number: %w", err)
		}
		params.CurrentBlock = latestBlockBig.Uint64()
	}

	return params, nil
}

// handleGetAccountAnalytics - Questions 1, 2, 5a
func (s *Server) handleGetAccountAnalytics(w http.ResponseWriter, r *http.Request) {
	params, err := s.parseQueryParams(r)
	if err != nil {
		s.log.Warn("Invalid query parameters", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	if params.ExpiryBlock == 0 {
		s.log.Warn("Missing expiry_block parameter", "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Missing required 'expiry_block' query parameter")
		return
	}

	analytics, err := s.repo.GetAccountAnalytics(r.Context(), params)
	if err != nil {
		s.log.Error("Failed to get account analytics",
			"error", err,
			"expiry_block", params.ExpiryBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get account analytics")
		return
	}

	s.log.Debug("Served account analytics",
		"expiry_block", params.ExpiryBlock,
		"total_accounts", analytics.Total.Total,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}

// handleGetStorageAnalytics - Questions 3, 4, 5b
func (s *Server) handleGetStorageAnalytics(w http.ResponseWriter, r *http.Request) {
	params, err := s.parseQueryParams(r)
	if err != nil {
		s.log.Warn("Invalid query parameters", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	if params.ExpiryBlock == 0 {
		s.log.Warn("Missing expiry_block parameter", "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Missing required 'expiry_block' query parameter")
		return
	}

	analytics, err := s.repo.GetStorageAnalytics(r.Context(), params)
	if err != nil {
		s.log.Error("Failed to get storage analytics",
			"error", err,
			"expiry_block", params.ExpiryBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get storage analytics")
		return
	}

	s.log.Debug("Served storage analytics",
		"expiry_block", params.ExpiryBlock,
		"total_slots", analytics.Total.TotalSlots,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}

// handleGetContractAnalytics - Questions 7, 8, 9, 10, 11, 15
func (s *Server) handleGetContractAnalytics(w http.ResponseWriter, r *http.Request) {
	params, err := s.parseQueryParams(r)
	if err != nil {
		s.log.Warn("Invalid query parameters", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	if params.ExpiryBlock == 0 {
		s.log.Warn("Missing expiry_block parameter", "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Missing required 'expiry_block' query parameter")
		return
	}

	analytics, err := s.repo.GetContractAnalytics(r.Context(), params)
	if err != nil {
		s.log.Error("Failed to get contract analytics",
			"error", err,
			"expiry_block", params.ExpiryBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get contract analytics")
		return
	}

	s.log.Debug("Served contract analytics",
		"expiry_block", params.ExpiryBlock,
		"top_expired_count", len(analytics.Rankings.TopByExpiredSlots),
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}

// handleGetBlockActivityAnalytics - Questions 6, 12, 13, 14
func (s *Server) handleGetBlockActivityAnalytics(w http.ResponseWriter, r *http.Request) {
	params, err := s.parseQueryParams(r)
	if err != nil {
		s.log.Warn("Invalid query parameters", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	if params.StartBlock == 0 || params.EndBlock == 0 {
		s.log.Warn("Missing start_block or end_block parameters", "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Missing required 'start_block' and 'end_block' query parameters")
		return
	}

	analytics, err := s.repo.GetBlockActivityAnalytics(r.Context(), params)
	if err != nil {
		s.log.Error("Failed to get block activity analytics",
			"error", err,
			"start_block", params.StartBlock,
			"end_block", params.EndBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get block activity analytics")
		return
	}

	s.log.Debug("Served block activity analytics",
		"start_block", params.StartBlock,
		"end_block", params.EndBlock,
		"top_blocks_count", len(analytics.TopBlocks),
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}

// handleGetUnifiedAnalytics - All Questions 1-15
func (s *Server) handleGetUnifiedAnalytics(w http.ResponseWriter, r *http.Request) {
	params, err := s.parseQueryParams(r)
	if err != nil {
		s.log.Warn("Invalid query parameters", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	if params.ExpiryBlock == 0 {
		s.log.Warn("Missing expiry_block parameter", "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Missing required 'expiry_block' query parameter")
		return
	}

	// Set default range for block activity if not provided
	if params.StartBlock == 0 {
		params.StartBlock = params.ExpiryBlock - 10000 // Default to 10k blocks before expiry
	}
	if params.EndBlock == 0 {
		params.EndBlock = params.CurrentBlock
	}

	analytics, err := s.repo.GetUnifiedAnalytics(r.Context(), params)
	if err != nil {
		s.log.Error("Failed to get unified analytics",
			"error", err,
			"expiry_block", params.ExpiryBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get unified analytics")
		return
	}

	s.log.Debug("Served unified analytics",
		"expiry_block", params.ExpiryBlock,
		"current_block", params.CurrentBlock,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}

// handleGetBasicStats - Quick overview
func (s *Server) handleGetBasicStats(w http.ResponseWriter, r *http.Request) {
	expiryBlock, err := getUint64QueryParam(r, "expiry_block")
	if err != nil {
		s.log.Warn("Invalid expiry_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'expiry_block' query parameter")
		return
	}

	// Get the latest block number from the RPC client
	latestBlockBig, err := s.rpcClient.GetLatestBlockNumber(r.Context())
	if err != nil {
		s.log.Error("Failed to get latest block number from RPC",
			"error", err,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get latest block number")
		return
	}

	stats, err := s.repo.GetBasicStats(r.Context(), expiryBlock)
	if err != nil {
		s.log.Error("Failed to get basic stats",
			"error", err,
			"expiry_block", expiryBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get basic stats")
		return
	}

	// Fill in current block
	stats.Metadata.CurrentBlock = latestBlockBig.Uint64()

	s.log.Debug("Served basic stats",
		"expiry_block", expiryBlock,
		"total_accounts", stats.Accounts.TotalEOAs+stats.Accounts.TotalContracts,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, stats)
}

// ==============================================================================
// SPECIALIZED ENDPOINT HANDLERS
// ==============================================================================

// handleGetTopExpiredContracts - Question 7 specific endpoint
func (s *Server) handleGetTopExpiredContracts(w http.ResponseWriter, r *http.Request) {
	expiryBlock, err := getUint64QueryParam(r, "expiry_block")
	if err != nil {
		s.log.Warn("Invalid expiry_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'expiry_block' query parameter")
		return
	}

	// Optional top_n parameter with default value
	topN := 10
	if topNStr := r.URL.Query().Get("top_n"); topNStr != "" {
		if topNVal, err := strconv.Atoi(topNStr); err == nil && topNVal > 0 {
			topN = topNVal
		}
	}

	contracts, err := s.repo.GetTopContractsByExpiredSlots(r.Context(), expiryBlock, topN)
	if err != nil {
		s.log.Error("Failed to get top expired contracts",
			"error", err,
			"expiry_block", expiryBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get top expired contracts")
		return
	}

	s.log.Debug("Served top expired contracts",
		"expiry_block", expiryBlock,
		"top_n", topN,
		"count", len(contracts),
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"top_expired_contracts": contracts,
		"expiry_block":          expiryBlock,
		"limit":                 topN,
	})
}

// handleGetTopVolumeContracts - Question 15 specific endpoint
func (s *Server) handleGetTopVolumeContracts(w http.ResponseWriter, r *http.Request) {
	// Optional top_n parameter with default value
	topN := 10
	if topNStr := r.URL.Query().Get("top_n"); topNStr != "" {
		if topNVal, err := strconv.Atoi(topNStr); err == nil && topNVal > 0 {
			topN = topNVal
		}
	}

	contracts, err := s.repo.GetTopContractsByTotalSlots(r.Context(), topN)
	if err != nil {
		s.log.Error("Failed to get top volume contracts",
			"error", err,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get top volume contracts")
		return
	}

	s.log.Debug("Served top volume contracts",
		"top_n", topN,
		"count", len(contracts),
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"top_volume_contracts": contracts,
		"limit":                topN,
	})
}

// handleGetTopActivityBlocks - Question 6 specific endpoint
func (s *Server) handleGetTopActivityBlocks(w http.ResponseWriter, r *http.Request) {
	startBlock, err := getUint64QueryParam(r, "start_block")
	if err != nil {
		s.log.Warn("Invalid start_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'start_block' query parameter")
		return
	}

	endBlock, err := getUint64QueryParam(r, "end_block")
	if err != nil {
		s.log.Warn("Invalid end_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'end_block' query parameter")
		return
	}

	// Optional top_n parameter with default value
	topN := 10
	if topNStr := r.URL.Query().Get("top_n"); topNStr != "" {
		if topNVal, err := strconv.Atoi(topNStr); err == nil && topNVal > 0 {
			topN = topNVal
		}
	}

	blocks, err := s.repo.GetTopActivityBlocks(r.Context(), startBlock, endBlock, topN)
	if err != nil {
		s.log.Error("Failed to get top activity blocks",
			"error", err,
			"start_block", startBlock,
			"end_block", endBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get top activity blocks")
		return
	}

	s.log.Debug("Served top activity blocks",
		"start_block", startBlock,
		"end_block", endBlock,
		"top_n", topN,
		"count", len(blocks),
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"top_activity_blocks": blocks,
		"start_block":         startBlock,
		"end_block":           endBlock,
		"limit":               topN,
	})
}

// handleGetTrendAnalysis - Questions 12, 14 specific endpoint
func (s *Server) handleGetTrendAnalysis(w http.ResponseWriter, r *http.Request) {
	startBlock, err := getUint64QueryParam(r, "start_block")
	if err != nil {
		s.log.Warn("Invalid start_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'start_block' query parameter")
		return
	}

	endBlock, err := getUint64QueryParam(r, "end_block")
	if err != nil {
		s.log.Warn("Invalid end_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'end_block' query parameter")
		return
	}

	trend, err := s.repo.GetTrendAnalysis(r.Context(), startBlock, endBlock)
	if err != nil {
		s.log.Error("Failed to get trend analysis",
			"error", err,
			"start_block", startBlock,
			"end_block", endBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get trend analysis")
		return
	}

	s.log.Debug("Served trend analysis",
		"start_block", startBlock,
		"end_block", endBlock,
		"trend", trend.TrendDirection,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, map[string]interface{}{
		"trend_analysis": trend,
		"start_block":    startBlock,
		"end_block":      endBlock,
	})
}
