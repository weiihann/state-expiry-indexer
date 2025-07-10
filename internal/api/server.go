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

		// Advanced analytics endpoints
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

	analytics, err := s.repo.GetAnalyticsData(r.Context(), expiryBlock, currentBlock)
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

	analytics, err := s.repo.GetSingleAccessAnalytics(r.Context(), expiryBlock, currentBlock)
	if err != nil {
		if advancedErr, ok := err.(*repository.AdvancedAnalyticsError); ok {
			s.log.Warn("Advanced analytics not supported",
				"operation", advancedErr.Operation,
				"database", advancedErr.Database,
				"remote_addr", r.RemoteAddr)
			respondWithError(w, http.StatusNotImplemented, advancedErr.Error())
			return
		}

		s.log.Error("Failed to get single access analytics",
			"error", err,
			"expiry_block", expiryBlock,
			"current_block", currentBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get single access analytics")
		return
	}

	s.log.Debug("Served single access analytics",
		"expiry_block", expiryBlock,
		"current_block", currentBlock,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}

func (s *Server) handleGetBlockActivityAnalytics(w http.ResponseWriter, r *http.Request) {
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

	analytics, err := s.repo.GetBlockActivityAnalytics(r.Context(), startBlock, endBlock, topN)
	if err != nil {
		// Check if this is an advanced analytics error (unsupported in PostgreSQL)
		if advancedErr, ok := err.(*repository.AdvancedAnalyticsError); ok {
			s.log.Warn("Advanced analytics not supported",
				"operation", advancedErr.Operation,
				"database", advancedErr.Database,
				"remote_addr", r.RemoteAddr)
			respondWithError(w, http.StatusNotImplemented, advancedErr.Error())
			return
		}

		s.log.Error("Failed to get block activity analytics",
			"error", err,
			"start_block", startBlock,
			"end_block", endBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get block activity analytics")
		return
	}

	s.log.Debug("Served block activity analytics",
		"start_block", startBlock,
		"end_block", endBlock,
		"top_n", topN,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}

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

	analytics, err := s.repo.GetTimeSeriesAnalytics(r.Context(), startBlock, endBlock, windowSize)
	if err != nil {
		// Check if this is an advanced analytics error (unsupported in PostgreSQL)
		if advancedErr, ok := err.(*repository.AdvancedAnalyticsError); ok {
			s.log.Warn("Advanced analytics not supported",
				"operation", advancedErr.Operation,
				"database", advancedErr.Database,
				"remote_addr", r.RemoteAddr)
			respondWithError(w, http.StatusNotImplemented, advancedErr.Error())
			return
		}

		s.log.Error("Failed to get time series analytics",
			"error", err,
			"start_block", startBlock,
			"end_block", endBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get time series analytics")
		return
	}

	s.log.Debug("Served time series analytics",
		"start_block", startBlock,
		"end_block", endBlock,
		"window_size", windowSize,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
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

	analytics, err := s.repo.GetStorageVolumeAnalytics(r.Context(), expiryBlock, currentBlock, topN)
	if err != nil {
		// Check if this is an advanced analytics error (unsupported in PostgreSQL)
		if advancedErr, ok := err.(*repository.AdvancedAnalyticsError); ok {
			s.log.Warn("Advanced analytics not supported",
				"operation", advancedErr.Operation,
				"database", advancedErr.Database,
				"remote_addr", r.RemoteAddr)
			respondWithError(w, http.StatusNotImplemented, advancedErr.Error())
			return
		}

		s.log.Error("Failed to get storage volume analytics",
			"error", err,
			"expiry_block", expiryBlock,
			"current_block", currentBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get storage volume analytics")
		return
	}

	s.log.Debug("Served storage volume analytics",
		"expiry_block", expiryBlock,
		"current_block", currentBlock,
		"top_n", topN,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}
