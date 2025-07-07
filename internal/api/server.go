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
	repo      *repository.StateRepository
	rpcClient *rpc.Client
	rangeSize uint64
	log       *slog.Logger
	server    *http.Server
}

func NewServer(repo *repository.StateRepository, rpcClient *rpc.Client, rangeSize uint64) *Server {
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
		r.Get("/stats/analytics", s.handleGetAnalytics)
		r.Get("/lookup", s.handleStateLookup)
		r.Get("/sync", s.handleGetSyncStatus)
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

func (s *Server) handleStateLookup(w http.ResponseWriter, r *http.Request) {
	address := r.URL.Query().Get("address")
	if address == "" {
		s.log.Warn("Missing address parameter", "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Missing 'address' query parameter")
		return
	}

	slot := r.URL.Query().Get("slot")
	var slotPtr *string
	if slot != "" {
		slotPtr = &slot
	}

	if slotPtr != nil {
		// For storage lookup, just return the access block as before
		lastAccessedBlock, err := s.repo.GetStateLastAccessedBlock(r.Context(), address, slotPtr)
		if err != nil {
			s.log.Error("Failed to get state last access block",
				"error", err,
				"address", address,
				"slot", slot,
				"remote_addr", r.RemoteAddr)
			respondWithError(w, http.StatusInternalServerError, "Could not get state last access block")
			return
		}

		s.log.Debug("Served storage lookup",
			"address", address,
			"slot", slot,
			"last_access_block", lastAccessedBlock,
			"remote_addr", r.RemoteAddr)
		respondWithJSON(w, http.StatusOK, map[string]uint64{"last_access_block": lastAccessedBlock})
		return
	}

	// For account lookup, return full account info including type
	accountInfo, err := s.repo.GetAccountInfo(r.Context(), address)
	if err != nil {
		s.log.Error("Failed to get account info",
			"error", err,
			"address", address,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get account info")
		return
	}

	if accountInfo == nil {
		s.log.Debug("Account not found", "address", address, "remote_addr", r.RemoteAddr)
		respondWithJSON(w, http.StatusOK, map[string]uint64{"last_access_block": 0})
		return
	}

	s.log.Debug("Served account lookup",
		"address", address,
		"last_access_block", accountInfo.LastAccessBlock,
		"is_contract", accountInfo.IsContract,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, accountInfo)
}

func getUint64QueryParam(r *http.Request, key string) (uint64, error) {
	valStr := r.URL.Query().Get(key)
	if valStr == "" {
		return 0, fmt.Errorf("missing query parameter: %s", key)
	}
	return strconv.ParseUint(valStr, 10, 64)
}

func getIntQueryParam(r *http.Request, key string, defaultValue int) (int, error) {
	valStr := r.URL.Query().Get(key)
	if valStr == "" {
		return defaultValue, nil
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return 0, fmt.Errorf("invalid integer parameter: %s", key)
	}
	return val, nil
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

func (s *Server) handleGetAnalytics(w http.ResponseWriter, r *http.Request) {
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
		s.log.Error("Failed to get analytics data",
			"error", err,
			"expiry_block", expiryBlock,
			"current_block", currentBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get analytics data")
		return
	}

	s.log.Debug("Served analytics data",
		"expiry_block", expiryBlock,
		"current_block", currentBlock,
		"expired_accounts", analytics.AccountExpiry.TotalExpiredAccounts,
		"total_accounts", analytics.AccountExpiry.TotalAccounts,
		"expired_slots", analytics.StorageSlotExpiry.ExpiredSlots,
		"total_slots", analytics.StorageSlotExpiry.TotalSlots,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, analytics)
}
