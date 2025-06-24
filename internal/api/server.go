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
)

type Server struct {
	repo   *repository.StateRepository
	log    *slog.Logger
	server *http.Server
}

func NewServer(repo *repository.StateRepository) *Server {
	return &Server{
		repo: repo,
		log:  logger.GetLogger("api-server"),
	}
}

func (s *Server) Run(ctx context.Context, host string, port int) error {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("State Expiry API"))
	})

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/stats/expired-count", s.handleGetExpiredStateCount)
		r.Get("/stats/top-expired-contracts", s.handleGetTopNExpiredContracts)
		r.Get("/lookup", s.handleStateLookup)
		r.Get("/account-type", s.handleGetAccountType)
		r.Get("/accounts", s.handleGetAccounts)
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

func (s *Server) handleGetExpiredStateCount(w http.ResponseWriter, r *http.Request) {
	expiryBlock, err := getUint64QueryParam(r, "expiry_block")
	if err != nil {
		s.log.Warn("Invalid expiry_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'expiry_block' query parameter")
		return
	}

	count, err := s.repo.GetExpiredStateCount(r.Context(), expiryBlock)
	if err != nil {
		s.log.Error("Failed to get expired state count",
			"error", err,
			"expiry_block", expiryBlock,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get expired state count")
		return
	}

	s.log.Debug("Served expired state count",
		"expiry_block", expiryBlock,
		"count", count,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, map[string]int{"expired_state_count": count})
}

func (s *Server) handleGetTopNExpiredContracts(w http.ResponseWriter, r *http.Request) {
	expiryBlock, err := getUint64QueryParam(r, "expiry_block")
	if err != nil {
		s.log.Warn("Invalid expiry_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'expiry_block' query parameter")
		return
	}

	n, err := getIntQueryParam(r, "n", 10)
	if err != nil {
		s.log.Warn("Invalid n parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'n' query parameter")
		return
	}

	contracts, err := s.repo.GetTopNExpiredContracts(r.Context(), expiryBlock, n)
	if err != nil {
		s.log.Error("Failed to get top N expired contracts",
			"error", err,
			"expiry_block", expiryBlock,
			"n", n,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get top N expired contracts")
		return
	}

	s.log.Debug("Served top expired contracts",
		"expiry_block", expiryBlock,
		"n", n,
		"contract_count", len(contracts),
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, contracts)
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

func (s *Server) handleGetAccountType(w http.ResponseWriter, r *http.Request) {
	address := r.URL.Query().Get("address")
	if address == "" {
		s.log.Warn("Missing address parameter", "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Missing 'address' query parameter")
		return
	}

	isContract, err := s.repo.GetAccountType(r.Context(), address)
	if err != nil {
		s.log.Error("Failed to get account type",
			"error", err,
			"address", address,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get account type")
		return
	}

	if isContract == nil {
		s.log.Debug("Account not found", "address", address, "remote_addr", r.RemoteAddr)
		respondWithJSON(w, http.StatusNotFound, map[string]string{"error": "Account not found"})
		return
	}

	accountType := "eoa"
	if *isContract {
		accountType = "contract"
	}

	s.log.Debug("Served account type lookup",
		"address", address,
		"account_type", accountType,
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, map[string]string{
		"address":      address,
		"account_type": accountType,
	})
}

func (s *Server) handleGetAccounts(w http.ResponseWriter, r *http.Request) {
	expiryBlock, err := getUint64QueryParam(r, "expiry_block")
	if err != nil {
		s.log.Warn("Invalid expiry_block parameter", "error", err, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'expiry_block' query parameter")
		return
	}

	// Parse account type filter (optional)
	var isContractFilter *bool
	accountTypeStr := r.URL.Query().Get("account_type")
	if accountTypeStr != "" {
		switch accountTypeStr {
		case "contract":
			isContract := true
			isContractFilter = &isContract
		case "eoa":
			isContract := false
			isContractFilter = &isContract
		case "all":
			// Leave as nil to get all accounts
		default:
			s.log.Warn("Invalid account_type parameter", "account_type", accountTypeStr, "remote_addr", r.RemoteAddr)
			respondWithError(w, http.StatusBadRequest, "Invalid 'account_type' parameter. Must be 'contract', 'eoa', or 'all'")
			return
		}
	}

	limit, err := getIntQueryParam(r, "limit", 100)
	if err != nil || limit <= 0 || limit > 1000 {
		s.log.Warn("Invalid limit parameter", "error", err, "limit", limit, "remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusBadRequest, "Invalid 'limit' parameter. Must be between 1 and 1000")
		return
	}

	accounts, err := s.repo.GetExpiredAccountsByType(r.Context(), expiryBlock, isContractFilter)
	if err != nil {
		s.log.Error("Failed to get expired accounts by type",
			"error", err,
			"expiry_block", expiryBlock,
			"account_type_filter", accountTypeStr,
			"remote_addr", r.RemoteAddr)
		respondWithError(w, http.StatusInternalServerError, "Could not get expired accounts")
		return
	}

	// Apply limit
	if len(accounts) > limit {
		accounts = accounts[:limit]
	}

	s.log.Debug("Served expired accounts by type",
		"expiry_block", expiryBlock,
		"account_type_filter", accountTypeStr,
		"returned_count", len(accounts),
		"remote_addr", r.RemoteAddr)
	respondWithJSON(w, http.StatusOK, accounts)
}
