package indexer

import (
	"context"

	"github.com/weiihann/state-expiry-indexer/internal/repository"
)

var _ StateAccess = &stateAccessArchive{}

type StateAccess interface {
	AddAccount(addr string, blockNumber uint64, isContract bool) error
	AddStorage(addr string, slot string, blockNumber uint64)
	Commit(ctx context.Context, repo repository.StateRepositoryInterface, rangeNumber uint64) error
	Reset()
	Count() int
}

type stateAccessArchive struct {
	accountsByBlock map[uint64]map[string]struct{}
	accountType     map[string]bool
	storageByBlock  map[uint64]map[string]map[string]struct{}

	count int
}

func newStateAccessArchive() *stateAccessArchive {
	return &stateAccessArchive{
		accountsByBlock: make(map[uint64]map[string]struct{}),
		accountType:     make(map[string]bool),
		storageByBlock:  make(map[uint64]map[string]map[string]struct{}),
	}
}

func (s *stateAccessArchive) AddAccount(addr string, blockNumber uint64, isContract bool) error {
	if _, exists := s.accountsByBlock[blockNumber]; !exists {
		s.accountsByBlock[blockNumber] = make(map[string]struct{})
	}

	// If the account is not in the map, add it
	if old, ok := s.accountType[addr]; !ok {
		s.accountType[addr] = isContract
	} else if !old { // If it exists, update it if it's a contract
		s.accountType[addr] = isContract
	}

	if _, exists := s.accountsByBlock[blockNumber][addr]; !exists {
		s.count++
	}

	s.accountsByBlock[blockNumber][addr] = struct{}{}

	return nil
}

func (s *stateAccessArchive) AddStorage(addr string, slot string, blockNumber uint64) {
	if _, exists := s.storageByBlock[blockNumber]; !exists {
		s.storageByBlock[blockNumber] = make(map[string]map[string]struct{})
	}
	if _, exists := s.storageByBlock[blockNumber][addr]; !exists {
		s.storageByBlock[blockNumber][addr] = make(map[string]struct{})
	}

	if _, exists := s.storageByBlock[blockNumber][addr][slot]; !exists {
		s.count++
	}

	s.storageByBlock[blockNumber][addr][slot] = struct{}{}
}

func (s *stateAccessArchive) Commit(ctx context.Context, repo repository.StateRepositoryInterface, rangeNumber uint64) error {
	return repo.InsertRange(ctx, s.accountsByBlock, s.accountType, s.storageByBlock, rangeNumber)
}

func (s *stateAccessArchive) Reset() {
	s.accountsByBlock = make(map[uint64]map[string]struct{})
	s.accountType = make(map[string]bool)
	s.storageByBlock = make(map[uint64]map[string]map[string]struct{})
	s.count = 0
}

func (s *stateAccessArchive) Count() int {
	return s.count
}
