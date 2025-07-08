package indexer

import (
	"context"

	"github.com/weiihann/state-expiry-indexer/internal/repository"
)

var (
	_ StateAccess = &stateAccessLatest{}
	_ StateAccess = &stateAccessArchive{}
)

type StateAccess interface {
	AddAccount(addr string, blockNumber uint64, isContract bool) error
	AddStorage(addr string, slot string, blockNumber uint64)
	Commit(ctx context.Context, repo repository.StateRepositoryInterface, rangeNumber uint64) error
	Reset()
	Count() int
}

type stateAccessLatest struct {
	// For deduplication mode (PostgreSQL) - stores latest access only
	accounts    map[string]uint64
	accountType map[string]bool
	storage     map[string]map[string]uint64

	count int
}

func newStateAccessLatest() *stateAccessLatest {
	return &stateAccessLatest{
		accounts:    make(map[string]uint64),
		accountType: make(map[string]bool),
		storage:     make(map[string]map[string]uint64),
	}
}

func (s *stateAccessLatest) AddAccount(addr string, blockNumber uint64, isContract bool) error {
	if _, ok := s.accounts[addr]; !ok {
		s.count++
	}

	if old, ok := s.accountType[addr]; !ok {
		s.accountType[addr] = isContract
	} else if !old {
		s.accountType[addr] = isContract
	}

	s.accounts[addr] = blockNumber

	return nil
}

func (s *stateAccessLatest) AddStorage(addr string, slot string, blockNumber uint64) {
	if _, ok := s.storage[addr]; !ok {
		s.storage[addr] = make(map[string]uint64)
	}

	if _, exists := s.storage[addr][slot]; !exists {
		s.count++
	}

	s.storage[addr][slot] = blockNumber
}

func (s *stateAccessLatest) Commit(ctx context.Context, repo repository.StateRepositoryInterface, rangeNumber uint64) error {
	return repo.UpdateRangeDataInTx(ctx, s.accounts, s.accountType, s.storage, rangeNumber)
}

func (s *stateAccessLatest) Reset() {
	s.accounts = make(map[string]uint64)
	s.accountType = make(map[string]bool)
	s.storage = make(map[string]map[string]uint64)
	s.count = 0
}

func (s *stateAccessLatest) Count() int {
	return s.count
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

	if old, ok := s.accountType[addr]; !ok {
		s.accountType[addr] = isContract
	} else if !old {
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
	return repo.UpdateRangeDataWithAllEventsInTx(ctx, s.accountsByBlock, s.accountType, s.storageByBlock, rangeNumber)
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
