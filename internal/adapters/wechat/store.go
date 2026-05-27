package wechat

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Store persists WeChat credentials and sync tokens on disk.
type Store struct {
	dir string
	mu  sync.RWMutex
}

// NewStore creates a new file-based credential store.
func NewStore(dir string) *Store {
	return &Store{dir: dir}
}

func (s *Store) Init() error {
	return os.MkdirAll(s.accountsDir(), 0700)
}

func (s *Store) accountsDir() string {
	return filepath.Join(s.dir, "accounts")
}

func (s *Store) SaveCredentials(cred *Credentials) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := s.accountsDir()
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}

	path := filepath.Join(dir, cred.AccountID+".json")
	data, err := json.MarshalIndent(cred, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

func (s *Store) LoadCredentials(accountID string) (*Credentials, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := filepath.Join(s.accountsDir(), accountID+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cred Credentials
	if err := json.Unmarshal(data, &cred); err != nil {
		return nil, err
	}
	return &cred, nil
}

func (s *Store) ListAccounts() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dir := s.accountsDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var accounts []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			accounts = append(accounts, e.Name()[:len(e.Name())-5])
		}
	}
	return accounts, nil
}

func (s *Store) SaveSyncToken(accountID string, syncToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := s.accountsDir()
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	path := filepath.Join(dir, accountID+".sync")
	return os.WriteFile(path, []byte(syncToken), 0600)
}

func (s *Store) LoadSyncToken(accountID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := filepath.Join(s.accountsDir(), accountID+".sync")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}
