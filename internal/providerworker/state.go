package providerworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/example/earthquake-service/internal/provider"
)

type State struct {
	Provider         string                   `json:"provider"`
	Validators       provider.CacheValidators `json:"validators"`
	BaselineComplete bool                     `json:"baseline_complete"`
	Checkpoint       time.Time                `json:"checkpoint"`
}

type StateStore interface {
	Load(context.Context) (State, error)
	Save(context.Context, State) error
}

type FileStateStore struct {
	path string
	mu   sync.Mutex
}

func NewFileStateStore(path string) (*FileStateStore, error) {
	if path == "" || !filepath.IsAbs(path) {
		return nil, errors.New("provider state file must be an absolute path")
	}
	return &FileStateStore{path: path}, nil
}

func (s *FileStateStore) Load(_ context.Context) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, nil
	}
	if err != nil {
		return State{}, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil {
		return State{}, err
	}
	if len(data) > 64<<10 {
		return State{}, errors.New("provider state file exceeds 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state State
	if err := decoder.Decode(&state); err != nil {
		return State{}, fmt.Errorf("decode provider state: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return State{}, errors.New("provider state contains multiple JSON values")
	}
	return state, nil
}

func (s *FileStateStore) Save(_ context.Context, state State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(s.path), ".provider-state-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, s.path)
}
