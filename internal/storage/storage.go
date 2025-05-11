package storage

import (
	"errors"
	//	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

// Engine defines the interface for storage backends
type Engine interface {
	Get(key string) (string, error)
	Put(key string, value string) error
	Delete(key string) error
	Close() error
}

// MemoryStorage is an in-memory implementation of the storage engine
type MemoryStorage struct {
	data  map[string]string
	mutex sync.RWMutex
}

// NewMemoryStorage creates a new in-memory storage engine
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string]string),
	}
}

// Get retrieves a value for a key
func (m *MemoryStorage) Get(key string) (string, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	value, ok := m.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	return value, nil
}

// Put stores a key-value pair
func (m *MemoryStorage) Put(key string, value string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.data[key] = value
	return nil
}

// Delete removes a key-value pair
func (m *MemoryStorage) Delete(key string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, ok := m.data[key]
	if !ok {
		return ErrKeyNotFound
	}

	delete(m.data, key)
	return nil
}

// Close does nothing for memory storage
func (m *MemoryStorage) Close() error {
	return nil
}

// FileStorage is a file-based implementation of the storage engine
type FileStorage struct {
	baseDir string
	mutex   sync.RWMutex
}

// NewFileStorage creates a new file-based storage engine
func NewFileStorage(baseDir string) (*FileStorage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}

	return &FileStorage{
		baseDir: baseDir,
	}, nil
}

// Get retrieves a value for a key
func (f *FileStorage) Get(key string) (string, error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	filePath := filepath.Join(f.baseDir, key)

	//data, err := ioutil.ReadFile(filePath)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrKeyNotFound
		}
		return "", err
	}

	return string(data), nil
}

// Put stores a key-value pair
func (f *FileStorage) Put(key string, value string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	filePath := filepath.Join(f.baseDir, key)
	//return ioutil.WriteFile(filePath, []byte(value), 0644)
	return os.WriteFile(filePath, []byte(value), 0644)
}

// Delete removes a key-value pair
func (f *FileStorage) Delete(key string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	filePath := filepath.Join(f.baseDir, key)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return ErrKeyNotFound
	}

	return os.Remove(filePath)
}

// Close does nothing for file storage
func (f *FileStorage) Close() error {
	return nil
}
