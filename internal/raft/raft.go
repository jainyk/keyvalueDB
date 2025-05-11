package raft

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/jainyk/distkv/internal/config"
)

// RaftNode represents a node in the Raft cluster
type RaftNode struct {
	config *config.Config
	raft   *raft.Raft
	fsm    *FSM
}

// NewRaftNode creates a new Raft node
func NewRaftNode(config *config.Config, fsm *FSM) (*RaftNode, error) {
	// Create Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	raftConfig.HeartbeatTimeout = time.Duration(config.HeartbeatInterval) * time.Millisecond
	raftConfig.ElectionTimeout = time.Duration(config.ElectionTimeout) * time.Millisecond
	raftConfig.LogLevel = "debug"

	// Create transport for Raft communication
	addr, err := net.ResolveTCPAddr("tcp", config.RaftBindAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Raft bind address: %v", err)
	}

	transport, err := raft.NewTCPTransport(config.RaftBindAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft transport: %v", err)
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(config.RaftLogDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %v", err)
	}

	// Create BoltDB store for Raft logs
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(config.RaftLogDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create BoltDB store: %v", err)
	}

	// Create Raft instance
	r, err := raft.NewRaft(raftConfig, fsm, boltDB, boltDB, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft: %v", err)
	}

	node := &RaftNode{
		config: config,
		raft:   r,
		fsm:    fsm,
	}

	// Bootstrap a single-node cluster if this is the first node
	if config.JoinAddress == "" {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(config.NodeID),
					Address: transport.LocalAddr(),
				},
			},
		}

		r.BootstrapCluster(configuration)
	}

	return node, nil
}

// Apply applies a command to the Raft cluster
func (r *RaftNode) Apply(cmd []byte) error {
	future := r.raft.Apply(cmd, 5*time.Second)
	if err := future.Error(); err != nil {
		return err
	}

	response := future.Response()
	if err, ok := response.(error); ok && err != nil {
		return err
	}

	return nil
}

// Join joins a node to the Raft cluster
func (r *RaftNode) Join(nodeID, address string) error {
	// Check if the node is already a member of the cluster
	configFuture := r.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	for _, server := range configFuture.Configuration().Servers {
		if server.ID == raft.ServerID(nodeID) {
			// Node already exists
			return nil
		}
	}

	// Add the node to the cluster
	future := r.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %v", err)
	}

	return nil
}

// Leave removes a node from the Raft cluster
func (r *RaftNode) Leave(nodeID string) error {
	// Remove the node from the cluster
	future := r.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove server: %v", err)
	}

	return nil
}

// IsLeader returns whether this node is the Raft leader
func (r *RaftNode) IsLeader() bool {
	return r.raft.State() == raft.Leader
}

// GetLeader returns the address of the Raft leader
func (r *RaftNode) GetLeader() string {
	return string(r.raft.Leader())
}

// Shutdown shuts down the Raft node
func (r *RaftNode) Shutdown() error {
	return r.raft.Shutdown().Error()
}

// FSM is a finite state machine that implements raft.FSM
type FSM struct {
	dataDir string
}

// NewFSM creates a new FSM
func NewFSM(dataDir string) (*FSM, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	return &FSM{
		dataDir: dataDir,
	}, nil
}

// Apply applies a log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	// Implement command application logic here
	// For now, we'll just return nil
	return nil
}

// Snapshot returns a snapshot of the FSM
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &Snapshot{}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	return nil
}

// Snapshot is an implementation of raft.FSMSnapshot
type Snapshot struct{}

// Persist is used to persist the FSM snapshot
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

// Release is called when we are finished with the snapshot
func (s *Snapshot) Release() {}
