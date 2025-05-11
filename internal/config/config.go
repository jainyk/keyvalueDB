package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Config holds all configuration for the distributed key-value store
type Config struct {
	NodeID            string
	BindAddress       string
	RaftBindAddress   string
	JoinAddress       string
	DataDir           string
	RaftLogDir        string
	ReplicationFactor int
	HeartbeatInterval int
	ElectionTimeout   int
	ConsistencyModel  string // "strong" or "eventual"
	PartitionStrategy string // "consistent-hash" or "range"
}

// LoadConfig loads configuration from file and environment variables
func LoadConfig(configPath string) (*Config, error) {
	// Default config
	config := &Config{
		NodeID:            "",
		BindAddress:       "127.0.0.1:50051",
		RaftBindAddress:   "127.0.0.1:50052",
		JoinAddress:       "",
		DataDir:           "./data",
		RaftLogDir:        "./raft",
		ReplicationFactor: 3,
		HeartbeatInterval: 500,  // milliseconds
		ElectionTimeout:   1500, // milliseconds
		ConsistencyModel:  "strong",
		PartitionStrategy: "consistent-hash",
	}

	// Set up viper
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else {
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("$HOME/.distkv")
	}

	// Read environment variables
	viper.AutomaticEnv()
	viper.SetEnvPrefix("DISTKV")

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %v", err)
		}
		log.Println("No config file found, using defaults and environment variables")
	}

	// Override config with values from file/env if they exist
	if viper.IsSet("node_id") {
		config.NodeID = viper.GetString("node_id")
	} else if config.NodeID == "" {
		// Generate random node ID if not provided
		config.NodeID = generateNodeID()
	}

	if viper.IsSet("bind_address") {
		config.BindAddress = viper.GetString("bind_address")
	}

	if viper.IsSet("raft_bind_address") {
		config.RaftBindAddress = viper.GetString("raft_bind_address")
	}

	if viper.IsSet("join_address") {
		config.JoinAddress = viper.GetString("join_address")
	}

	if viper.IsSet("data_dir") {
		config.DataDir = viper.GetString("data_dir")
	}

	if viper.IsSet("raft_log_dir") {
		config.RaftLogDir = viper.GetString("raft_log_dir")
	}

	if viper.IsSet("replication_factor") {
		config.ReplicationFactor = viper.GetInt("replication_factor")
	}

	if viper.IsSet("heartbeat_interval") {
		config.HeartbeatInterval = viper.GetInt("heartbeat_interval")
	}

	if viper.IsSet("election_timeout") {
		config.ElectionTimeout = viper.GetInt("election_timeout")
	}

	if viper.IsSet("consistency_model") {
		config.ConsistencyModel = viper.GetString("consistency_model")
	}

	if viper.IsSet("partition_strategy") {
		config.PartitionStrategy = viper.GetString("partition_strategy")
	}

	// Create directories if they don't exist
	// Append node ID to make directories unique per node
	config.DataDir = filepath.Join(config.DataDir, config.NodeID)
	config.RaftLogDir = filepath.Join(config.RaftLogDir, config.NodeID)

	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	if err := os.MkdirAll(config.RaftLogDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create Raft log directory: %v", err)
	}

	return config, nil
}

// generateNodeID generates a random node ID
func generateNodeID() string {
	// Simple implementation for demo purposes
	return fmt.Sprintf("node-%d", os.Getpid())
}
