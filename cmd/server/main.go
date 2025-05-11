package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jainyk/distkv/internal/config"
	"github.com/jainyk/distkv/internal/server"
)

func main() {
	configFile := flag.String("config", "", "Path to config file")
	nodeID := flag.String("node-id", "", "Node ID")
	bindAddr := flag.String("bind", "", "Bind address for the server")
	raftAddr := flag.String("raft", "", "Bind address for Raft")
	joinAddr := flag.String("join", "", "Address of a node to join")
	dataDir := flag.String("data-dir", "", "Data directory")
	raftDir := flag.String("raft-dir", "", "Raft log directory")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Override config with command line flags if provided
	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}

	if *bindAddr != "" {
		cfg.BindAddress = *bindAddr
	}

	if *raftAddr != "" {
		cfg.RaftBindAddress = *raftAddr
	}

	if *joinAddr != "" {
		cfg.JoinAddress = *joinAddr
	}

	if *dataDir != "" {
		cfg.DataDir = filepath.Join(*dataDir, cfg.NodeID)
		if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
			log.Fatalf("Failed to create data directory: %v", err)
		}
	}

	if *raftDir != "" {
		cfg.RaftLogDir = filepath.Join(*raftDir, cfg.NodeID)
		if err := os.MkdirAll(cfg.RaftLogDir, 0755); err != nil {
			log.Fatalf("Failed to create Raft log directory: %v", err)
		}
	}

	// Create and start server
	srv, err := server.NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Handle graceful shutdown
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stopCh
		log.Println("Shutting down server...")
		srv.Stop()
		os.Exit(0)
	}()

	log.Printf("Starting server with ID %s on %s", cfg.NodeID, cfg.BindAddress)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
