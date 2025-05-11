package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/jainyk/distkv/internal/config"
	"github.com/jainyk/distkv/internal/partition"
	"github.com/jainyk/distkv/internal/raft"
	"github.com/jainyk/distkv/internal/storage"
	pb "github.com/jainyk/distkv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server represents a KV store server node
type Server struct {
	pb.UnimplementedKVStoreServer
	pb.UnimplementedNodeCommunicationServer

	config      *config.Config
	store       storage.Engine
	raftNode    *raft.RaftNode
	partitioner *partition.ConsistentHash
	grpcServer  *grpc.Server

	// For tracking cluster membership
	nodes map[string]string // nodeID -> address
}

// NewServer creates a new server instance
func NewServer(config *config.Config) (*Server, error) {
	// Create storage engine
	store, err := storage.NewFileStorage(config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %v", err)
	}

	// Create FSM for Raft
	fsm, err := raft.NewFSM(config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create FSM: %v", err)
	}

	// Create Raft node
	raftNode, err := raft.NewRaftNode(config, fsm)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft node: %v", err)
	}

	// Create consistent hash partitioner
	partitioner := partition.NewConsistentHash(100) // 100 virtual nodes per physical node
	partitioner.Add(config.NodeID)

	server := &Server{
		config:      config,
		store:       store,
		raftNode:    raftNode,
		partitioner: partitioner,
		nodes:       make(map[string]string),
	}

	// If joining an existing cluster
	if config.JoinAddress != "" {
		if err := server.joinCluster(); err != nil {
			return nil, fmt.Errorf("failed to join cluster: %v", err)
		}
	}

	return server, nil
}

// Start starts the server
func (s *Server) Start() error {
	// Start gRPC server
	lis, err := net.Listen("tcp", s.config.BindAddress)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	s.grpcServer = grpc.NewServer()
	pb.RegisterKVStoreServer(s.grpcServer, s)
	pb.RegisterNodeCommunicationServer(s.grpcServer, s)

	log.Printf("Server starting on %s", s.config.BindAddress)

	return s.grpcServer.Serve(lis)
}

// Stop stops the server
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.raftNode != nil {
		s.raftNode.Shutdown()
	}

	if s.store != nil {
		s.store.Close()
	}
}

// Get implements the KVStore Get RPC
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Get request for key: %s", req.Key)

	// Find the node responsible for this key
	responsibleNode := s.partitioner.Get(req.Key)

	// If this node is responsible, handle the request locally
	if responsibleNode == s.config.NodeID {
		value, err := s.store.Get(req.Key)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				return &pb.GetResponse{Found: false}, nil
			}
			return nil, err
		}

		return &pb.GetResponse{
			Found: true,
			Value: value,
		}, nil
	}

	// Otherwise, forward the request to the responsible node
	nodeAddr, ok := s.nodes[responsibleNode]
	if !ok {
		return nil, fmt.Errorf("responsible node %s not found in cluster", responsibleNode)
	}

	conn, err := grpc.Dial(nodeAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to node %s: %v", responsibleNode, err)
	}
	defer conn.Close()

	client := pb.NewKVStoreClient(conn)
	return client.Get(ctx, req)
}

// Put implements the KVStore Put RPC
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Put request for key: %s", req.Key)

	// Only the leader can accept writes in strong consistency mode
	if s.config.ConsistencyModel == "strong" && !s.raftNode.IsLeader() {
		leaderAddr := s.raftNode.GetLeader()
		// Forward to leader
		//conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure())
		conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to leader: %v", err)
		}
		defer conn.Close()

		client := pb.NewKVStoreClient(conn)
		return client.Put(ctx, req)
	}

	// Find the nodes responsible for this key (for replication)
	responsibleNodes := s.partitioner.GetN(req.Key, s.config.ReplicationFactor)

	// Handle the write locally
	if err := s.store.Put(req.Key, req.Value); err != nil {
		return nil, err
	}

	// Replicate to other nodes
	for _, nodeID := range responsibleNodes {
		if nodeID == s.config.NodeID {
			continue // Skip self
		}

		nodeAddr, ok := s.nodes[nodeID]
		if !ok {
			log.Printf("Warning: Node %s not found in cluster", nodeID)
			continue
		}
		log.Printf("Replicating data to node %s: and %s", nodeID, nodeAddr)
		// Asynchronously replicate to other nodes
		go func(addr string) {
			//conn, err := grpc.Dial(addr, grpc.WithInsecure())
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to node %s: %v", addr, err)
				return
			}
			defer conn.Close()
			// client := pb.NewKVStoreClient(conn)
			// return client.Put(ctx, req)
			client := pb.NewNodeCommunicationClient(conn)
			_, err = client.ReplicateData(context.Background(), &pb.ReplicateRequest{
				Key:       req.Key,
				Value:     req.Value,
				Operation: "PUT",
			})

			if err != nil {
				log.Printf("Failed to replicate data to %s: %v", addr, err)
			}
		}(nodeAddr)
	}
	return &pb.PutResponse{Success: true}, nil
}

// Delete implements the KVStore Delete RPC
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Printf("Delete request for key: %s", req.Key)

	// Only the leader can accept writes in strong consistency mode
	if s.config.ConsistencyModel == "strong" && !s.raftNode.IsLeader() {
		leaderAddr := s.raftNode.GetLeader()
		// Forward to leader
		conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("failed to connect to leader: %v", err)
		}
		defer conn.Close()

		client := pb.NewKVStoreClient(conn)
		return client.Delete(ctx, req)
	}

	// Find the nodes responsible for this key (for replication)
	responsibleNodes := s.partitioner.GetN(req.Key, s.config.ReplicationFactor)

	// Handle the delete locally
	if err := s.store.Delete(req.Key); err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return &pb.DeleteResponse{Success: false}, nil
		}
		return nil, err
	}

	// Replicate to other nodes
	for _, nodeID := range responsibleNodes {
		if nodeID == s.config.NodeID {
			continue // Skip self
		}

		nodeAddr, ok := s.nodes[nodeID]
		if !ok {
			log.Printf("Warning: Node %s not found in cluster", nodeID)
			continue
		}

		// Asynchronously replicate to other nodes
		go func(addr string) {
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Printf("Failed to connect to node %s: %v", addr, err)
				return
			}
			defer conn.Close()

			client := pb.NewNodeCommunicationClient(conn)
			_, err = client.ReplicateData(context.Background(), &pb.ReplicateRequest{
				Key:       req.Key,
				Operation: "DELETE",
			})

			if err != nil {
				log.Printf("Failed to replicate data to %s: %v: %s", addr, err, s.config.NodeID)
			}
		}(nodeAddr)
	}

	return &pb.DeleteResponse{Success: true}, nil
}

// JoinCluster implements the NodeCommunication JoinCluster RPC
func (s *Server) JoinCluster(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	log.Printf("Join request from node %s at %s", req.NodeId, req.Address)

	// Add node to the Raft cluster
	if err := s.raftNode.Join(req.NodeId, req.Address); err != nil {
		return nil, fmt.Errorf("failed to add node to Raft cluster: %v", err)
	}

	// Add node to the consistent hash ring
	s.partitioner.Add(req.NodeId)

	// Add node to the known nodes map
	s.nodes[req.NodeId] = req.Address

	// Return the current cluster nodes
	clusterNodes := make([]string, 0, len(s.nodes))
	for nodeID, addr := range s.nodes {
		clusterNodes = append(clusterNodes, fmt.Sprintf("%s=%s", nodeID, addr))
	}

	return &pb.JoinResponse{
		Accepted:     true,
		ClusterNodes: clusterNodes,
	}, nil
}

// LeaveCluster implements the NodeCommunication LeaveCluster RPC
func (s *Server) LeaveCluster(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	log.Printf("Leave request from node %s", req.NodeId)

	// Remove node from the Raft cluster
	if err := s.raftNode.Leave(req.NodeId); err != nil {
		return nil, fmt.Errorf("failed to remove node from Raft cluster: %v", err)
	}

	// Remove node from the consistent hash ring
	s.partitioner.Remove(req.NodeId)

	// Remove node from the known nodes map
	delete(s.nodes, req.NodeId)

	return &pb.LeaveResponse{
		Acknowledged: true,
	}, nil
}

// Heartbeat implements the NodeCommunication Heartbeat RPC
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	log.Printf("Heartbeat from node %s at %d", req.NodeId, req.Timestamp)

	// Update last seen timestamp for the node (not implemented)
	// In a real implementation, you would update the last seen timestamp
	// in a data structure to track node health.
	// For now, just log the heartbeat
	// log.Printf("Received heartbeat from node %s", req.NodeId)
	// // If the node is not already in the cluster, add it
	// if _, exists := s.nodes[req.NodeId]; !exists {
	// 	s.nodes[req.NodeId] = req.Address
	// 	s.partitioner.Add(req.NodeId)
	// }
	// // Send a response back to the node
	// // This is a simple acknowledgment, but in a real implementation,
	// // you might want to send more information back, such as the current
	// // cluster state or leader information.
	// log.Printf("Acknowledging heartbeat from node %s", req.NodeId)

	return &pb.HeartbeatResponse{
		Acknowledged: true,
	}, nil
}

// ReplicateData implements the NodeCommunication ReplicateData RPC
func (s *Server) ReplicateData(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	log.Printf("ReplicateData request for key: %s, operation: %s", req.Key, req.Operation)

	var err error
	log.Printf("Replicating data for key: %s, operation: %s, NodeId: %s", req.Key, req.Operation, s.config.NodeID)
	switch req.Operation {
	case "PUT":
		err = s.store.Put(req.Key, req.Value)
	case "DELETE":
		err = s.store.Delete(req.Key)
	default:
		return nil, fmt.Errorf("unknown operation: %s", req.Operation)
	}

	if err != nil {
		return nil, err
	}

	return &pb.ReplicateResponse{
		Success: true,
	}, nil
}

// joinCluster joins an existing cluster
func (s *Server) joinCluster() error {
	conn, err := grpc.Dial(s.config.JoinAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to join address: %v", err)
	}
	defer conn.Close()

	client := pb.NewNodeCommunicationClient(conn)
	resp, err := client.JoinCluster(context.Background(), &pb.JoinRequest{
		NodeId:  s.config.NodeID,
		Address: s.config.RaftBindAddress,
	})

	if err != nil {
		return fmt.Errorf("failed to join cluster: %v", err)
	}

	if !resp.Accepted {
		return errors.New("join request was not accepted")
	}

	// Parse and store cluster nodes
	for _, nodeStr := range resp.ClusterNodes {
		var nodeID, nodeAddr string
		fmt.Sscanf(nodeStr, "%s=%s", &nodeID, &nodeAddr)
		s.nodes[nodeID] = nodeAddr
		s.partitioner.Add(nodeID)
	}

	return nil
}
