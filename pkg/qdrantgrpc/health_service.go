package qdrantgrpc

import (
	"context"

	pb "github.com/orneryd/nornicdb/pkg/qdrantgrpc/gen"
)

// HealthService implements the gRPC health checking protocol.
type HealthService struct {
	pb.UnimplementedHealthServer
}

// NewHealthService creates a new Health service.
func NewHealthService() *HealthService {
	return &HealthService{}
}

// Check returns the health status of the server.
func (s *HealthService) Check(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		Status: pb.ServingStatus_SERVING,
	}, nil
}

