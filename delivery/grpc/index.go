package grpchandle

import (
	"app/generated/grpc/servicegrpc"
	"app/internal/connection"
)

type grpcHandle struct {
	servicegrpc.UnimplementedQuantityServiceServer
	infoConnection connection.Connection
}

func Register() servicegrpc.QuantityServiceServer {
	return &grpcHandle{
		infoConnection: connection.GetConnect(),
	}
}