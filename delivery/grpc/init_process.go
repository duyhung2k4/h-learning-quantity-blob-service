package grpchandle

import (
	appcommon "app/cmd/quantity-blob-service/app_common"
	"app/generated/proto/servicegrpc"
	constant "app/internal/constants"
	logapp "app/pkg/log"
	"context"
	"fmt"
)

func (h *grpcHandle) InitProcessQuantity(ctx context.Context, req *servicegrpc.InitProcessQuantityRequest) (*servicegrpc.InitProcessQuantityResponse, error) {
	err := appcommon.CreateProcess(req.UuidProcess)
	if err != nil {
		logapp.Logger("goroutine-process-quantity", err.Error(), constant.ERROR_LOG)
		return nil, err
	}

	ipServer := fmt.Sprintf(
		"%s:%s",
		h.infoConnection.QuantityBlobService.Host,
		h.infoConnection.QuantityBlobService.Grpc,
	)

	res := &servicegrpc.InitProcessQuantityResponse{
		Ip: ipServer,
	}

	return res, nil
}
