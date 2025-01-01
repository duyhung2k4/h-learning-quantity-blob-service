package grpchandle

import (
	"app/generated/grpc/servicegrpc"
	constant "app/internal/constants"
	logapp "app/pkg/log"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (h *grpcHandle) SendBlobQuantity(stream grpc.ClientStreamingServer[servicegrpc.SendBlobQuantityRequest, servicegrpc.SendBlobQuantityResponse]) error {
	// get info config
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return errors.New("metadata nil")
	}
	ipMergeBlob := md["ip_merge_blob"][0]
	uuid := md["uuid"][0]

	// log.Println("IP Merge blob: ", ipMergeBlob)
	// log.Println("UUID: ", uuid)

	//connect merge-blob-service
	connMergeBlobService, err := grpc.NewClient(ipMergeBlob, grpc.WithInsecure())
	if err != nil {
		logapp.Logger("connection-quantity-grpc", err.Error(), constant.ERROR_LOG)
		return err
	}
	grpcClientMergeBlob := servicegrpc.NewMergeBlobServiceClient(connMergeBlobService)
	ctxGrpc := metadata.NewOutgoingContext(stream.Context(), metadata.New(map[string]string{
		"uuid": uuid,
	}))
	streamMergeBlob, err := grpcClientMergeBlob.SendMergeBlob(ctxGrpc)
	if err != nil {
		return err
	}

	// io
	inputReader, inputWriter := io.Pipe()
	outputReader, outputWriter := io.Pipe()
	chanBlob := make(chan []byte, 1*100*100)

	// config ffmpeg
	cmd := exec.Command("ffmpeg",
		"-f", "webm", // Định dạng đầu vào là WebM
		"-i", "pipe:0", // Nhận từ stdin
		"-f", "matroska", // Định dạng đầu ra là WebM
		"-vf", "scale=-2:360", // Giảm độ phân giải để giảm tải CPU
		"-preset", "ultrafast",
		"-vcodec", "libx264", // Bộ mã hóa video VP8
		"-acodec", "aac",
		"-c:a", "copy", // Giữ bitrate âm thanh ở mức 64 kbps (âm thanh giữ nguyên chất lượng)
		"pipe:1", // Ghi ra stdout
	)

	// original
	// cmd := exec.Command("ffmpeg",
	// 	"-f", "webm", // Định dạng đầu vào là WebM
	// 	"-i", "pipe:0", // Nhận dữ liệu từ stdin
	// 	"-f", "matroska",
	// 	"-c:v", "copy", // Sao chép luồng video, không mã hóa lại
	// 	"-c:a", "copy", // Sao chép luồng âm thanh, không mã hóa lại
	// 	"pipe:1", // Xuất dữ liệu ra stdout
	// )

	cmd.Stdin = inputReader
	cmd.Stdout = outputWriter
	cmd.Stderr = os.Stderr

	// Start ffmpeg
	err = cmd.Start()
	if err != nil {
		log.Fatalf("error start ffmpeg: %v", err)
	}
	defer cmd.Wait()

	// log info
	// log.Println("UUID: ", uuid)
	// log.Println("IP Merge blob server: ", ipMergeBlob)

	// Read output
	go func() {
		defer outputReader.Close()
		buffer := make([]byte, 4096)

		for {
			n, err := outputReader.Read(buffer)

			if err == io.EOF {
				log.Println("error output ffmpeg")
				break
			}

			if err != nil {
				log.Printf("error read ffmpeg: %v", err)
				break
			}

			// log.Printf("Data encoding: %d", len(buffer[:n]))

			streamMergeBlob.Send(&servicegrpc.SendMergeBlobRequest{
				Blob: buffer[:n],
			})
		}
	}()

	// push blob
	go func() {
		for blob := range chanBlob {
			log.Printf("%s: %d", uuid, len(blob))
			_, err = inputWriter.Write(blob)
			if err != nil {
				log.Println("Error encoding: ", err)
			}
		}
	}()

	for {
		req, err := stream.Recv()

		// log.Println("Error: ", err)
		if err == io.EOF {
			log.Println("Stream ended by client.")
			return stream.SendAndClose(&servicegrpc.SendBlobQuantityResponse{})
		}

		if err != nil {
			log.Printf("Error receiving data: %v", err)
			return err
		}

		// log.Println("Mess: ", len(req.Blob))
		chanBlob <- req.Blob
	}
}
