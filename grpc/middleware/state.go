package middleware

import (
	"context"
	"github.com/mitesoro/code/stat/cpu"
	"github.com/mitesoro/code/stat/summary"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"strconv"
	"time"
)

const (
	CPUUsage = "cpu_usage"
	Errors   = "errors"
	Requests = "requests"
)

func Stats() grpc.UnaryServerInterceptor {
	errstat := summary.New(time.Second*3, 10)

	return func(ctx context.Context, req interface{}, args *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		c := int64(0)
		resp, err = handler(ctx, req)
		if err != nil {
			c = 1
		}
		errstat.Add(c)
		errors, requests := errstat.Value()
		kv := []string{
			Errors, strconv.FormatInt(errors, 10),
			Requests, strconv.FormatInt(requests, 10),
		}
		usage := cpu.CpuUsage()
		if usage != 0 {
			kv = append(kv, CPUUsage, strconv.FormatInt(usage, 10))
		}

		trailer := metadata.Pairs(kv...)
		grpc.SetTrailer(ctx, trailer)

		return
	}
}
