/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmgimp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dtm-labs/dtmdriver"
	"github.com/dtm-labs/logger"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/tengzbiao/dtm-client/dtmcli/dtmimp"
	"github.com/tengzbiao/dtm-client/dtmgrpc/dtmgpb"
)

type rawCodec struct{}

func (cb rawCodec) Marshal(v interface{}) ([]byte, error) {
	return v.([]byte), nil
}

func (cb rawCodec) Unmarshal(data []byte, v interface{}) error {
	ba, ok := v.(*[]byte)
	dtmimp.PanicIf(!ok, fmt.Errorf("please pass in *[]byte"))
	*ba = append(*ba, data...)

	return nil
}

func (cb rawCodec) Name() string { return "dtm_raw" }

// 连接配置
type connConfig struct {
	conn      *grpc.ClientConn
	lastUsed  time.Time
	createdAt time.Time
}

var (
	normalClients, rawClients sync.Map
	connMutex                 sync.RWMutex
	balancerName              = "round_robin" // 默认使用round_robin负载均衡
)

// ClientInterceptors declares grpc.UnaryClientInterceptors slice
var ClientInterceptors = []grpc.UnaryClientInterceptor{}

// MustGetDtmClient 1
func MustGetDtmClient(grpcServer string) dtmgpb.DtmClient {
	return dtmgpb.NewDtmClient(MustGetGrpcConn(grpcServer, false))
}

// GetGrpcConn 获取gRPC连接，支持连接复用和健康检查
func GetGrpcConn(grpcServer string, isRaw bool) (conn *grpc.ClientConn, rerr error) {
	clients := &normalClients
	if isRaw {
		clients = &rawClients
	}
	grpcServer = dtmimp.MayReplaceLocalhost(grpcServer)

	// 尝试从缓存获取连接
	v, ok := clients.Load(grpcServer)
	if ok {
		if connConfig, ok := v.(*connConfig); ok {
			// 检查连接是否仍然有效
			if isConnHealthy(connConfig.conn) {
				connConfig.lastUsed = time.Now()
				return connConfig.conn, nil
			} else {
				// 连接不健康，移除并重新创建
				logger.Warnf("unhealthy connection detected for %s, recreating", grpcServer)
				clients.Delete(grpcServer)
				closeConn(connConfig.conn)
			}
		}
	}

	// 创建新连接
	connMutex.Lock()
	defer connMutex.Unlock()

	// 双重检查，避免并发创建
	v, ok = clients.Load(grpcServer)
	if ok {
		if connConfig, ok := v.(*connConfig); ok {
			connConfig.lastUsed = time.Now()
			return connConfig.conn, nil
		}
	}

	// 创建连接配置
	opts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}],"healthCheckConfig":{"serviceName":""}}`, balancerName)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(),
	}

	if isRaw {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))
	}

	// 添加拦截器
	interceptors := append(ClientInterceptors, GrpcClientLog)
	interceptors = append(interceptors, dtmdriver.Middlewares.Grpc...)
	opts = append(opts, grpc.WithChainUnaryInterceptor(interceptors...))

	logger.Debugf("grpc client connecting %s", grpcServer)

	// 创建连接
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, rerr = grpc.DialContext(ctx, grpcServer, opts...)
	if rerr == nil {
		// 存储连接配置
		connConfig := &connConfig{
			conn:      conn,
			lastUsed:  time.Now(),
			createdAt: time.Now(),
		}
		clients.Store(grpcServer, connConfig)
		logger.Debugf("grpc client inited for %s", grpcServer)
	}

	return conn, rerr
}

// MustGetGrpcConn 1
func MustGetGrpcConn(grpcServer string, isRaw bool) *grpc.ClientConn {
	conn, err := GetGrpcConn(grpcServer, isRaw)
	dtmimp.E2P(err)
	return conn
}

// isConnHealthy 检查连接是否健康
func isConnHealthy(conn *grpc.ClientConn) bool {
	if conn == nil {
		return false
	}

	// 检查连接状态
	state := conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// closeConn 安全关闭连接
func closeConn(conn *grpc.ClientConn) {
	if conn != nil {
		if err := conn.Close(); err != nil {
			logger.Errorf("failed to close grpc connection: %v", err)
		}
	}
}
