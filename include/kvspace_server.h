// kvspace-rdma server public C API.
#pragma once
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kvspace_server kvspace_server;

// 创建 server 实例。listen_addr: "0.0.0.0:9000"
kvspace_server* kvspace_server_create(const char* listen_addr, int num_shards);

// 销毁 server
void kvspace_server_destroy(kvspace_server* s);

// 启动（阻塞当前线程直到 stop）
int kvspace_server_start(kvspace_server* s);

// 停止
void kvspace_server_stop(kvspace_server* s);

// 已处理请求总数
uint64_t kvspace_server_processed(kvspace_server* s);

#ifdef __cplusplus
}
#endif
