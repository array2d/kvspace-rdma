// kvspace-rdma wire protocol opcodes.
#pragma once
#include <cstdint>

namespace kvspace::proto {

enum class Op : uint8_t {
    // ── 请求 (0x01–0x3F) ──
    Get     = 0x01,
    GetMany = 0x02,
    Set     = 0x03,
    SetMany = 0x04,
    Del     = 0x05,
    DelTree = 0x06,
    List    = 0x07,
    Watch   = 0x08,
    Notify  = 0x09,
    Link    = 0x0A,
    Unlink  = 0x0B,
    DisConn = 0x0C,

    // ── 响应 (0x81–0xBF) ──
    Resp_OK  = 0x81,
    Resp_Err = 0x82,
    Resp_Push = 0x83,  // Watch server-push
};

enum class Flag : uint16_t {
    NONE       = 0,
    TAIL_CALL  = 1 << 0,   // DisConn
    BATCH_MORE = 1 << 1,   // SetMany/GetMany 未完
    HAS_MORE   = 1 << 0,   // GetMany/List 多值分片 (响应)
    TIMEOUT    = 1 << 1,   // Watch 超时 (响应)
};

inline constexpr uint16_t MAGIC = 0x4B56;
inline constexpr uint8_t  VERSION = 1;

} // namespace kvspace::proto
