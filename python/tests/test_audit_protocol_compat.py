"""/audit protocol-compat 举证测试(2026-08-08)。

每个测试在当前代码上 FAIL,证明对应缺陷存在;修复后转绿即回归锁。
只含测试,不改实现。对照 .claude/protocol-compat-plan.md 批次 0-5。
"""

import struct
from types import SimpleNamespace

import pytest

from lite_server.exceptions import HTTPException
from lite_server.kserve import parse_inputs
from lite_server.pipeline import _split_binary_inputs


def test_audit_parse_inputs_shape_mismatch_raises_400_not_500():
    """数据假设:客户端可控的 shape 与二进制长度不符 → np.reshape 的
    ValueError 逃逸为 500。shape 来自客户端 JSON 头,客户端输入错误必须
    400(HTTPException),与 _split_binary_inputs 的校验一致。"""
    ctx = SimpleNamespace(
        binary_data={"x": memoryview(struct.pack("<2f", 1.0, 2.0))},
        request={
            "inputs": [
                {
                    "name": "x",
                    "shape": [3],  # 8 字节 = 2 个 FP32,装不下 3
                    "datatype": "FP32",
                    "parameters": {"binary_data_size": 8},
                }
            ]
        },
    )
    with pytest.raises(HTTPException):  # 当前抛 ValueError → FAIL
        parse_inputs(ctx)


def test_audit_parse_inputs_ragged_buffer_raises_400_not_500():
    """同上:tail 长度不被 itemsize 整除 → np.frombuffer ValueError → 500;
    必须 400。"""
    ctx = SimpleNamespace(
        binary_data={"x": memoryview(b"\x00" * 7)},  # 7 不是 4 的倍数
        request={
            "inputs": [
                {
                    "name": "x",
                    "shape": [1],
                    "datatype": "FP32",
                    "parameters": {"binary_data_size": 7},
                }
            ]
        },
    )
    with pytest.raises(HTTPException):  # 当前抛 ValueError → FAIL
        parse_inputs(ctx)


def test_audit_parse_inputs_supports_bytes_datatype():
    """功能缺失:BYTES 是 KServe 合法 datatype(文本 tensor 主通道,每元素
    4B LE 长度前缀)。Rust 响应侧 encode_value 支持 BYTES(kserve.rs),
    请求侧 G15 helper 却 400 'unsupported datatype' —— 非对称,文本模型
    无法用 parse_inputs。"""
    tail = b"\x04\x00\x00\x00test"
    ctx = SimpleNamespace(
        binary_data={"prompt": memoryview(tail)},
        request={
            "inputs": [
                {
                    "name": "prompt",
                    "shape": [1],
                    "datatype": "BYTES",
                    "parameters": {"binary_data_size": 8},
                }
            ]
        },
    )
    out = parse_inputs(ctx)  # 当前 HTTPException(400) → FAIL
    assert list(out["prompt"]) == [b"test"]


def test_audit_split_binary_inputs_structural_garbage_raises_400():
    """健壮性(D4 防御意图):gRPC/自定义路径可带入结构垃圾——非 dict 的
    JSON 头、非 dict 的 parameters(HTTP 边缘已被 Rust serde 挡 400)。
    request.get / params.get 命中 list → AttributeError → 500。"""
    with pytest.raises(HTTPException):  # 当前 AttributeError → FAIL
        _split_binary_inputs(["not-a-dict"], memoryview(b""))
    with pytest.raises(HTTPException):  # 当前 AttributeError → FAIL
        _split_binary_inputs(
            {"inputs": [{"name": "a", "datatype": "FP32", "parameters": [1]}]},
            memoryview(b""),
        )


def test_audit_cli_profile_flag_kept_as_deprecated_alias(tmp_path):
    """CLI 兼容:--profile 随 0.8.3 发布(origin/main cli.py:185),批次 3
    改名 --interop 未保留 alias → 旧脚本 argparse exit 2 硬破。过渡期内
    --profile 应作为废弃别名继续被解析(退出码 ≠ 2 = 参数层面被接受)。"""
    from lite_server.cli import main

    try:
        main(["analyze", "--model", "m", "--model-repo", str(tmp_path),
              "--profile", "kserve-v2"])
    except SystemExit as e:
        assert e.code != 2, (
            "--profile must remain a deprecated alias, not argparse exit 2 "
            "(shipped in 0.8.3, removed without deprecation window)"
        )
    except Exception:
        pass  # analyze 对空 repo 报错无所谓——参数解析已接受 --profile
