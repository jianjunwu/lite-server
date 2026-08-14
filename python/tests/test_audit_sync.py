"""Audit sync checks (read-only): proto/pb2 parity and Python/Rust ensemble
schema parity after the ensemble-streaming (batches 0-6) and
model-upload-and-retire (batches 0-4) plans.

Mechanically verifies:
- pb2 exposes the 7 repository RPCs on the Admin service with the right
  streaming flags and request/response types (18 RPCs total);
- the new messages carry the fields/numbers declared in
  src/proto/liteserver.proto;
- python/lite_server/ensemble.py dataclass field surfaces match the Rust
  serde structs in src/ensemble/types.rs (parsed mechanically, no
  hardcoded field lists on the Rust side).
"""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path

import pytest

from lite_server import ensemble as py_ensemble
from lite_server.proto import liteserver_pb2 as pb2
from lite_server.proto import liteserver_pb2_grpc as pb2_grpc

REPO_ROOT = Path(__file__).resolve().parents[2]
TYPES_RS = REPO_ROOT / "src" / "ensemble" / "types.rs"

ADMIN_RPCS = {
    # name: (client_streaming, server_streaming, request, response)
    "GetInfo": (False, False, "GetInfoRequest", "GetInfoResponse"),
    "ListModels": (False, False, "ListModelsRequest", "ListModelsResponse"),
    "ListVersions": (False, False, "ListVersionsRequest", "ListVersionsResponse"),
    "ModelReady": (False, False, "ModelReadyRequest", "ModelReadyResponse"),
    "ModelHealth": (False, False, "ModelHealthRequest", "ModelHealthResponse"),
    "LoadModel": (False, False, "LoadModelRequest", "LoadModelResponse"),
    "UnloadModel": (False, False, "UnloadModelRequest", "UnloadModelResponse"),
    "ReloadModel": (False, False, "ReloadModelRequest", "ReloadModelResponse"),
    "ActivateVersion": (
        False,
        False,
        "ActivateVersionRequest",
        "ActivateVersionResponse",
    ),
    "SetRouting": (False, False, "SetRoutingRequest", "SetRoutingResponse"),
    "GetModelStats": (False, False, "GetModelStatsRequest", "GetModelStatsResponse"),
    # --- model-upload-and-retire plan batch 3 (7 RPCs) ---
    "DeleteVersion": (False, False, "DeleteVersionRequest", "DeleteVersionResponse"),
    "DeleteModel": (False, False, "DeleteModelRequest", "DeleteModelResponse"),
    "DeleteVersions": (
        False,
        False,
        "DeleteVersionsRequest",
        "DeleteVersionsResponse",
    ),
    "RepositoryDrift": (
        False,
        False,
        "RepositoryDriftRequest",
        "RepositoryDriftResponse",
    ),
    "UploadModel": (True, False, "UploadModelRequest", "UploadModelResponse"),
    "DownloadModel": (False, True, "DownloadModelRequest", "DownloadModelChunk"),
    "ListFiles": (False, False, "ListFilesRequest", "ListFilesResponse"),
}

# field name -> number, transcribed from src/proto/liteserver.proto
MESSAGE_FIELDS = {
    "DeleteVersionRequest": {"model_name": 1, "version": 2, "force": 3},
    "DeleteVersionResponse": {"success": 1, "message": 2},
    "DeleteModelRequest": {"model_name": 1, "force": 2},
    "DeleteModelResponse": {"success": 1, "message": 2},
    "DeleteVersionsRequest": {"model_name": 1, "keep": 2, "versions": 3, "force": 4},
    "DeleteVersionsResponse": {"deleted": 1, "failed": 2},
    "DeleteFailure": {"version": 1, "error": 2},
    "RepositoryDriftRequest": {"model_name": 1},
    "RepositoryDriftResponse": {"configured_missing": 1, "on_disk_unconfigured": 2},
    "DriftMissingEntry": {"model": 1, "version": 2},
    "DriftDiskEntry": {
        "model": 1,
        "version": 2,
        "size_bytes": 3,
        "ensemble_referenced": 4,
    },
    "UploadModelRequest": {
        "model_name": 1,
        "version": 2,
        "load": 3,
        "file_name": 4,
        "data": 5,
    },
    "UploadModelResponse": {
        "success": 1,
        "model": 2,
        "version": 3,
        "files": 4,
        "loaded": 5,
        "load_error": 6,
    },
    "DownloadModelRequest": {"model_name": 1, "version": 2, "file": 3},
    "DownloadModelChunk": {"data": 1, "is_final": 2, "sha256": 3, "size": 4},
    "ListFilesRequest": {"model_name": 1, "version": 2},
    "ListFilesResponse": {"model": 1, "version": 2, "files": 3},
    "FileEntry": {"name": 1, "size": 2, "modified": 3, "is_dir": 4},
}


def _admin_service():
    return pb2.DESCRIPTOR.services_by_name["Admin"]


def test_admin_service_has_18_rpcs_with_expected_shapes():
    svc = _admin_service()
    assert set(ADMIN_RPCS) == {m.name for m in svc.methods}
    for name, (cs, ss, req, resp) in ADMIN_RPCS.items():
        m = svc.methods_by_name[name]
        assert m.client_streaming is cs, name
        assert m.server_streaming is ss, name
        assert m.input_type.name == req, name
        assert m.output_type.name == resp, name


@pytest.mark.parametrize("message", sorted(MESSAGE_FIELDS))
def test_message_fields_match_proto(message):
    desc = pb2.DESCRIPTOR.message_types_by_name[message]
    got = {f.name: f.number for f in desc.fields}
    assert got == MESSAGE_FIELDS[message]


def test_grpc_stub_and_servicer_expose_repository_rpcs():
    stub_src = Path(pb2_grpc.__file__).read_text()
    for rpc in ADMIN_RPCS:
        assert f"'/liteserver.Admin/{rpc}'" in stub_src


def _rust_struct_fields(struct_name: str) -> list[str]:
    """Parse `pub struct <name> { pub <field>: ... }` out of types.rs."""
    src = TYPES_RS.read_text()
    m = re.search(
        r"pub struct " + struct_name + r" \{(?P<body>.*?)\n\}", src, re.DOTALL
    )
    assert m, f"struct {struct_name} not found in {TYPES_RS}"
    return re.findall(r"pub (\w+):", m.group("body"))


def _py_fields(cls) -> set[str]:
    return {f.name for f in dataclasses.fields(cls)}


def test_step_fields_match_rust_ensemble_step_raw():
    rust = set(_rust_struct_fields("EnsembleStepRaw"))
    assert _py_fields(py_ensemble.Step) == rust


def test_input_decl_fields_match_rust():
    rust = {"type" if f == "ty" else f for f in _rust_struct_fields("InputDecl")}
    assert _py_fields(py_ensemble.InputDecl) == rust


def test_step_output_fields_match_rust():
    rust = {"type" if f == "ty" else f for f in _rust_struct_fields("StepOutputDecl")}
    assert _py_fields(py_ensemble.StepOutput) == rust


def test_dag_set_fields_match_rust():
    rust = set(_rust_struct_fields("EnsembleDagSet"))
    assert _py_fields(py_ensemble.DagSet) == rust


def test_ensemble_dag_fields_match_rust_block():
    rust = set(_rust_struct_fields("EnsembleBlock"))
    assert _py_fields(py_ensemble.EnsembleDAG) == rust
