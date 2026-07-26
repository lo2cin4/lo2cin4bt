from __future__ import annotations

import os

import scripts.doctor as doctor
from scripts.doctor import (
    _configure_preferred_rust_home,
    _is_supported_node_version,
    _node_version_failure_is_fatal,
    _parse_node_version,
    _preferred_command,
)


def test_doctor_rejects_node_versions_below_vite_floor() -> None:
    assert _parse_node_version("v20.18.0") == (20, 18, 0)
    assert not _is_supported_node_version((20, 18, 0))
    assert not _is_supported_node_version((22, 11, 0))
    assert not _is_supported_node_version((21, 7, 3))


def test_doctor_accepts_supported_lts_node_versions() -> None:
    assert _is_supported_node_version((20, 19, 0))
    assert _is_supported_node_version((22, 12, 0))
    assert _is_supported_node_version((24, 17, 0))


def test_doctor_allows_static_serve_when_frontend_dist_exists() -> None:
    assert not _node_version_failure_is_fatal(
        frontend_dist_exists=True,
        require_frontend_build=False,
    )
    assert _node_version_failure_is_fatal(
        frontend_dist_exists=True,
        require_frontend_build=True,
    )
    assert _node_version_failure_is_fatal(
        frontend_dist_exists=False,
        require_frontend_build=False,
    )


def test_doctor_prefers_project_node_home(tmp_path, monkeypatch) -> None:
    node_home = tmp_path / "node"
    node_home.mkdir()
    executable_name = "node.exe" if os.name == "nt" else "node"
    node_path = node_home / executable_name
    node_path.write_text("", encoding="utf-8")
    monkeypatch.setenv("LO2CIN4BT_NODE_HOME", str(node_home))

    assert _preferred_command("node") == str(node_path)


def test_doctor_prefers_project_rust_home(tmp_path, monkeypatch) -> None:
    rust_home = tmp_path / "rust"
    cargo_bin = rust_home / "cargo" / "bin"
    cargo_bin.mkdir(parents=True)
    executable_name = "cargo.exe" if os.name == "nt" else "cargo"
    cargo_path = cargo_bin / executable_name
    cargo_path.write_text("", encoding="utf-8")
    monkeypatch.setenv("LO2CIN4BT_RUST_HOME", str(rust_home))
    monkeypatch.delenv("CARGO_HOME", raising=False)
    monkeypatch.delenv("RUSTUP_HOME", raising=False)
    monkeypatch.setenv("PATH", os.environ.get("PATH", ""))

    _configure_preferred_rust_home()

    assert os.environ["CARGO_HOME"] == str(rust_home / "cargo")
    assert os.environ["RUSTUP_HOME"] == str(rust_home / "rustup")
    assert _preferred_command("cargo") == str(cargo_path)


def test_doctor_detects_missing_rust_release_bins(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(doctor, "ROOT", tmp_path)

    assert not doctor._rust_release_bins_ready()
