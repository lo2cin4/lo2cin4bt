from __future__ import annotations

import os
from types import SimpleNamespace

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


def _run_dependency_route_doctor(
    *,
    tmp_path,
    monkeypatch,
    capsys,
    uv_version: str | None,
    lock_returncode: int,
) -> tuple[int, str]:
    frontend_root = tmp_path / "plotter" / "web"
    (frontend_root / "dist").mkdir(parents=True)
    (frontend_root / "dist" / "index.html").write_text("", encoding="utf-8")
    (frontend_root / "package-lock.json").write_text("{}", encoding="utf-8")
    (tmp_path / ".python-version").write_text("3.12\n", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text(
        '[tool.uv]\nrequired-version = "==0.11.32"\n',
        encoding="utf-8",
    )

    versions = {
        "uv": uv_version,
        "rustc": "rustc 1.96.0",
        "cargo": "cargo 1.96.0",
    }
    monkeypatch.setattr(doctor, "ROOT", tmp_path)
    monkeypatch.setattr(doctor, "_configure_preferred_rust_home", lambda: None)
    monkeypatch.setattr(doctor, "_command_version", versions.get)
    monkeypatch.setattr(doctor, "_rust_release_bins_ready", lambda: True)
    monkeypatch.setattr(
        doctor.importlib.util,
        "find_spec",
        lambda _module: object(),
    )
    monkeypatch.setattr(
        doctor.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=lock_returncode,
            stdout="",
            stderr="uv.lock is stale" if lock_returncode else "",
        ),
    )
    monkeypatch.setattr(
        doctor.sys,
        "argv",
        ["doctor.py", "--skip-node"],
    )

    exit_code = doctor.main()
    return exit_code, capsys.readouterr().out


def test_doctor_rejects_missing_uv(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    exit_code, output = _run_dependency_route_doctor(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        uv_version=None,
        lock_returncode=0,
    )

    assert exit_code == 1
    assert "[FAIL] uv 0.11.32 is required" in output


def test_doctor_rejects_unapproved_uv_version(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    exit_code, output = _run_dependency_route_doctor(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        uv_version="uv 0.10.5",
        lock_returncode=0,
    )

    assert exit_code == 1
    assert "[FAIL] uv 0.11.32 is required; found uv 0.10.5" in output


def test_doctor_rejects_stale_uv_lock(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    exit_code, output = _run_dependency_route_doctor(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        uv_version="uv 0.11.32",
        lock_returncode=1,
    )

    assert exit_code == 1
    assert "[FAIL] uv.lock is stale" in output


def test_doctor_accepts_exact_uv_and_current_lock(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    exit_code, output = _run_dependency_route_doctor(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        uv_version="uv 0.11.32",
        lock_returncode=0,
    )

    assert exit_code == 0
    assert "[OK] uv available: uv 0.11.32" in output
    assert "[OK] uv.lock is current" in output
