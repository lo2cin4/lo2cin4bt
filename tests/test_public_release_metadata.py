from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RELEASE_VERSION = "2.1.0"
LICENSE_ID = "CC-BY-NC-4.0"


def _read_toml(relative_path: str) -> dict:
    with (PROJECT_ROOT / relative_path).open("rb") as handle:
        return tomllib.load(handle)


def _read_json(relative_path: str) -> dict:
    return json.loads((PROJECT_ROOT / relative_path).read_text(encoding="utf-8"))


def test_product_versions_and_license_are_consistent() -> None:
    pyproject = _read_toml("pyproject.toml")
    cargo = _read_toml("rust/lo2cin4bt_core/Cargo.toml")
    package = _read_json("plotter/web/package.json")
    package_lock = _read_json("plotter/web/package-lock.json")

    assert pyproject["project"]["version"] == RELEASE_VERSION
    assert pyproject["project"]["license"] == {"file": "LICENSE"}
    assert cargo["package"]["version"] == RELEASE_VERSION
    assert cargo["package"]["license"] == LICENSE_ID
    assert package["version"] == RELEASE_VERSION
    assert package["license"] == LICENSE_ID
    assert package_lock["version"] == RELEASE_VERSION
    assert package_lock["packages"][""]["version"] == RELEASE_VERSION
    assert package_lock["packages"][""]["license"] == LICENSE_ID
    assert "Attribution-NonCommercial 4.0 International" in (
        PROJECT_ROOT / "LICENSE"
    ).read_text(encoding="utf-8")
    assert "CC BY-NC 4.0" in (PROJECT_ROOT / "README.md").read_text(
        encoding="utf-8"
    )
    assert "does not permit commercial use" in (
        PROJECT_ROOT / "README.en.md"
    ).read_text(encoding="utf-8")


def test_public_runtime_metadata_uses_release_version() -> None:
    api_source = (PROJECT_ROOT / "app/api/app.py").read_text(encoding="utf-8")
    assert f'version="{RELEASE_VERSION}"' in api_source

    skill_files = sorted((PROJECT_ROOT / "skills").glob("*/SKILL.md"))
    assert skill_files
    for skill_file in skill_files:
        frontmatter = skill_file.read_text(encoding="utf-8").split("---", 2)[1]
        assert f"version: {RELEASE_VERSION}" in frontmatter, skill_file


def test_python_installation_routes_use_hash_locked_requirements() -> None:
    for filename in (
        "requirements.lock",
        "requirements-dev.lock",
        "requirements-brokers.lock",
    ):
        lock_text = (PROJECT_ROOT / filename).read_text(encoding="utf-8")
        package_lines = [
            line
            for line in lock_text.splitlines()
            if line and not line.startswith(("#", " ", "\\", "-"))
        ]
        assert package_lines, filename
        assert all("==" in line for line in package_lines), filename
        assert "--hash=sha256:" in lock_text, filename

    setup_ps1 = (PROJECT_ROOT / "scripts/setup.ps1").read_text(encoding="utf-8")
    setup_sh = (PROJECT_ROOT / "scripts/setup.sh").read_text(encoding="utf-8")
    ci = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "requirements.lock" in setup_ps1
    assert "requirements-dev.lock" in setup_ps1
    assert "requirements-brokers.lock" in setup_ps1
    assert "requirements.lock" in setup_sh
    assert "requirements-dev.lock" in setup_sh
    assert "requirements-brokers.lock" in setup_sh
    assert "requirements-dev.lock" in ci


def test_ci_has_minimal_permissions_and_pinned_actions() -> None:
    ci = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert re.search(r"(?m)^permissions:\n  contents: read$", ci)
    action_refs = re.findall(r"uses:\s+[^@\s]+@([^\s#]+)", ci)
    assert action_refs
    assert all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in action_refs)


def test_frontend_does_not_depend_on_external_browser_router() -> None:
    package = _read_json("plotter/web/package.json")
    package_lock = _read_json("plotter/web/package-lock.json")

    assert "react-router-dom" not in package.get("dependencies", {})
    assert "node_modules/react-router-dom" not in package_lock["packages"]
