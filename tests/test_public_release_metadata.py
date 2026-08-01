from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RELEASE_VERSION = "2.2.1"
LICENSE_ID = "CC-BY-NC-4.0"
UV_VERSION = "0.11.32"
RETIRED_REQUIREMENTS_FILES = (
    "requirements.txt",
    "requirements.lock",
    "requirements-dev.txt",
    "requirements-dev.lock",
    "requirements-brokers.txt",
    "requirements-brokers.lock",
)


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
    assert "CC BY-NC 4.0" in (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
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


def test_python_installation_route_uses_locked_uv_project() -> None:
    issues: list[str] = []
    python_version_path = PROJECT_ROOT / ".python-version"
    uv_lock_path = PROJECT_ROOT / "uv.lock"
    pyproject = _read_toml("pyproject.toml")
    project = pyproject["project"]
    uv = pyproject.get("tool", {}).get("uv", {})
    groups = pyproject.get("dependency-groups", {})

    if not python_version_path.is_file():
        issues.append("missing .python-version")
    elif python_version_path.read_text(encoding="utf-8").strip() != "3.12":
        issues.append(".python-version must select Python 3.12")

    if not uv_lock_path.is_file():
        issues.append("missing uv.lock")

    runtime_dependencies = project.get("dependencies")
    if not isinstance(runtime_dependencies, list) or len(runtime_dependencies) != 41:
        issues.append("[project].dependencies must contain 41 runtime specs")

    dev_dependencies = groups.get("dev")
    if not isinstance(dev_dependencies, list) or len(dev_dependencies) != 10:
        issues.append("[dependency-groups].dev must contain 10 direct specs")

    broker_dependencies = groups.get("brokers")
    if not isinstance(broker_dependencies, list) or len(broker_dependencies) != 2:
        issues.append("[dependency-groups].brokers must contain 2 direct specs")

    if uv.get("package") is not False:
        issues.append("[tool.uv] package must remain false")
    if uv.get("default-groups") != []:
        issues.append("[tool.uv] default-groups must be empty")
    if uv.get("required-version") != f"=={UV_VERSION}":
        issues.append(f"[tool.uv] required-version must be =={UV_VERSION}")

    present_retired = [
        filename
        for filename in RETIRED_REQUIREMENTS_FILES
        if (PROJECT_ROOT / filename).exists()
    ]
    if present_retired:
        issues.append(
            "retired requirements files still present: " + ", ".join(present_retired)
        )

    assert not issues, "\n".join(issues)


def test_python_tooling_has_no_retired_setup_cfg_route() -> None:
    assert not (PROJECT_ROOT / "setup.cfg").exists()

    pyproject = _read_toml("pyproject.toml")
    tool_config = pyproject.get("tool", {})
    assert "ruff" in tool_config
    assert "mypy" in tool_config


def test_csv_fixtures_use_repository_owned_lf_line_endings() -> None:
    attributes = (PROJECT_ROOT / ".gitattributes").read_text(encoding="utf-8")
    assert "*.csv text eol=lf" in attributes.splitlines()


def test_generated_contract_writer_uses_platform_independent_lf() -> None:
    exporter = (PROJECT_ROOT / "backtester" / "ops" / "export.py").read_text(
        encoding="utf-8"
    )

    assert 'write_text(stable_json(payload), encoding="utf-8", newline="\\n")' in exporter


def test_repository_text_defaults_are_cross_platform_and_binary_assets_are_explicit() -> (
    None
):
    lines = (PROJECT_ROOT / ".gitattributes").read_text(encoding="utf-8").splitlines()

    assert "* text=auto eol=lf" in lines
    assert "*.ps1 text eol=crlf" in lines
    for pattern in ("*.ico binary", "*.webp binary", "*.ttf binary", "*.woff binary"):
        assert pattern in lines


def test_windows_shortcut_uses_the_tracked_public_icon() -> None:
    icon = PROJECT_ROOT / "assets" / "desktop" / "lo2cin4bt-logo.ico"
    script = (PROJECT_ROOT / "scripts" / "create_windows_shortcut.ps1").read_text(
        encoding="utf-8"
    )

    assert icon.is_file()
    assert "assets\\desktop\\lo2cin4bt-logo.ico" in script
    assert "lo2cin4-logo.ico" not in script
    assert "SourceLogoPath" not in script
    assert "LO2CIN4BT_SOURCE_LOGO" not in script


def test_obsolete_parameter_matrix_readme_media_are_retired() -> None:
    retired = {
        Path("assets/readme/en/06-parameter-matrix.png"),
        Path("assets/readme/zh-Hant/06-parameter-matrix.png"),
        Path("assets/readme/full/en/06-metrics-parameter-matrix-full.png"),
        Path("assets/readme/full/zh-Hant/06-metrics-parameter-matrix-full.png"),
        Path("assets/readme/scroll/en/06-metrics-parameter-matrix-scroll.gif"),
        Path("assets/readme/scroll/zh-Hant/06-metrics-parameter-matrix-scroll.gif"),
    }
    ignore = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")

    for relative in retired:
        assert not (PROJECT_ROOT / relative).exists(), relative
        assert relative.as_posix() not in ignore


def test_ci_has_minimal_permissions_and_pinned_actions() -> None:
    ci = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert re.search(r"(?m)^permissions:\n  contents: read$", ci)
    action_refs = re.findall(r"uses:\s+[^@\s]+@([^\s#]+)", ci)
    assert action_refs
    assert all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in action_refs)
    assert (
        "cargo +1.96.0 test --manifest-path rust/lo2cin4bt_core/Cargo.toml "
        "--locked --quiet"
    ) in ci
    assert (
        "cargo +1.96.0 build --manifest-path rust/lo2cin4bt_core/Cargo.toml "
        "--release --bins --locked"
    ) in ci


def test_frontend_does_not_depend_on_external_browser_router() -> None:
    package = _read_json("plotter/web/package.json")
    package_lock = _read_json("plotter/web/package-lock.json")

    assert "react-router-dom" not in package.get("dependencies", {})
    assert "node_modules/react-router-dom" not in package_lock["packages"]
