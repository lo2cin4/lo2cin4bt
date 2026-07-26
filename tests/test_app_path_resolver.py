from utils.path_resolver import build_app_run_paths, ensure_app_outputs_structure


def test_ensure_app_outputs_structure_creates_canonical_layout(tmp_path) -> None:
    paths = ensure_app_outputs_structure(tmp_path)

    assert paths["root"] == tmp_path / "outputs" / "app"
    assert paths["run_registry"].is_dir()
    assert paths["run_snapshots"].is_dir()
    assert paths["artifact_manifests"].is_dir()
    assert paths["chart_payloads"].is_dir()
    assert paths["ai_review"].is_dir()
    assert paths["stage_status"].is_dir()
    assert paths["latest_runs"].is_file()
    assert paths["latest_runs"].read_text(encoding="utf-8").strip() == "[]"
    for legacy_name in ("backtester", "metricstracker", "plotter", "statanalyser", "wfanalyser"):
        assert not (tmp_path / "outputs" / legacy_name).exists()


def test_build_app_run_paths_returns_stable_run_local_paths(tmp_path) -> None:
    paths = build_app_run_paths(tmp_path, "20260414_b7c3d8129f4a")

    assert paths["run_registry"] == (
        tmp_path
        / "outputs"
        / "app"
        / "run_registry"
        / "20260414_b7c3d8129f4a.json"
    )
    assert paths["artifact_manifest"] == (
        tmp_path
        / "outputs"
        / "app"
        / "artifact_manifests"
        / "20260414_b7c3d8129f4a.json"
    )
    assert paths["stage_status"] == (
        tmp_path
        / "outputs"
        / "app"
        / "stage_status"
        / "20260414_b7c3d8129f4a.json"
    )
    assert paths["snapshot_dir"].is_dir()
    assert paths["chart_payload_dir"].is_dir()
    assert paths["ai_review_dir"].is_dir()
    assert paths["ai_readable_output"] == (
        tmp_path
        / "outputs"
        / "app"
        / "ai_review"
        / "20260414_b7c3d8129f4a"
        / "ai_review_pack.json"
    )
    assert paths["dataloader_health"] == paths["snapshot_dir"] / "dataloader_health.json"
    assert paths["data_lineage_manifest"] == paths["snapshot_dir"] / "data_lineage_manifest.json"
    assert "records" not in str(paths["run_registry"]).lower()


def test_resolve_app_run_paths_does_not_create_output_tree(tmp_path) -> None:
    paths = build_app_run_paths(tmp_path, "missing-run", create=False)

    assert paths["chart_payload_dir"] == (
        tmp_path / "outputs" / "app" / "chart_payloads" / "missing-run"
    )
    assert not (tmp_path / "outputs").exists()


def test_build_app_run_paths_rejects_empty_run_id(tmp_path) -> None:
    try:
        build_app_run_paths(tmp_path, "   ")
    except ValueError as exc:
        assert "run_id is required" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected build_app_run_paths to reject empty run ids")


def test_build_app_run_paths_rejects_path_traversal_run_id(tmp_path) -> None:
    for bad_run_id in ["../escape", "..\\escape", "nested/run", "C:escape"]:
        try:
            build_app_run_paths(tmp_path, bad_run_id)
        except ValueError as exc:
            assert "plain filename-safe" in str(exc)
        else:  # pragma: no cover
            raise AssertionError(f"expected build_app_run_paths to reject {bad_run_id}")
