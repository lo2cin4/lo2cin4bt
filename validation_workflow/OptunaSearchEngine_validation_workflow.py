from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

import optuna
from optuna.samplers import GPSampler, NSGAIISampler, TPESampler
from optuna.trial import TrialState


@dataclass(slots=True)
class SearchSpaceField:
    name: str
    field_type: str
    low: Any = None
    high: Any = None
    step: Any = None
    choices: Optional[List[Any]] = None
    log: bool = False


class OptunaSearchEngine:
    """Thin Optuna orchestration layer for WFA/parameter research."""

    def __init__(
        self,
        optimizer_config: Optional[Dict[str, Any]] = None,
        *,
        storage_dir: Optional[Path] = None,
        logger: Any = None,
    ) -> None:
        self.optimizer_config = optimizer_config or {}
        self.storage_dir = Path(storage_dir) if storage_dir else None
        self.logger = logger

    def optimize(
        self,
        *,
        study_name: str,
        search_space: Iterable[SearchSpaceField | Dict[str, Any]],
        objective_fn: Callable[[Dict[str, Any], optuna.trial.Trial], float | List[float]],
    ) -> Dict[str, Any]:
        mode = str(self.optimizer_config.get("mode", "single_objective")).strip().lower()
        sampler_name = str(self.optimizer_config.get("sampler", "tpe")).strip().lower()
        pruner_name = str(self.optimizer_config.get("pruner", "hyperband")).strip().lower()
        n_trials = int(self.optimizer_config.get("n_trials", 50))
        timeout_seconds = self.optimizer_config.get("timeout_seconds")
        directions = self._resolve_directions(mode)

        study = optuna.create_study(
            study_name=study_name,
            directions=directions if len(directions) > 1 else None,
            direction=directions[0] if len(directions) == 1 else None,
            sampler=self._build_sampler(sampler_name),
            pruner=self._build_pruner(pruner_name),
            storage=self._storage_url(study_name),
            load_if_exists=True,
        )
        normalized_space = [self._normalize_field(field) for field in search_space]

        def wrapped_objective(trial: optuna.trial.Trial):
            params = {field.name: self._suggest(trial, field) for field in normalized_space}
            result = objective_fn(params, trial)
            return result

        study.optimize(
            wrapped_objective,
            n_trials=n_trials,
            timeout=int(timeout_seconds) if timeout_seconds else None,
        )
        completed_trials = [trial for trial in study.trials if trial.state == TrialState.COMPLETE]
        payload: Dict[str, Any] = {
            "study_name": study_name,
            "mode": mode,
            "sampler": sampler_name,
            "pruner": pruner_name,
            "n_trials": len(study.trials),
            "completed_trials": len(completed_trials),
            "failed_trials": len([trial for trial in study.trials if trial.state == TrialState.FAIL]),
            "pruned_trials": len([trial for trial in study.trials if trial.state == TrialState.PRUNED]),
            "trials": [self._serialize_trial(trial) for trial in study.trials],
        }
        if len(directions) == 1 and study.best_trial is not None:
            payload["best_params"] = study.best_trial.params
            payload["best_value"] = study.best_trial.value
        else:
            payload["pareto_front"] = [self._serialize_trial(trial) for trial in getattr(study, "best_trials", [])]
        return payload

    def _build_sampler(self, sampler_name: str):
        seed = self.optimizer_config.get("random_seed", 42)
        startup = int(self.optimizer_config.get("n_startup_trials", 20))
        multivariate = bool(self.optimizer_config.get("multivariate", True))
        if sampler_name == "nsga2":
            return NSGAIISampler(seed=seed)
        if sampler_name == "gp":
            return GPSampler(seed=seed)
        return TPESampler(
            seed=seed,
            n_startup_trials=startup,
            multivariate=multivariate,
        )

    @staticmethod
    def _build_pruner(pruner_name: str):
        if pruner_name == "median":
            return optuna.pruners.MedianPruner()
        if pruner_name == "successive_halving":
            return optuna.pruners.SuccessiveHalvingPruner()
        if pruner_name == "none":
            return optuna.pruners.NopPruner()
        return optuna.pruners.HyperbandPruner()

    def _storage_url(self, study_name: str) -> Optional[str]:
        if self.storage_dir is None:
            return None
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{(self.storage_dir / f'{study_name}.sqlite3').as_posix()}"

    @staticmethod
    def _resolve_directions(mode: str) -> List[str]:
        if mode == "multi_objective":
            return ["maximize", "maximize", "minimize"]
        return ["maximize"]

    @staticmethod
    def _normalize_field(field: SearchSpaceField | Dict[str, Any]) -> SearchSpaceField:
        if isinstance(field, SearchSpaceField):
            return field
        return SearchSpaceField(
            name=str(field["name"]),
            field_type=str(field["type"]),
            low=field.get("low"),
            high=field.get("high"),
            step=field.get("step"),
            choices=field.get("choices"),
            log=bool(field.get("log", False)),
        )

    @staticmethod
    def _suggest(trial: optuna.trial.Trial, field: SearchSpaceField):
        if field.field_type == "int":
            return trial.suggest_int(field.name, int(field.low), int(field.high), step=int(field.step or 1), log=field.log)
        if field.field_type == "float":
            return trial.suggest_float(field.name, float(field.low), float(field.high), step=field.step, log=field.log)
        if field.field_type == "categorical":
            return trial.suggest_categorical(field.name, list(field.choices or []))
        raise ValueError(f"Unsupported search space field_type={field.field_type}")

    @staticmethod
    def _serialize_trial(trial: optuna.trial.FrozenTrial) -> Dict[str, Any]:
        return {
            "number": trial.number,
            "state": trial.state.name,
            "params": dict(trial.params),
            "values": list(trial.values) if trial.values is not None else (
                [trial.value] if trial.value is not None else []
            ),
        }
