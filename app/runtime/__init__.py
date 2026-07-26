"""App runtime services for lo2cin4bt."""

__all__ = ["AppRegistry", "AppJobManager", "AppRuntimeService"]


def __getattr__(name: str):
    if name == "AppRegistry":
        from .registry import AppRegistry

        return AppRegistry
    if name in {"AppJobManager", "AppRuntimeService"}:
        from .runtime import AppJobManager, AppRuntimeService

        return {
            "AppJobManager": AppJobManager,
            "AppRuntimeService": AppRuntimeService,
        }[name]
    raise AttributeError(name)
