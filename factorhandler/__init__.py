"""FactorHandler runtime package.

The factorhandler layer sits between dataloader and backtester.  It turns raw
data frames into audited factor score frames, but does not select assets,
manage positions, or run portfolio accounting.
"""

from .FactorHandler_factorhandler import FactorHandler, FactorHandlerError, FactorHandlerResult
from .FactorArtifactExporter_factorhandler import FactorArtifactExporter

__all__ = ["FactorArtifactExporter", "FactorHandler", "FactorHandlerError", "FactorHandlerResult"]
