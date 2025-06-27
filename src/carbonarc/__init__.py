try:
    from importlib.metadata import version, PackageNotFoundError
    try:
        __version__ = version("carbonarc")
    except PackageNotFoundError:
        __version__ = "unkown"
except ImportError:
    __version__ = "unkown"

from carbonarc.client import CarbonArcClient


__all__ = ["CarbonArcClient"]
