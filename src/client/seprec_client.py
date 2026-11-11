"""Compatibility alias for the SEPREC client."""
import sys
from src.sites.seprec import client as _implementation
sys.modules[__name__] = _implementation
