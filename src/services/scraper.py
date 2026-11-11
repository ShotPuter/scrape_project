"""Compatibility alias for the SEPREC scraper."""
import sys
from src.sites.seprec import scraper as _implementation
sys.modules[__name__] = _implementation
