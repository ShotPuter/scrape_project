"""Resolve human-readable publication category names from the SEPREC catalog."""

from __future__ import annotations

from typing import Protocol

from src.models.publication import CatalogItem
from src.utils.logger import setup_logger

logger = setup_logger("category_resolver")

MAIN_CATEGORY_NAMES: dict[str, str] = {"1": "INSTRUMENTOS", "2": "CONVOCATORIAS Y AVISOS", "3": "MEMORIAS", "4": "BALANCES"}


class CatalogProvider(Protocol):
    """Protocol for fetching publication catalog items."""

    async def get_catalogs(self) -> list[CatalogItem]:
        """Retrieve parametric catalog definitions from SEPREC API."""
        ...


class CategoryResolver:
    """Caches and resolves human-readable category and subcategory names."""

    def __init__(self, client: CatalogProvider) -> None:
        self.client = client
        self._cached_catalogs: list[CatalogItem] | None = None

    async def resolve(self, category: str, sub_category: str) -> tuple[str | None, str | None]:
        """
        Map category and subcategory codes to human-readable names.

        Args:
            category: High-level category code (e.g., '1', '2').
            sub_category: Granular parametric subcategory code (e.g., '1001').

        Returns:
            Tuple of (category_name, subcategory_name).
        """
        catalogs = await self._load_catalogs()

        subcategory_names = {
            item.subcategory_code: item.subcategory_name
            for item in catalogs
            if item.subcategory_code and item.subcategory_name
        }
        category_names = {
            item.category_code: item.category_name for item in catalogs if item.category_code and item.category_name
        }

        resolved_category = category_names.get(category) or MAIN_CATEGORY_NAMES.get(category)
        resolved_subcategory = subcategory_names.get(sub_category) if sub_category else None

        return resolved_category, resolved_subcategory

    async def _load_catalogs(self) -> list[CatalogItem]:
        """Fetch catalogs from remote client or return cached in-memory list."""
        if self._cached_catalogs is not None:
            return self._cached_catalogs

        try:
            self._cached_catalogs = await self.client.get_catalogs()
        except Exception as error:
            logger.warning("Failed to fetch publication category catalog", extra={"error": str(error)})
            self._cached_catalogs = []

        return self._cached_catalogs
