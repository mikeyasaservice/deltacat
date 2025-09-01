"""Unity Catalog integration for DeltaCAT.

This module provides first-class support for Unity Catalog as a catalog provider,
enabling DeltaCAT to work with Delta, Iceberg, and Hudi tables through Unity's
universal interface.
"""

from deltacat.catalog.unity.unity_catalog_config import UnityCatalogConfig

__all__ = [
    "UnityCatalogConfig",
]