"""Daft Catalog integration for DeltaCAT.

This module provides seamless integration between DeltaCAT and Daft,
allowing users to leverage Daft's distributed DataFrame capabilities
with DeltaCAT's catalog management.
"""

from deltacat.catalog.daft.daft_catalog import DaftCatalog, DaftTable

__all__ = ["DaftCatalog", "DaftTable"]