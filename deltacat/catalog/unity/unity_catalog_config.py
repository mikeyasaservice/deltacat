"""Configuration for Unity Catalog integration."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class UnityCatalogConfig:
    """Configuration for Unity Catalog integration.
    
    This configuration class holds all necessary parameters for connecting to
    and working with Unity Catalog, including workspace credentials and
    optional compute resources.
    
    Attributes:
        workspace_url: Databricks workspace URL (e.g., https://workspace.databricks.com)
        token: Personal access token or service principal token
        catalog_name: Name of the Unity Catalog to use
        warehouse_id: Optional SQL warehouse ID for query execution
        cluster_id: Optional compute cluster ID for data operations
        additional_properties: Additional configuration properties
    """
    
    workspace_url: str
    token: str
    catalog_name: str
    warehouse_id: Optional[str] = None
    cluster_id: Optional[str] = None
    additional_properties: Dict[str, Any] = field(default_factory=dict)