from __future__ import annotations

from typing import Tuple, Optional

from deltacat.catalog.model.catalog import Catalog as DCCatalog
from deltacat.catalog.model.table_definition import TableDefinition

from daft.catalog import Catalog, Identifier, Table
from daft.dataframe import DataFrame
from daft.logical.schema import Schema
from deltacat.constants import DEFAULT_NAMESPACE


class DaftCatalog(Catalog):
    """
    Wrapper class to create a Daft catalog from a DeltaCAT catalog.

    The initialization of DeltaCAT and Daft catalogs is managed in `deltacat.catalog.catalog.py`. The user
    is just expected to initialize catalogs through the DeltaCAT public interface (init / put_catalog).

    TODO (mccember) in follow up PR we need to consider how to keep the DeltaCAT Catalogs class and Daft session in sync,
      and the user-facing entrypoint to get a Daft catalog

    This class itself expects a `Catalog` and will invoke the underlying implementation
    similar to `deltacat.catalog.delegate.py`, like:
      catalog.impl.create_namespace(namespace, inner=catalog.inner)

    We cannot route calls through the higher level catalog registry / delegate since this wrapper class is at a lower
     layer and does not manage registering catalogs.
    """

    def __init__(self, catalog: DCCatalog, name: str):
        """
        Initialize given DeltaCAT catalog. This catalog is also registered with DeltaCAT (via deltacat.put_catalog) given the provided Name

        :param catalog: DeltaCAT Catalog object. If None, the catalog will be fetched from `deltacat.Catalogs`
            given the catalog name.

        :param name: Name of DeltaCAT catalog. If the name is not yet registered with `deltacat.Catalogs`,
            it will be registered upon creation to ensure that the DeltaCAT and Daft catalogs keep in sync.

        :param kwargs: Additional keyword arguments passed to deltacat.get_catalog or deltacat.put_catalog,
                       such as 'namespace' for tests.
        """
        self.dc_catalog = catalog
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    ###
    # create_*
    ###
    def create_namespace(self, identifier: Identifier | str):
        """Create a new namespace in the catalog."""
        if isinstance(identifier, Identifier):
            identifier = str(identifier)
        self.dc_catalog.impl.create_namespace(identifier, inner=self.dc_catalog.inner)

    def create_table(
        self, identifier: Identifier | str, source: Schema | DataFrame, **kwargs
    ) -> Table:
        """
        Create a DeltaCAT table via Daft catalog API

        End users calling create_table through the daft table API may provide kwargs which will be plumbed through
        to deltacat create_table. For full list of keyword arguments accepted by create_table.

        Note: as of 4/22, Daft create_table does not yet support kwargs. Tracked at: https://github.com/Eventual-Inc/Daft/issues/4195

        :param identifier: Daft table identifier. Sequence of strings of the format (namespace) or (namespace, table)
            or (namespace, table, table version). If this is a string, it is a dot delimited string of the same format.
            Identifiers can be created either like Identifier("namespace", "table", "version") OR
                Identifier.from_str("namespace.table.version")

        :param source: a TableSource, either a Daft DataFrame, Daft Schema, or str (filesystem path)
        """
        if isinstance(source, DataFrame):
            return self._create_table_from_df(identifier, source)
        elif isinstance(source, Schema):
            return self._create_table_from_schema(identifier, source)
        else:
            raise Exception(
                f"Expected table source to be Schema or DataFrame. Found: {type(source)}"
            )

    def _create_table_from_df(
        self, ident: Identifier | str, source: DataFrame, **kwargs
    ) -> Table:
        """
        Create a table from a DataFrame.
        """
        t = self._create_table_from_schema(ident, source.schema(), **kwargs)
        # TODO (mccember) append data upon creation
        return t

    def _create_table_from_schema(
        self, ident: Identifier | str, source: Schema, **kwargs
    ) -> Table:
        """
        Create a table from a schema.
        """
        namespace, name, version = self._extract_namespace_name_version(ident)

        # Convert the Daft schema to a DeltaCAT schema
        # This is a simplified version, would need to be enhanced for production
        deltacat_schema = self._convert_schema_to_deltacat(source)

        # Create the table in DeltaCAT
        table_def = self.dc_catalog.impl.create_table(
            name,
            namespace=namespace,
            version=version,
            schema=deltacat_schema,
            inner=self.dc_catalog.inner,
            **kwargs,
        )

        return DaftTable._from_obj(table_def)

    ###
    # drop_*
    ###

    def drop_namespace(self, identifier: Identifier | str):
        """Drop a namespace from the catalog.
        
        Args:
            identifier: Namespace identifier to drop
        """
        if isinstance(identifier, Identifier):
            identifier = str(identifier)
        
        self.dc_catalog.impl.drop_namespace(
            identifier,
            inner=self.dc_catalog.inner
        )

    def drop_table(self, identifier: Identifier | str):
        """Drop a table from the catalog.
        
        Args:
            identifier: Table identifier to drop
        """
        namespace, table, _ = self._extract_namespace_name_version(identifier)
        
        self.dc_catalog.impl.drop_table(
            table,
            namespace=namespace,
            inner=self.dc_catalog.inner
        )

    ###
    # get_*
    ###

    def get_table(self, identifier: Identifier | str, **kwargs) -> Table:
        namespace, table, version = self._extract_namespace_name_version(identifier)

        table_def = self.dc_catalog.impl.get_table(
            table,
            namespace=namespace,
            table_version=version,
            inner=self.dc_catalog.inner,
            **kwargs,
        )

        if not table_def:
            raise ValueError(f"Table {identifier} not found")

        return DaftTable._from_obj(table_def)

    ###
    # list_*
    ###

    def list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """List all namespaces in the catalog.
        
        Args:
            pattern: Optional pattern to filter namespaces (supports wildcards)
            
        Returns:
            List of namespace identifiers
        """
        from deltacat.catalog import list_namespaces as dc_list_namespaces
        
        result = dc_list_namespaces(catalog=self.dc_catalog)
        namespaces = []
        
        for ns in result.all_items():
            # Apply pattern filtering if provided
            if pattern is None or self._matches_pattern(ns.name, pattern):
                namespaces.append(Identifier(ns.name))
        
        return namespaces

    def list_tables(self, pattern: str | None = None) -> list[str]:
        """List all tables in the catalog.
        
        Args:
            pattern: Optional pattern to filter tables (supports wildcards)
            
        Returns:
            List of table names
        """
        from deltacat.catalog import list_tables as dc_list_tables
        
        tables = []
        # List tables from all namespaces
        namespaces = self.list_namespaces()
        
        for ns_ident in namespaces:
            namespace = str(ns_ident)
            result = dc_list_tables(namespace=namespace, catalog=self.dc_catalog)
            
            for table_def in result.all_items():
                table_name = f"{namespace}.{table_def.table.name}"
                # Apply pattern filtering if provided
                if pattern is None or self._matches_pattern(table_name, pattern):
                    tables.append(table_name)
        
        return tables
    
    def _matches_pattern(self, name: str, pattern: str) -> bool:
        """Check if name matches the given pattern.
        
        Args:
            name: Name to check
            pattern: Pattern with optional wildcards (* and ?)
            
        Returns:
            True if name matches pattern
        """
        import fnmatch
        return fnmatch.fnmatch(name, pattern)

    def _extract_namespace_name_version(
        self, ident: Identifier | str
    ) -> Tuple[str, str, Optional[str]]:
        """
        Extract namespace, name,version from identifier

        Returns a 3-tuple. If no namespace is provided, uses DeltaCAT defualt namespace
        """
        default_namespace = DEFAULT_NAMESPACE

        if isinstance(ident, str):
            ident = Identifier.from_str(ident)

        if isinstance(ident, Identifier):
            if len(ident) == 1:
                return (default_namespace, ident[0], None)
            elif len(ident) == 2:
                return (ident[0], ident[1], None)
            elif len(ident) == 3:
                return (ident[0], ident[1], ident[2])
            else:
                raise ValueError(
                    f"Expected table identifier to be in format (table) or (namespace, table)"
                    f"or (namespace, table, version). Found: {ident}"
                )

    def _convert_schema_to_deltacat(self, schema: Schema):
        """Convert Daft schema to DeltaCAT schema.
        For now, just use PyArrow schema as intermediary
        TODO look into how enhancements on schema can be propagated between Daft<=>DeltaCAT
        """
        from deltacat.storage.model.schema import Schema as DeltaCATSchema

        return DeltaCATSchema.of(schema=schema.to_pyarrow_schema())

    ###
    # Private abstract method implementations
    # These are required by the Daft Catalog base class
    ###
    
    def _create_namespace(self, identifier: Identifier | str):
        """Private method for creating namespace - delegates to public method."""
        return self.create_namespace(identifier)
    
    def _create_table(self, identifier: Identifier | str, source: Schema | DataFrame, **kwargs) -> Table:
        """Private method for creating table - delegates to public method."""
        return self.create_table(identifier, source, **kwargs)
    
    def _drop_namespace(self, identifier: Identifier | str):
        """Private method for dropping namespace - delegates to public method."""
        return self.drop_namespace(identifier)
    
    def _drop_table(self, identifier: Identifier | str):
        """Private method for dropping table - delegates to public method."""
        return self.drop_table(identifier)
    
    def _get_table(self, identifier: Identifier | str, **kwargs) -> Table:
        """Private method for getting table - delegates to public method."""
        return self.get_table(identifier, **kwargs)
    
    def _has_namespace(self, identifier: Identifier | str) -> bool:
        """Check if namespace exists."""
        if isinstance(identifier, Identifier):
            identifier = str(identifier)
        
        try:
            # Check if we can list tables in this namespace
            from deltacat.catalog import list_tables as dc_list_tables
            result = dc_list_tables(namespace=identifier, catalog=self.dc_catalog)
            return True
        except:
            return False
    
    def _has_table(self, identifier: Identifier | str) -> bool:
        """Check if table exists."""
        try:
            self.get_table(identifier)
            return True
        except:
            return False
    
    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        """Private method for listing namespaces - delegates to public method."""
        return self.list_namespaces(pattern)
    
    def _list_tables(self, pattern: str | None = None) -> list[str]:
        """Private method for listing tables - delegates to public method."""
        return self.list_tables(pattern)


class DaftTable(Table):
    """
    Wrapper class to create a Daft table from a DeltaCAT table
    """

    _inner: TableDefinition

    _read_options = set()
    _write_options = set()

    def __init__(self, inner: TableDefinition):
        self._inner = inner

    @property
    def name(self) -> str:
        """Return the table name."""
        return self._inner.table_version.table_name

    @staticmethod
    def _from_obj(obj: object) -> DaftTable:
        """Returns a DeltaCATTable if the given object can be adapted so."""
        if isinstance(obj, TableDefinition):
            t = DaftTable.__new__(DaftTable)
            t._inner = obj
            return t
        raise ValueError(f"Unsupported DeltaCAT table type: {type(obj)}")

    def read(self, **options) -> DataFrame:
        raise NotImplementedError("Not implemented")

    def write(self, df: DataFrame | object, mode: str = "append", **options):
        raise NotImplementedError("Not implemented")
    
    def append(self, df: DataFrame | object, **options):
        """Append data to the table."""
        self.write(df, mode="append", **options)
    
    def overwrite(self, df: DataFrame | object, **options):
        """Overwrite the table with new data."""
        self.write(df, mode="overwrite", **options)
    
    def schema(self) -> Schema:
        """Return the table schema."""
        # Convert DeltaCAT schema to Daft schema
        if self._inner.table_version.properties.schema:
            dc_schema = self._inner.table_version.properties.schema
            # Convert to PyArrow schema first, then to Daft schema
            pa_schema = dc_schema.to_pyarrow_schema()
            from daft import Schema as DaftSchema
            return DaftSchema.from_pyarrow_schema(pa_schema)
        else:
            # Return empty schema if none exists
            from daft import Schema as DaftSchema
            import pyarrow as pa
            return DaftSchema.from_pyarrow_schema(pa.schema([]))
