<p align="center">
  <img src="media/deltacat-logo-alpha-750.png" alt="DeltaCAT Logo" style="width:55%; height:auto; text-align: center;">
</p>

DeltaCAT is a portable Pythonic Data Lakehouse powered by [Ray](https://github.com/ray-project/ray). It lets you define and manage
fast, scalable, ACID-compliant multimodal data lakes, and has been used to [successfully manage exabyte-scale enterprise
data lakes](https://aws.amazon.com/blogs/opensource/amazons-exabyte-scale-migration-from-apache-spark-to-ray-on-amazon-ec2/).

It uses the Ray distributed compute framework together with [Apache Arrow](https://github.com/apache/arrow) and
[Daft](https://github.com/Eventual-Inc/Daft) to efficiently scale common table management tasks, like petabyte-scale
merge-on-read and copy-on-write operations.

DeltaCAT provides four high-level components:
1. **Catalog**: High-level APIs to create, discover, organize, share, and manage datasets.
2. **Compute**: Distributed data management procedures to read, write, and optimize datasets.
3. **Storage**: In-memory and on-disk multimodal dataset formats.
4. **Sync**: Synchronize DeltaCAT datasets to data warehouses and other table formats.


## Getting Started

### Installation

```bash
pip install deltacat
```

For Daft integration:
```bash
pip install deltacat daft
```

### Quick Start

```python
import deltacat
import daft

# Initialize DeltaCAT
deltacat.init()

# Create a catalog
catalog = deltacat.Catalog()

# Use with Daft for distributed DataFrame operations
daft_catalog = deltacat.DaftCatalog(catalog, "my_catalog")
daft.attach_catalog(daft_catalog, "my_catalog")

# Create a table with Daft
df = daft.from_pydict({"id": [1, 2, 3], "value": ["a", "b", "c"]})
daft_catalog.create_table("my_table", df)

# Query with unified compute engine
from deltacat.compute.engine import create_engine

engine = create_engine()
result = engine.execute("SELECT * FROM my_table")  # Automatically uses optimal engine
```

For more examples, see our [examples directory](https://github.com/ray-project/deltacat/tree/2.0/deltacat/examples/).

## Features

### Unified Compute Engine
DeltaCAT automatically routes queries to the optimal compute engine:
- **DuckDB**: For small to medium queries (<100GB)
- **Daft**: For large-scale distributed processing
- **Ray**: For ML workloads and Python UDFs

### Multi-Format Support
- **Iceberg**: Full read/write support with time travel
- **Delta Lake**: Native integration (coming soon)
- **Parquet**: High-performance columnar storage

### Enterprise Ready
- **Unity Catalog**: First-class integration for governance
- **Schema Evolution**: Add, drop, rename columns without rewriting data
- **Partition Pruning**: Automatic query optimization
