import pytest
from pyspark.sql import SparkSession
import os
import ray
from pyiceberg.catalog import Catalog, load_catalog


@pytest.fixture
def spark():
    import importlib.metadata

    spark_version = ".".join(importlib.metadata.version("pyspark").split(".")[:2])
    scala_version = "2.12"
    iceberg_version = "1.6.0"

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--packages org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version},"
        f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version} pyspark-shell"
    )
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

    import tempfile
    warehouse_dir = tempfile.mkdtemp(prefix="spark_warehouse_")
    
    spark = (
        SparkSession.builder.appName("PyIceberg integration test")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_dir)
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.master", "local[2]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    return spark


@pytest.fixture(scope="session")
def session_catalog() -> Catalog:
    # Use in-memory catalog instead of REST to avoid connection errors
    import tempfile
    warehouse_path = tempfile.mkdtemp(prefix="iceberg_warehouse_")
    return load_catalog(
        "local",
        **{
            "type": "sql",
            "uri": f"sqlite:///{warehouse_path}/catalog.db",
            "warehouse": warehouse_path,
        },
    )


@pytest.fixture(autouse=True, scope="module")
def setup_ray_cluster():
    # Initialize Ray without local_mode to support async actors
    import logging
    logging.getLogger("ray").setLevel(logging.ERROR)
    
    if not ray.is_initialized():
        ray.init(
            ignore_reinit_error=True,
            num_cpus=2,
            _temp_dir="/tmp/ray",
            logging_level=logging.ERROR
        )
    yield
    if ray.is_initialized():
        ray.shutdown()
