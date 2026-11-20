import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "pipelines"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """A local Spark session with Delta Lake. Skips if PySpark isn't installed."""
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from telemetry.spark import get_spark

    session = get_spark("telemetry-tests")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()
