import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "airflow" / "dags"))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    os.environ.setdefault("SPARK_JARS_REPOSITORIES", "https://maven-central.storage-download.googleapis.com/maven2/")
    from pipelines.ehrp2biis.job import get_spark

    s = get_spark()
    yield s
    s.stop()
