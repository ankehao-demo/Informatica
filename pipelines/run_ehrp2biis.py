"""Databricks spark_python_task entry point (adds the repo root to sys.path)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.ehrp2biis.job import main

if __name__ == "__main__":
    sys.exit(main())
