"""Databricks spark_python_task entry point for the dual-run reconciliation."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reconcile.reconcile import main

if __name__ == "__main__":
    sys.exit(main())
