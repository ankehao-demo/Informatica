# PySpark ports of PowerCenter mappings

Databricks-targeted PySpark equivalents of two mappings exported under `XML/`.
Additive: nothing under `XML/`, `Transfer Scripts/` or `Maintenance Scripts/` is
modified.

```
src/pcmigration/
  expressions.py   PowerCenter expression-language semantics (IIF, DECODE, ||, IS_NUMBER, ...)
  transforms.py    transformation shapes (Lookup, Router, Aggregator, Update Strategy)
  schemas.py       source/target schemas transcribed from the export
  config.py        RunConfig: parameter file, session properties, $PM* variables
  jobs/
    pay_calendar_set_pay_calendar.py        <- m_Pay_Calendar_Set_Pay_Calendar   (XML/Pay_Calendar)
    comptime_build_message_counters.py      <- m_COMPTIME_Build_Message_Counters (XML/COMPTIME)
tests/             pytest, deterministic fixtures, local Spark - no cluster required
```

Each job module documents the PowerCenter chain it implements, one function per
transformation, named after the transformation instance in the mapping.

```bash
pip install -r requirements.txt
pytest
```

Read [MIGRATION_NOTES.md](MIGRATION_NOTES.md) for the translation table, the
constructs with no 1:1 Databricks equivalent, and the semantic traps found - and
[`docs/INFORMATICA_INVENTORY.md`](../docs/INFORMATICA_INVENTORY.md) for the estate
these two mappings were picked from.
