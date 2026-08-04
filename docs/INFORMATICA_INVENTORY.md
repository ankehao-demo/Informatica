# Informatica PowerCenter Inventory

Generated from the repository exports in `XML/` by `tools/inventory_powercenter.py`. Re-run that script after any repository re-export; do not hand-edit this file.

## Summary

- **Export files (`XML/`):** 11
- **PowerCenter repository folders:** 6 (COMP_TIME, CPM, EHRP2BIIS, LES, Pay_Calendar, Pseudossn) — several exports were taken from the same folder, so the export file is the unit of scoping below
- **Workflows:** 11 (one per export, all `ON DEMAND` — externally scheduled, see below)
- **Mappings:** 108
- **Sessions:** 108
- **Distinct transformation instances:** 747 across 11 transformation types

### Complexity spread

| Complexity | Mappings | Share |
| --- | ---: | ---: |
| Low | 27 | 25% |
| Medium | 56 | 52% |
| High | 22 | 20% |
| Very High | 3 | 3% |
| **Total** | **108** | |

Complexity is a weighted score over the mapping's transformation chain (Update Strategy / Normalizer weigh most, Expression / Filter least), plus its source and target count, plus one point per 100 port-level connectors. Bands: Low `<10`, Medium `<25`, High `<45`, Very High `>=45`. The connector term matters — several mappings wire more than 1,000 ports, which is the real hand-porting cost.

### Transformation usage across all mappings

| Transformation | Instances | Mappings using it |
| --- | ---: | ---: |
| Expression | 343 | 100 |
| Lookup Procedure | 112 | 55 |
| Source Qualifier | 112 | 101 |
| Filter | 61 | 37 |
| Normalizer | 39 | 34 |
| Aggregator | 29 | 19 |
| Joiner | 27 | 23 |
| Router | 7 | 7 |
| Update Strategy | 7 | 6 |
| Sequence | 5 | 5 |
| Sorter | 5 | 5 |

### Mappings per export

| Export | Repository folder | Mappings | Low | Medium | High | Very High |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `XML/COMPTIME` | COMP_TIME | 3 | 1 | 2 | 0 | 0 |
| `XML/CPM` | CPM | 15 | 2 | 3 | 8 | 2 |
| `XML/CPM_AFPS` | CPM | 17 | 8 | 6 | 3 | 0 |
| `XML/CPM_CDC` | CPM | 6 | 2 | 3 | 1 | 0 |
| `XML/CPM_NIH` | CPM | 6 | 2 | 3 | 1 | 0 |
| `XML/CPM_OIG` | CPM | 4 | 0 | 3 | 1 | 0 |
| `XML/EHRP2BIIS_UPDATE` | EHRP2BIIS | 1 | 0 | 0 | 1 | 0 |
| `XML/FDA_Leave` | CPM | 10 | 3 | 4 | 3 | 0 |
| `XML/LES` | LES | 32 | 4 | 26 | 1 | 1 |
| `XML/Pay_Calendar` | Pay_Calendar | 4 | 3 | 0 | 1 | 0 |
| `XML/Pseudossn` | Pseudossn | 10 | 2 | 6 | 2 | 0 |

### Highest-complexity mappings

| Score | Export | Mapping | Chain |
| ---: | --- | --- | --- |
| 69 | `XML/CPM` | `m_CPM_Build_Message_Counters` | Expression x9, Aggregator x4, Lookup Procedure x4, Joiner x3, Normalizer x3, Source Qualifier x3 |
| 58 | `XML/LES` | `m_LES_Build_Message_Counters` | Expression x14, Aggregator x4, Joiner x3, Source Qualifier x3, Normalizer x2, Filter, Lookup Procedure |
| 49 | `XML/CPM` | `m_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL` | Expression x9, Lookup Procedure x3, Filter x2, Source Qualifier x2, Joiner, Normalizer |
| 43 | `XML/FDA_Leave` | `m_0500_PM_FDA_IO_Counter` | Expression x8, Lookup Procedure x3, Aggregator x2, Normalizer x2, Source Qualifier x2, Filter, Joiner |
| 41 | `XML/EHRP2BIIS_UPDATE` | `m_EHRP2BIIS_UPDATE` | Lookup Procedure x9, Expression x3, Source Qualifier |
| 36 | `XML/CPM_AFPS` | `m_CPM_AFPS_0800_Build_Message_Counters` | Expression x5, Aggregator x2, Normalizer x2, Source Qualifier x2, Joiner |
| 32 | `XML/CPM_AFPS` | `m_CPM_AFPS_0820_Build_Message_Totals` | Expression x5, Aggregator x2, Source Qualifier x2, Joiner, Normalizer |
| 31 | `XML/FDA_Leave` | `m_0200_PM_FDA_Create_Insert_200_Rows` | Lookup Procedure x6, Expression x2, Filter, Normalizer, Source Qualifier |
| 31 | `XML/Pseudossn` | `m_Pseudossn_Load_Pseudossn_Tbl` | Expression x6, Filter x2, Lookup Procedure x2, Normalizer, Router, Sorter, Source Qualifier |
| 30 | `XML/CPM` | `m_CPM_Load_CPM_NEWPAY_STG_TYPE_3_TBL` | Expression x3, Lookup Procedure x3, Aggregator, Source Qualifier |

### Ported to PySpark so far

| Mapping | Export | Why it is representative | Port |
| --- | --- | --- | --- |
| `m_Pay_Calendar_Set_Pay_Calendar` | `XML/Pay_Calendar` | the only Router + Update Strategy pair in the estate, two lookups (one a date-range non-equijoin) and parameter-file driven branching | [`pyspark/src/pcmigration/jobs/pay_calendar_set_pay_calendar.py`](../pyspark/src/pcmigration/jobs/pay_calendar_set_pay_calendar.py) |
| `m_COMPTIME_Build_Message_Counters` | `XML/COMPTIME` | the message/counter shape repeated in 14 mappings and ending 8 of the 11 workflows: flat-file source, record-type filter, aggregation, lookup, SETVARIABLE and two heterogeneous targets | [`pyspark/src/pcmigration/jobs/comptime_build_message_counters.py`](../pyspark/src/pcmigration/jobs/comptime_build_message_counters.py) |

The remaining mappings are not ported here; see `pyspark/MIGRATION_NOTES.md` for the translation patterns and the semantic traps that apply across the estate.

## Mappings by export

### `XML/COMPTIME` (repository folder `COMP_TIME`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_COMPTIME_Build_Message_Counters` | U0287D01 | COMPTIME_MESSAGE_FILE, COUNTER_TBL | Expression x6, Aggregator, Filter, Lookup Procedure, Source Qualifier | 38 | 17 | Medium |
| `m_COMPTIME_Current_Pay_Period` | PAY_PERIOD | COMP_TIME_DATE_FILE | Expression x2, Source Qualifier | 15 | 5 | Low |
| `m_COMPTIME_Load_COMP_TIME_DAILY_TBL` | U0287D01 | COMP_TIME_DAILY_TBL | Expression x3, Filter, Lookup Procedure, Source Qualifier | 83 | 10 | Medium |

Mapping parameters / variables in this export:

- `m_COMPTIME_Build_Message_Counters`: variables `$$MAP_MESSAGE`, `$$MAP_SUBJECT`
- `m_COMPTIME_Current_Pay_Period`: variables `$$MAP_PP_YEAR_NUM`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`

<details><summary>Mapping descriptions from the repository</summary>

- **`m_COMPTIME_Build_Message_Counters`** — This mapping gets the count of detail records on the CompTime file that was processed and loads it to the Counters Table.
- **`m_COMPTIME_Current_Pay_Period`** — This mapping returns the Current Pay Period from the Pay Period table.

</details>

### `XML/CPM` (repository folder `CPM`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_CPM_Build_Message_Counters` | CPM_NEWPAY_TBL, ERROR_TBL, PAYMASTER_THREE | COUNTER_TBL, CPM_MESSAGE_FILE | Expression x9, Aggregator x4, Lookup Procedure x4, Joiner x3, Normalizer x3, Source Qualifier x3 | 768 | 69 | Very High |
| `m_CPM_Current_Pay_Period` | PAY_PERIOD | CPM_PAY_PERIOD_DATE_FILE | Expression x2, Source Qualifier | 15 | 5 | Low |
| `m_CPM_Load_CPM_MER_Staging_Tables` | MER_FILE | CPM_MER_DETAIL_STG_TBL, CPM_MER_HEADER_STG_TBL | Expression x5, Lookup Procedure x2, Normalizer, Router | 541 | 26 | High |
| `m_CPM_Load_CPM_NEWPAY_STG_ALT_TBL` | CPM_PM3_STG_TBL | CPM_NEWPAY_STG_ALT_TBL, ERROR_TBL | Expression x6, Filter x2, Aggregator, Lookup Procedure, Normalizer, Source Qualifier | 284 | 24 | Medium |
| `m_CPM_Load_CPM_NEWPAY_STG_DETAIL_TBL` | CPM_PM3_STG_TBL | CPM_NEWPAY_STG_DETAIL_TBL | Expression x3, Aggregator, Source Qualifier | 125 | 10 | Medium |
| `m_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL` | CPM_PM1_STG_TBL, CPM_PM2_STG_TBL, CPM_YTD_DETAIL_STG_TBL, PSEUDOSSN_TBL | CPM_NEWPAY_STG_TYPE_1_2_TBL, ERROR_TBL | Expression x9, Lookup Procedure x3, Filter x2, Source Qualifier x2, Joiner, Normalizer | 1436 | 49 | Very High |
| `m_CPM_Load_CPM_NEWPAY_STG_TYPE_3_FDR_TBL` | CPM_PM3_STG_TBL | CPM_NEWPAY_STG_TYPE_3_FDR_TBL | Expression x6, Lookup Procedure x2, Aggregator, Source Qualifier | 805 | 26 | High |
| `m_CPM_Load_CPM_NEWPAY_STG_TYPE_3_TBL` | CPM_NEWPAY_STG_TYPE_3_FDR_TBL | CPM_NEWPAY_STG_TYPE_3_TBL | Expression x3, Lookup Procedure x3, Aggregator, Source Qualifier | 1230 | 30 | High |
| `m_CPM_Load_CPM_NEWPAY_STG_YTD_STATE_TBL` | CPM_YTD_STATE_STG_TBL | CPM_NEWPAY_STG_YTD_STATE_TBL | Expression x4, Aggregator, Source Qualifier | 87 | 10 | Medium |
| `m_CPM_Load_CPM_PAD_Staging_Tables` | PAD_FILE | CPM_PAD_DETAIL_STG_TBL, CPM_PAD_HEADER_STG_TBL | Expression x5, Lookup Procedure x2, Normalizer, Router | 805 | 29 | High |
| `m_CPM_Load_CPM_PMR_Staging_Tables` | PAYMASTER_FILE | CPM_PM1_STG_TBL, CPM_PM2_STG_TBL, CPM_PM3_STG_TBL, CPM_PMH_STG_TBL | Expression x7, Lookup Procedure x2, Normalizer, Router | 592 | 30 | High |
| `m_CPM_Load_CPM_YTD_Staging_Tables` | YTD_FILE | CPM_YTD_DETAIL_STG_TBL, CPM_YTD_HEADER_STG_TBL, CPM_YTD_STATE_STG_TBL | Expression x6, Lookup Procedure x2, Normalizer, Router | 615 | 29 | High |
| `m_CPM_Load_FDR_CPM_NEWPAY_TBL` | CPM_NEWPAY_STG_TYPE_1_2_TBL, CPM_NEWPAY_STG_TYPE_3_FDR_TBL | CPM_NEWPAY_TBL | Expression x3, Lookup Procedure, Source Qualifier | 2007 | 30 | High |
| `m_CPM_Load_PMR_To_CPM_NEWPAY_TBL` | CPM_NEWPAY_STG_TYPE_1_2_TBL, CPM_NEWPAY_STG_TYPE_3_TBL | CPM_NEWPAY_TBL | Expression x3, Source Qualifier | 1985 | 26 | High |
| `m_Generic_Mapping` | HI_GENERIC_SRC_TBL | GENERIC_TARGET_FILE | Source Qualifier | 2 | 3 | Low |

Mapping parameters / variables in this export:

- `m_CPM_Build_Message_Counters`: variables `$$MAP_SUBJECT`, `$$MAP_MESSAGE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Current_Pay_Period`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_PP_YEAR_NUM`
- `m_CPM_Load_CPM_MER_Staging_Tables`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_CPM_NEWPAY_STG_ALT_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_CPM_NEWPAY_STG_DETAIL_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_CPM_NEWPAY_STG_TYPE_3_FDR_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_CPM_NEWPAY_STG_TYPE_3_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_CPM_NEWPAY_STG_YTD_STATE_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_CPM_PAD_Staging_Tables`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_CPM_PMR_Staging_Tables`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_CPM_YTD_Staging_Tables`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_FDR_CPM_NEWPAY_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_Load_PMR_To_CPM_NEWPAY_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`

<details><summary>Mapping descriptions from the repository</summary>

- **`m_CPM_Current_Pay_Period`** — This mapping returns the Current Pay Period from the Pay Period table.

</details>

### `XML/CPM_AFPS` (repository folder `CPM`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_CPM_AFPS_0010_Set_CPM_Calendar` | PAY_PERIOD | CPM_AFPS_PAY_PERIOD_CAL_FILE | Expression x6, Lookup Procedure x2, Source Qualifier | 36 | 15 | Medium |
| `m_CPM_AFPS_0025_Set_Pay_Calendar` | PAY_PERIOD | CPM_AFPS_PAY_PERIOD_FILE | Expression x4, Lookup Procedure x3, Source Qualifier | 38 | 16 | Medium |
| `m_CPM_AFPS_0050_Update_CPM_CYCLE_TBL` | CPM_CYCLE_TBL | CPM_CYCLE_TBL | Expression x2, Lookup Procedure, Source Qualifier, Update Strategy | 31 | 12 | Medium |
| `m_CPM_AFPS_0100_Data_Seperate` | CPM_NEWPAY_TBL, PAY_PERIOD | HI_AFPS_FEEDER_TBL | Expression x3, Sorter, Source Qualifier | 1519 | 23 | Medium |
| `m_CPM_AFPS_0200_Debridge_To_FEEDER_FLAT` | HI_AFPS_FEEDER_TBL | feeder_FEEDER_RECORD | Source Qualifier | 556 | 8 | Low |
| `m_CPM_AFPS_0300_Gross_Exp_Report` | HI_AFPS_FEEDER_TBL | HI_GROSS_EXP_TBL | Expression, Source Qualifier | 377 | 7 | Low |
| `m_CPM_AFPS_0400_Crossfoot_Errors` | CPM_PM3_STG_TBL, HI_GROSS_EXP_TBL | ERROR_TBL | Expression x2, Source Qualifier x2, Joiner, Lookup Procedure | 76 | 13 | Medium |
| `m_CPM_AFPS_0500_Crossfoot_Message_Header` | PAY_PERIOD | CPM_AFPS_MESSAGE_FILE | Expression x3, Source Qualifier | 16 | 6 | Low |
| `m_CPM_AFPS_0600_Crossfoot_Message_Details` | ERROR_TBL | CPM_AFPS_CROSSFOOT_FILE | Expression x3, Source Qualifier | 24 | 6 | Low |
| `m_CPM_AFPS_0700_Crossfoot_Message_Summary_Counts` | ERROR_TBL, HI_AFPS_FEEDER_TBL | CPM_AFPS_MESSAGE_COUNTS_FILE | Expression x4, Aggregator x2, Source Qualifier x2, Joiner, Normalizer | 426 | 26 | High |
| `m_CPM_AFPS_0720_Crossfoot_Message_Gross_Expend` | HI_AFPS_FEEDER_TBL | CPM_AFPS_MESSAGE_COUNTS_TOT_FILE | Expression x3, Aggregator, Source Qualifier | 384 | 12 | Medium |
| `m_CPM_AFPS_0760_Concatenate_Crossfoot_Files` | HI_GENERIC_SRC_TBL | GENERIC_TARGET_FILE | Source Qualifier | 2 | 3 | Low |
| `m_CPM_AFPS_0800_Build_Message_Counters` | CPM_NEWPAY_TBL, HI_AFPS_FEEDER_TBL | AFPS_COUNTER_TBL, CPM_AFPS_MESSAGE_COUNTS_FILE | Expression x5, Aggregator x2, Normalizer x2, Source Qualifier x2, Joiner | 832 | 36 | High |
| `m_CPM_AFPS_0820_Build_Message_Totals` | CPM_NEWPAY_TBL, HI_AFPS_FEEDER_TBL | AFPS_COUNTER_TBL, CPM_AFPS_MESSAGE_COUNTS_TOT_FILE | Expression x5, Aggregator x2, Source Qualifier x2, Joiner, Normalizer | 832 | 32 | High |
| `m_CPM_AFPS_0860_Concatenate_Counts_Files` | HI_GENERIC_SRC_TBL | GENERIC_TARGET_FILE | Source Qualifier | 2 | 3 | Low |
| `m_CPM_AFPS_0900_Build_Message` | PAY_PERIOD | CPM_AFPS_MESSAGE_FILE | Expression x2, Source Qualifier | 17 | 5 | Low |
| `m_CPM_AFPS_1000_Send_Report` | HI_GENERIC_SRC_TBL | GENERIC_TARGET_FILE | Source Qualifier | 2 | 3 | Low |

Mapping parameters / variables in this export:

- `m_CPM_AFPS_0010_Set_CPM_Calendar`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_PP_END_YEAR_DEC_FMT`, `$$MAP_PP_NUM_DEC_FMT`, `$$MAP_PP_YEAR_NUM`
- `m_CPM_AFPS_0025_Set_Pay_Calendar`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_PP_END_YEAR_LAST_DIGIT`
- `m_CPM_AFPS_0050_Update_CPM_CYCLE_TBL`: variables `$$MAP_CYCLE_ID`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_AFPS_0100_Data_Seperate`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_AFPS_0300_Gross_Exp_Report`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_AFPS_0400_Crossfoot_Errors`: variables `$$MAP_CYCLE_ID`
- `m_CPM_AFPS_0500_Crossfoot_Message_Header`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_SUBJECT`, `$$MAP_MESSAGE`
- `m_CPM_AFPS_0600_Crossfoot_Message_Details`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_CYCLE_ID`
- `m_CPM_AFPS_0700_Crossfoot_Message_Summary_Counts`: variables `$$MAP_SUBJECT`, `$$MAP_MESSAGE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_CYCLE_ID`
- `m_CPM_AFPS_0720_Crossfoot_Message_Gross_Expend`: variables `$$MAP_SUBJECT`, `$$MAP_MESSAGE`
- `m_CPM_AFPS_0800_Build_Message_Counters`: variables `$$MAP_SUBJECT`, `$$MAP_MESSAGE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_AFPS_0820_Build_Message_Totals`: variables `$$MAP_SUBJECT`, `$$MAP_MESSAGE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_AFPS_0900_Build_Message`: variables `$$MAP_MESSAGE`, `$$MAP_SUBJECT`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`

<details><summary>Mapping descriptions from the repository</summary>

- **`m_CPM_AFPS_0900_Build_Message`** — This mapping queries the Pay Calendar table for the record marked current and uses that record to build the subject and message for an email message.

</details>

### `XML/CPM_CDC` (repository folder `CPM`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_CPM_CDC_Build_Message` | CPM_NEWPAY_TBL | CPM_CDC_MESSAGE_FILE | Expression x3, Aggregator, Lookup Procedure, Source Qualifier | 480 | 16 | Medium |
| `m_CPM_CDC_Concatenate_Files` | HI_GENERIC_SRC_TBL | GENERIC_TARGET_FILE | Source Qualifier | 2 | 3 | Low |
| `m_CPM_CDC_Load_CPM_CDC_Data_File` | CPM_NEWPAY_TBL | cdcskel_WS_PAY_OUT_REC | Expression x4, Source Qualifier | 2091 | 27 | High |
| `m_CPM_CDC_Load_CPM_CDC_Header_File` | PAY_PERIOD | cdchdr_WS_CDC_HDR | Expression x2, Source Qualifier | 41 | 5 | Low |
| `m_CPM_CDC_Set_CPM_Calendar` | PAY_PERIOD | CPM_CDC_CPM_PAY_PERIOD_FILE | Expression x4, Lookup Procedure x2, Source Qualifier | 34 | 13 | Medium |
| `m_CPM_CDC_Set_Pay_Calendar` | PAY_PERIOD | CPM_CDC_PAY_PERIOD_FILE | Expression x4, Lookup Procedure x3, Source Qualifier | 37 | 16 | Medium |

Mapping parameters / variables in this export:

- `m_CPM_CDC_Build_Message`: variables `$$MAP_MESSAGE`, `$$MAP_SUBJECT`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_CDC_Load_CPM_CDC_Data_File`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_CDC_Load_CPM_CDC_Header_File`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_CDC_Set_CPM_Calendar`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_PP_YEAR_NUM`
- `m_CPM_CDC_Set_Pay_Calendar`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`

### `XML/CPM_NIH` (repository folder `CPM`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_CPM_NIH_Build_Message` | CPM_NEWPAY_TBL | CPM_NIH_MESSAGE_FILE | Expression x3, Aggregator, Lookup Procedure, Source Qualifier | 480 | 16 | Medium |
| `m_CPM_NIH_Concatenate_Files` | HI_GENERIC_SRC_TBL | GENERIC_TARGET_FILE | Source Qualifier | 2 | 3 | Low |
| `m_CPM_NIH_Load_CPM_NIH_Data_File` | CPM_NEWPAY_TBL | nihtest_NIH_PAYROLL_MASTER | Expression x4, Source Qualifier | 1971 | 26 | High |
| `m_CPM_NIH_Load_CPM_NIH_Header_File` | PAY_PERIOD | nihhdr_WS_NIH_HDR | Expression x2, Source Qualifier | 42 | 5 | Low |
| `m_CPM_NIH_Set_CPM_Calendar` | PAY_PERIOD | CPM_NIH_CPM_PAY_PERIOD_FILE | Expression x4, Lookup Procedure x2, Source Qualifier | 34 | 13 | Medium |
| `m_CPM_NIH_Set_Pay_Calendar` | PAY_PERIOD | CPM_NIH_PAY_PERIOD_FILE | Expression x4, Lookup Procedure x3, Source Qualifier | 37 | 16 | Medium |

Mapping parameters / variables in this export:

- `m_CPM_NIH_Build_Message`: variables `$$MAP_MESSAGE`, `$$MAP_SUBJECT`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_NIH_Load_CPM_NIH_Data_File`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_NIH_Load_CPM_NIH_Header_File`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_NIH_Set_CPM_Calendar`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_PP_YEAR_NUM`
- `m_CPM_NIH_Set_Pay_Calendar`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`

### `XML/CPM_OIG` (repository folder `CPM`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_CPM_OIG_Build_Message` | CPM_NEWPAY_TBL | CPM_OIG_MESSAGE_FILE | Expression x3, Aggregator, Source Qualifier | 478 | 13 | Medium |
| `m_CPM_OIG_Load_CPM_OIG_File` | CPM_NEWPAY_TBL | oigsgndec_SKPAYROLL_MASTER | Expression x4, Source Qualifier, Update Strategy | 1429 | 25 | High |
| `m_CPM_OIG_Set_CPM_Calendar` | PAY_PERIOD | CPM_OIG_CPM_PAY_PERIOD_FILE | Expression x4, Lookup Procedure x2, Source Qualifier | 34 | 13 | Medium |
| `m_CPM_OIG_Set_Pay_Calendar` | PAY_PERIOD | CPM_OIG_PAY_PERIOD_FILE | Expression x5, Lookup Procedure x3, Source Qualifier | 37 | 17 | Medium |

Mapping parameters / variables in this export:

- `m_CPM_OIG_Build_Message`: variables `$$MAP_MESSAGE`, `$$MAP_SUBJECT`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_OIG_Load_CPM_OIG_File`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_CPM_OIG_Set_CPM_Calendar`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_PP_YEAR_NUM`
- `m_CPM_OIG_Set_Pay_Calendar`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`

<details><summary>Mapping descriptions from the repository</summary>

- **`m_CPM_OIG_Load_CPM_OIG_File`** — 10/31/2018 MS: Changed the field Base Hrs in the transformation exp_Convert to ensure the absolute value of Base Hrs is passed to the target.

</details>

### `XML/EHRP2BIIS_UPDATE` (repository folder `EHRP2BIIS`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_EHRP2BIIS_UPDATE` | NWK_NEW_EHRP_ACTIONS_TBL, PS_GVT_JOB | EHRP_RECS_TRACKING_TBL, NWK_ACTION_PRIMARY_TBL, NWK_ACTION_SECONDARY_TBL | Lookup Procedure x9, Expression x3, Source Qualifier | 589 | 41 | High |

### `XML/FDA_Leave` (repository folder `CPM`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_0010_PM_FDA_Verify_File` | HI_PM_FDA_TATRAN_FLAT_FILE_NAME | CPM_FDA_PAY_PERIOD_FILE | Expression x5, Lookup Procedure x3, Aggregator, Sorter, Source Qualifier | 28 | 21 | Medium |
| `m_0020_PM_FDA_Set_CPM_Calendar` | PAY_PERIOD | CPM_FDA_CPM_PAY_PERIOD_FILE | Expression x4, Lookup Procedure x2, Source Qualifier | 33 | 13 | Medium |
| `m_0025_PM_FDA_Set_Pay_Calendar` | PAY_PERIOD | CPM_FDA_PAY_PERIOD_FILE | Expression x5, Lookup Procedure x3, Source Qualifier | 42 | 17 | Medium |
| `m_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA` | CPM_CYCLE_TBL | CPM_CYCLE_TBL | Expression x2, Lookup Procedure, Source Qualifier, Update Strategy | 31 | 12 | Medium |
| `m_0100_PM_FDA_Load_TATRAN_To_DB` | HI_PM_FDA_TATRAN_FLAT | HI_PM_FDA_TATRAN_TBL | Expression, Filter, Sorter, Source Qualifier | 34 | 6 | Low |
| `m_0150_PM_FDA_Error_Counter` | HI_PM_FDA_TATRAN_TBL | ERROR_TBL | Expression x5, Lookup Procedure x5, Filter x4, Source Qualifier | 89 | 27 | High |
| `m_0200_PM_FDA_Create_Insert_200_Rows` | HI_PM_FDA_TATRAN_TBL | HI_PM_FDA_TATRAN_TBL | Lookup Procedure x6, Expression x2, Filter, Normalizer, Source Qualifier | 303 | 31 | High |
| `m_0300_PM_FDA_Create_Output_File` | HI_PM_FDA_TATRAN_TBL | HI_PM_FDA_TATRAN_FLAT | Expression, Source Qualifier | 20 | 4 | Low |
| `m_0500_PM_FDA_IO_Counter` | ERROR_TBL, HI_PM_FDA_TATRAN_TBL, PAY_PERIOD | COUNTER_TBL, FDA_EXTRACT_MESSAGE_FILE | Expression x8, Lookup Procedure x3, Aggregator x2, Normalizer x2, Source Qualifier x2, Filter, Joiner | 113 | 43 | High |
| `m_1100_PM_FDA_Send_Email` | HI_GENERIC_SRC_TBL | GENERIC_TARGET_FILE | Source Qualifier | 2 | 3 | Low |

Mapping parameters / variables in this export:

- `m_0010_PM_FDA_Verify_File`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_0020_PM_FDA_Set_CPM_Calendar`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_0025_PM_FDA_Set_Pay_Calendar`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA`: variables `$$MAP_CYCLE_ID`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_0150_PM_FDA_Error_Counter`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_CYCLE_ID`
- `m_0200_PM_FDA_Create_Insert_200_Rows`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_0500_PM_FDA_IO_Counter`: variables `$$MAP_MESSAGE`, `$$MAP_SUBJECT`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_CYCLE_ID`

### `XML/LES` (repository folder `LES`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_LESRPT_Load_LESC` | LES_EMP_DETAIL_RECTYPE_C_TBL | LESC | Expression x2, Lookup Procedure, Sequence, Source Qualifier | 60 | 10 | Medium |
| `m_LESRPT_Load_LESD` | LES_EMP_DETAIL_RECTYPE_D_TBL | LESD | Expression x2, Lookup Procedure, Sequence, Source Qualifier | 56 | 10 | Medium |
| `m_LESRPT_Load_LESL` | LES_EMP_DETAIL_LEAVE_TBL | LESL | Expression x2, Lookup Procedure, Source Qualifier | 64 | 8 | Low |
| `m_LESRPT_Load_LESM` | LES_EMP_DETAIL_RECTYPE_M_TBL | LESM | Expression x2, Lookup Procedure, Source Qualifier | 29 | 8 | Low |
| `m_LESRPT_Load_LESR` | LES_EMP_DETAIL_RECTYPE_R_TBL | LESR | Expression x2, Lookup Procedure, Sequence, Source Qualifier | 60 | 10 | Medium |
| `m_LESRPT_Load_LESS` | LES_PRIMARY_DATA_TBL | LESS | Expression x2, Lookup Procedure, Source Qualifier | 324 | 11 | Medium |
| `m_LESRPT_Load_LEST` | LES_EMP_DETAIL_RECTYPE_T_TBL | LEST | Expression x2, Lookup Procedure, Sequence, Source Qualifier | 37 | 10 | Medium |
| `m_LESRPT_Load_LESU` | LES_EMP_DETAIL_RECTYPE_U_TBL | LESU | Expression x2, Lookup Procedure, Sequence, Source Qualifier | 48 | 10 | Medium |
| `m_LES_Build_Message_Counters` | EMP_REC_TYPE_E, ERROR_TBL, LES_NIH_EMPLOYEE_SUMMARY_TBL, LES_PRIMARY_DATA_TBL | COUNTER_TBL, LES_MESSAGE_FILE | Expression x14, Aggregator x4, Joiner x3, Source Qualifier x3, Normalizer x2, Filter, Lookup Procedure | 201 | 58 | Very High |
| `m_LES_Current_Pay_Period` | PAY_PERIOD | LES_PAY_PERIOD_DATE_FILE | Expression x2, Source Qualifier | 15 | 5 | Low |
| `m_LES_Load_LES_EMP_DETAIL_CURR_EARN_TBL` | LES_EMP_DETAIL_RECTYPE_C_TBL | LES_EMP_DETAIL_CURR_EARN_TBL | Expression x3, Filter, Normalizer, Source Qualifier | 71 | 11 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_LEAVE_TBL` | LES_EMP_DETAIL_RECTYPE_L_TBL | LES_EMP_DETAIL_LEAVE_TBL | Expression x2, Source Qualifier | 60 | 5 | Low |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_1_TBL` | EMP_REC_TYPE_1, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_1_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 145 | 16 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_2_TBL` | EMP_REC_TYPE_2, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_2_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 118 | 16 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_3_TBL` | EMP_REC_TYPE_3, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_3_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 144 | 16 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_4_TBL` | EMP_REC_TYPE_4, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_4_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 119 | 16 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_5_TBL` | EMP_REC_TYPE_5, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_5_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 126 | 16 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_6_TBL` | EMP_REC_TYPE_6, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_6_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 53 | 15 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_C_TBL` | EMP_REC_TYPE_C, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_C_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 95 | 15 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_D_TBL` | EMP_REC_TYPE_D, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_D_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 85 | 15 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_L_TBL` | EMP_REC_TYPE_L, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_L_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 100 | 16 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_M_TBL` | EMP_REC_TYPE_M, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_M_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 35 | 15 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_R_TBL` | EMP_REC_TYPE_R, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_R_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 95 | 15 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_T_TBL` | EMP_REC_TYPE_T, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_T_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 49 | 15 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RECTYPE_U_TBL` | EMP_REC_TYPE_U, LES_EMP_DETAIL_TBL | LES_EMP_DETAIL_RECTYPE_U_TBL | Expression x2, Filter x2, Joiner, Normalizer, Source Qualifier | 73 | 15 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_RETRO_EARN_TBL` | LES_EMP_DETAIL_RECTYPE_R_TBL | LES_EMP_DETAIL_RETRO_EARN_TBL | Expression x3, Filter, Normalizer, Source Qualifier | 71 | 11 | Medium |
| `m_LES_Load_LES_EMP_DETAIL_TBL` | EMP_REC_TYPE_E, LES_HEADER_TBL | ERROR_TBL, LES_EMP_DETAIL_TBL | Expression x6, Filter, Joiner, Lookup Procedure, Normalizer, Router, Source Qualifier | 78 | 25 | High |
| `m_LES_Load_LES_HEADER_TBL` | EMP_REC_TYPE_0 | LES_HEADER_TBL | Expression x3, Filter x2, Lookup Procedure, Normalizer | 59 | 14 | Medium |
| `m_LES_Load_LES_PRIMARY_DATA_TBL` | LES_EMP_DETAIL_RECTYPE_1_TBL, LES_EMP_DETAIL_RECTYPE_2_TBL, LES_EMP_DETAIL_RECTYPE_3_TBL, LES_EMP_DETAIL_RECTYPE_4_TBL, LES_EMP_DETAIL_RECTYPE_5_TBL, LES_EMP_DETAIL_RECTYPE_6_TBL, LES_EMP_DETAIL_TBL, LES_HEADER_TBL | LES_PRIMARY_DATA_TBL | Expression x3, Lookup Procedure x2, Source Qualifier | 456 | 23 | Medium |
| `m_LES_NIH_FILE` | LES_EMPLOYEE_DETAIL | LES_NIH_EMPLOYEE_DETAIL, LES_NIH_EMPLOYEE_SUMMARY_TBL | Expression x4, Filter x3, Lookup Procedure x2, Source Qualifier | 50 | 17 | Medium |
| `m_LES_Verify_Header` | EMP_REC_TYPE_0 | LES_HEADER_FILE | Expression x4, Lookup Procedure x2, Filter, Normalizer | 48 | 17 | Medium |
| `m_LES_Verify_Record_Count` | EMP_REC_TYPE_E | LES_TOTALS_FILE | Expression x4, Filter x2, Normalizer | 34 | 12 | Medium |

Mapping parameters / variables in this export:

- `m_LESRPT_Load_LESC`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LESRPT_Load_LESD`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LESRPT_Load_LESL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LESRPT_Load_LESM`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LESRPT_Load_LESR`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LESRPT_Load_LESS`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LESRPT_Load_LEST`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LESRPT_Load_LESU`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Build_Message_Counters`: variables `$$MAP_MESSAGE`, `$$MAP_SUBJECT`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Current_Pay_Period`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`, `$$MAP_PP_YEAR_NUM`
- `m_LES_Load_LES_EMP_DETAIL_CURR_EARN_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_LEAVE_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_1_TBL`: variables `$$MAP_PP_END_DTE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_2_TBL`: variables `$$MAP_PP_END_DTE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_3_TBL`: variables `$$MAP_PP_END_DTE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_4_TBL`: variables `$$MAP_PP_END_DTE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_5_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_6_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_C_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_D_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_L_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_M_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_R_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_T_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RECTYPE_U_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_RETRO_EARN_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_EMP_DETAIL_TBL`: variables `$$MAP_PP_END_DTE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_HEADER_TBL`: variables `$$MAP_PP_END_DTE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Load_LES_PRIMARY_DATA_TBL`: variables `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Verify_Header`: variables `$$MAP_PP_END_DTE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`
- `m_LES_Verify_Record_Count`: variables `$$MAP_PP_END_DTE`, `$$MAP_PP_END_YEAR`, `$$MAP_PP_NUM`

<details><summary>Mapping descriptions from the repository</summary>

- **`m_LES_Build_Message_Counters`** — This mapping creates counters and builds the subject and message for an email message.
- **`m_LES_Current_Pay_Period`** — This mapping returns the Current Pay Period from the Pay Period table.

</details>

### `XML/Pay_Calendar` (repository folder `Pay_Calendar`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_Pay_Calendar_Build_Message` | PAY_PERIOD | PAY_PERIOD_MESSAGE_FILE | Expression x2, Source Qualifier | 18 | 5 | Low |
| `m_Pay_Calendar_Reset_Pay_Calendar` | PAY_PERIOD | PAY_PERIOD | Expression, Source Qualifier, Update Strategy | 17 | 8 | Low |
| `m_Pay_Calendar_Set_Pay_Calendar` | PAY_PERIOD | PAY_PERIOD | Expression x5, Lookup Procedure x2, Update Strategy x2, Router, Source Qualifier | 41 | 25 | High |
| `m_Pay_Calendar_Verify_Pay_Calendar` | PAY_PERIOD | PAY_PERIOD_VERIFY_FILE | Expression x2, Lookup Procedure, Source Qualifier | 14 | 8 | Low |

Mapping parameters / variables in this export:

- `m_Pay_Calendar_Build_Message`: variables `$$MAP_MESSAGE`, `$$MAP_SUBJECT`
- `m_Pay_Calendar_Set_Pay_Calendar`: parameters `$$PP_END_YEAR`, `$$PP_NUM`

<details><summary>Mapping descriptions from the repository</summary>

- **`m_Pay_Calendar_Build_Message`** — This mapping queries the Pay Calendar table for the record marked current and uses that record to build the subject and message for an email message.
- **`m_Pay_Calendar_Reset_Pay_Calendar`** — This mapping retrieves the record from the Pay Period table currently set to current and resets that record to the default.
- **`m_Pay_Calendar_Set_Pay_Calendar`** — This mapping uses the contents of a parameter file to determine whether a row will be set to current on the Pay Period table. If the parameters within the parameter file are set, the values within the parameter file will be used to determine the current pay period. If the parameters within the parameter file are empty, the system date will be used to determine the current pay period.
- **`m_Pay_Calendar_Verify_Pay_Calendar`** — This mapping ensures that at least one record in the Pay Period table is set to current. No more than one record in the Pay Period table should be current.

</details>

### `XML/Pseudossn` (repository folder `Pseudossn`)

| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |
| --- | --- | --- | --- | ---: | ---: | --- |
| `m_Pseudossn_Counters` | PSEUDOSSN_FILE | COUNTER_TBL, PSEUDOSSN_MESSAGE_FILE | Expression x6, Lookup Procedure x3, Aggregator, Filter, Normalizer, Source Qualifier | 110 | 28 | High |
| `m_Pseudossn_Current_Pay_Period` | PAY_PERIOD | PAY_PERIOD_DATE_FILE | Expression x2, Source Qualifier | 13 | 5 | Low |
| `m_Pseudossn_Load_Archive_Pseudossn_Tbl_v1` | PSEUDOSSN_TBL | HI_ARCH_PSEUDOSSN_TBL | Expression, Source Qualifier | 189 | 5 | Low |
| `m_Pseudossn_Load_Pseudossn_From_SDA_Tbl` | PSEUDOSSN_FILE_TK_NUM | PSEUDOSSN_FROM_SDA_TBL | Expression x4, Filter, Lookup Procedure, Sorter, Source Qualifier | 505 | 17 | Medium |
| `m_Pseudossn_Load_Pseudossn_Tbl` | PSEUDOSSN_FILE | ERROR_TBL, PSEUDOSSN_TBL | Expression x6, Filter x2, Lookup Procedure x2, Normalizer, Router, Sorter, Source Qualifier | 591 | 31 | High |
| `m_Pseudossn_Load_SDA_Records_Pseudossn_Tbl` | PSEUDOSSN_FROM_SDA_TBL | PSEUDOSSN_TBL | Expression x2, Filter, Lookup Procedure, Source Qualifier | 318 | 12 | Medium |
| `m_Pseudossn_Update_Timekeeper_Number` | PSEUDOSSN_TBL | PSEUDOSSN_TBL | Expression x2, Filter, Lookup Procedure, Source Qualifier, Update Strategy | 87 | 13 | Medium |
| `m_Pseudossn_Verify_Header_Date_Current_Pay_Period` | PSEUDOSSN_FILE | PSEUDO_HDR_DATE_FILE | Expression x3, Lookup Procedure x2, Filter, Source Qualifier | 85 | 13 | Medium |
| `m_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA` | PSEUDOSSN_FILE_TK_NUM | PSEUDO_HDR_DATE_FILE | Expression x3, Lookup Procedure x2, Filter, Source Qualifier | 86 | 13 | Medium |
| `m_Pseudossn_Verify_Record_Count` | PSEUDOSSN_FILE | PSEUDO_RECORD_COUNT | Expression x3, Filter x2, Source Qualifier x2, Aggregator, Joiner | 167 | 16 | Medium |

Mapping parameters / variables in this export:

- `m_Pseudossn_Counters`: variables `$$MAP_MESSAGE`, `$$MAP_SUBJECT`
- `m_Pseudossn_Current_Pay_Period`: variables `$$MAP_PP_YEAR_NUM`

<details><summary>Mapping descriptions from the repository</summary>

- **`m_Pseudossn_Counters`** — This mapping gets different counts on the PseudoSSN file that was processed and loads them to the Counters Table.
- **`m_Pseudossn_Current_Pay_Period`** — This mapping returns the Current Pay Period from the Pay Period table.
- **`m_Pseudossn_Load_Archive_Pseudossn_Tbl_v1`** — This mapping loads the contents of the table Pseudossn_TBL into a backup of the table.
- **`m_Pseudossn_Load_Pseudossn_From_SDA_Tbl`** — This mapping loads the PseudoSSN SDA file into the table Pseudossn_From_SDA_TBL.
- **`m_Pseudossn_Load_Pseudossn_Tbl`** — This mapping loads the PseudoSSN file into the table Pseudossn_TBL. MS 12/13/2012: The following changes were made. 1) The field Position_Sensitivity_Code is linked to the target. 2) The format of the field Effective_Date has been changed from YYYYDDMM to YYYYMMDD. 3) The Sorter transformation has been changed to sort by Pseudossn (Asc), Emp_Status (Asc) and Effecitve Date Desc
- **`m_Pseudossn_Load_SDA_Records_Pseudossn_Tbl`** — This mapping loads records from the table Pseudossn_from_SDA_Tbl to the table Pseudossn_Tbl.
- **`m_Pseudossn_Update_Timekeeper_Number`** — This mapping updates the Timekeeper Number on the table Pseudossn_Tbl.
- **`m_Pseudossn_Verify_Header_Date_Current_Pay_Period`** — This mapping will verify that the Header Date in the PseudoSSN file matches the end date for the Current Pay Period.
- **`m_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA`** — This mapping will verify that the Header Date in the PseudoSSN file from SDA matches the end date for the Current Pay Period.
- **`m_Pseudossn_Verify_Record_Count`** — This mapping compares the total in the Trailer Record to the count of all Detail Records in the file.

</details>

## Workflows and sessions

Every export ships exactly one workflow whose scheduler is `ON DEMAND`: nothing in the repository schedules itself. The workflows are started externally (`pmcmd` from cron, per the shell scripts in `Transfer Scripts/`), which is the piece that has no PowerCenter-side definition to migrate — see `pyspark/MIGRATION_NOTES.md`.

### `wf_COMPTIME` (`XML/COMPTIME`)

- Integration Service: `Test_IS` — schedule: `ONDEMAND`
- Sessions (3): `s_COMPTIME_Build_Message_Counters`, `s_COMPTIME_Current_Pay_Period`, `s_COMPTIME_Load_COMP_TIME_DAILY_TBL`
- Non-session tasks: `Start (Start)`, `email_COMPTIME_Complete (Email)`
- Task links: 4 (3 guarded by a `$task.Status = Succeeded` condition)

### `wf_CPM` (`XML/CPM`)

- Integration Service: `Test_IS` — schedule: `ONDEMAND`
- Sessions (15): `s_CPM_Build_Message_Counters`, `s_CPM_Current_Pay_Period`, `s_CPM_Load_CPM_MER_Staging_Tables`, `s_CPM_Load_CPM_NEWPAY_STG_ALT_TBL`, `s_CPM_Load_CPM_NEWPAY_STG_DETAIL_TBL`, `s_CPM_Load_CPM_NEWPAY_STG_TYPE_1_2_TBL`, `s_CPM_Load_CPM_NEWPAY_STG_TYPE_3_FDR_TBL`, `s_CPM_Load_CPM_NEWPAY_STG_YTD_STATE_TBL`, `s_CPM_Load_CPM_NEWPAY_TBL`, `s_CPM_Load_CPM_PAD_Staging_Tables`, `s_CPM_Load_CPM_PMR_Staging_Tables`, `s_CPM_Load_CPM_YTD_Staging_Tables`, `s_CPM_Load_FDR_CPM_NEWPAY_TBL`, `s_CPM_Load_From_FDR_CPM_NEWPAY_STG_TYPE_3_TBL`, `s_CPM_Send_Counts`
- Non-session tasks: `Start (Start)`
- Task links: 15 (14 guarded by a `$task.Status = Succeeded` condition)

### `wf_CPM_AFPS` (`XML/CPM_AFPS`)

- Integration Service: `Test_IS` — schedule: `ONDEMAND`
- Sessions (17): `s_CPM_AFPS_0010_Set_CPM_Calendar`, `s_CPM_AFPS_0025_Set_Pay_Calendar`, `s_CPM_AFPS_0050_Update_CPM_CYCLE_TBL`, `s_CPM_AFPS_0100_Data_Seperate`, `s_CPM_AFPS_0200_Debridge_To_FEEDER_FLAT`, `s_CPM_AFPS_0300_Gross_Exp_Report`, `s_CPM_AFPS_0400_Crossfoot_Errors`, `s_CPM_AFPS_0500_Crossfoot_Message_Header`, `s_CPM_AFPS_0600_Crossfoot_Message_Details`, `s_CPM_AFPS_0700_Crossfoot_Message_Summary_Counts`, `s_CPM_AFPS_0720_Crossfoot_Message_Gross_Expend`, `s_CPM_AFPS_0760_Concatenate_Crossfoot_Files`, `s_CPM_AFPS_0800_Build_Message_Counters`, `s_CPM_AFPS_0820_Build_Message_Totals`, `s_CPM_AFPS_0860_Concatenate_Counts_Files`, `s_CPM_AFPS_0900_Build_Message`, `s_CPM_AFPS_1000_Send_Report`
- Non-session tasks: `Start (Start)`
- Task links: 17 (16 guarded by a `$task.Status = Succeeded` condition)

### `wf_CPM_CDC` (`XML/CPM_CDC`)

- Integration Service: `Test_IS` — schedule: `ONDEMAND`
- Sessions (6): `s_CPM_CDC_Build_Message`, `s_CPM_CDC_Concatenate_Files`, `s_CPM_CDC_Load_CPM_CDC_Data_File`, `s_CPM_CDC_Load_CPM_CDC_Header_File`, `s_CPM_CDC_Set_CPM_Calendar`, `s_CPM_CDC_Set_Pay_Calendar`
- Non-session tasks: `Start (Start)`, `email_CPM_CDC (Email)`
- Task links: 7 (6 guarded by a `$task.Status = Succeeded` condition)

### `wf_CPM_NIH` (`XML/CPM_NIH`)

- Integration Service: `Test_IS` — schedule: `ONDEMAND`
- Sessions (6): `s_CPM_NIH_Build_Message`, `s_CPM_NIH_Concatenate_Files`, `s_CPM_NIH_Load_CPM_NIH_Data_File`, `s_CPM_NIH_Load_CPM_NIH_Header_File`, `s_CPM_NIH_Set_CPM_Calendar`, `s_CPM_NIH_Set_Pay_Calendar`
- Non-session tasks: `Start (Start)`, `email_CPM_NIH (Email)`
- Task links: 7 (6 guarded by a `$task.Status = Succeeded` condition)

### `wf_CPM_OIG` (`XML/CPM_OIG`)

- Integration Service: `Test_IS` — schedule: `ONDEMAND`
- Sessions (4): `s_CPM_OIG_Build_Message`, `s_CPM_OIG_Load_CPM_OIG_File`, `s_CPM_OIG_Set_CPM_Calendar`, `s_CPM_OIG_Set_Pay_Calendar`
- Non-session tasks: `Start (Start)`, `email_CPM_OIG (Email)`
- Task links: 5 (4 guarded by a `$task.Status = Succeeded` condition)

### `wf_EHRP2BIIS_UPDATE` (`XML/EHRP2BIIS_UPDATE`)

- Integration Service: `Prd_IS` — schedule: `unknown`
- Sessions (1): `s_m_EHRP2BIIS_UPDATE`
- Non-session tasks: `Start (Start)`
- Task links: 1 (0 guarded by a `$task.Status = Succeeded` condition)

### `wf_FDA_Leave` (`XML/FDA_Leave`)

- Integration Service: `Test_IS` — schedule: `ONDEMAND`
- Sessions (10): `s_0010_PM_FDA_Verify_File`, `s_0020_PM_FDA_Set_CPM_Calendar`, `s_0025_PM_FDA_Set_Pay_Calendar`, `s_0050_PM_FDA_Update_CPM_CYCLE_TBL_FDA`, `s_0100_PM_FDA_Load_TATRAN_To_DB`, `s_0150_PM_FDA_Error_Counter`, `s_0200_PM_FDA_Create_200_Rows`, `s_0300_PM_FDA_Create_Output_File`, `s_0500_PM_FDA_IO_Counter`, `s_1100_PM_FDA_Send_Email`
- Non-session tasks: `Start (Start)`
- Task links: 10 (9 guarded by a `$task.Status = Succeeded` condition)

### `wf_LES` (`XML/LES`)

- Integration Service: `Prd_IS` — schedule: `ONDEMAND`
- Sessions (32): `s_LESRPT_Load_LESC`, `s_LESRPT_Load_LESD`, `s_LESRPT_Load_LESL`, `s_LESRPT_Load_LESM`, `s_LESRPT_Load_LESR`, `s_LESRPT_Load_LESS`, `s_LESRPT_Load_LEST`, `s_LESRPT_Load_LESU`, `s_LES_Build_Message_Counters`, `s_LES_Current_Pay_Period`, `s_LES_Load_LES_EMP_DETAIL_CURR_EARN_TBL`, `s_LES_Load_LES_EMP_DETAIL_LEAVE_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_1_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_2_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_3_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_4_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_5_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_6_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_C_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_D_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_L_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_M_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_R_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_T_TBL`, `s_LES_Load_LES_EMP_DETAIL_RECTYPE_U_TBL`, `s_LES_Load_LES_EMP_DETAIL_RETRO_EARN_TBL`, `s_LES_Load_LES_EMP_DETAIL_TBL`, `s_LES_Load_LES_HEADER_TBL`, `s_LES_Load_LES_PRIMARY_DATA_TBL`, `s_LES_NIH_FILE`, `s_LES_Verify_Header`, `s_LES_Verify_Record_Count`
- Non-session tasks: `Start (Start)`, `email_LES_Complete (Email)`
- Task links: 33 (32 guarded by a `$task.Status = Succeeded` condition)

### `wf_Pay_Calendar` (`XML/Pay_Calendar`)

- Integration Service: `Prd_IS` — schedule: `ONDEMAND`
- Sessions (4): `s_Pay_Calendar_Build_Message`, `s_Pay_Calendar_Reset_Pay_Calendar`, `s_Pay_Calendar_Set_Pay_Calendar`, `s_Pay_Calendar_Verify_Pay_Calendar`
- Non-session tasks: `Email_Pay_Calendar (Email)`, `Start (Start)`
- Task links: 5 (4 guarded by a `$task.Status = Succeeded` condition)

### `wf_Pseudossn` (`XML/Pseudossn`)

- Integration Service: `Prd_IS` — schedule: `ONDEMAND`
- Sessions (10): `s_Pseudossn_Counters`, `s_Pseudossn_Current_Pay_Period`, `s_Pseudossn_Load_Archive_Pseudossn_Tbl`, `s_Pseudossn_Load_Pseudossn_From_SDA_Tbl`, `s_Pseudossn_Load_Pseudossn_Tbl`, `s_Pseudossn_Load_SDA_Records_Pseudossn_Tbl`, `s_Pseudossn_Update_Timekeeper_Number`, `s_Pseudossn_Verify_Header_Date_Current_Pay_Period`, `s_Pseudossn_Verify_Header_Date_Current_Pay_Period_Pseudossn_From_SDA`, `s_Pseudossn_Verify_Record_Count`
- Non-session tasks: `Email_Pseudossn (Email)`, `Start (Start)`
- Task links: 11 (10 guarded by a `$task.Status = Succeeded` condition)

## Non-PowerCenter assets in this repository

- `Transfer Scripts/` — Korn shell SFTP senders (`cdc_transfer`, `nih_transfer_les`, `oig_transfer`, `fda_transfer`, `afps_transfer`, `nih_cpm_transfer`, `nih_les_transfer`). Each checks for an output file, SFTPs it to a partner host and emails a hard-coded distribution list.
- `Maintenance Scripts/` — `archive_files` (renames processed files with a pay-period suffix) and `remove_file`.
- `actstage_load`, `ehrp2biis_preload`, `Pseudossn` — shell drivers; `ehrp2biis_afterload.sql` — post-load SQL.
- `main.tf` — Terraform.

