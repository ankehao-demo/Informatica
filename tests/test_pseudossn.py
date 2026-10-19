from pipelines.pseudossn import layout, parse_record, raw_record_type, record_length, record_type_flag, write_record


def _line(ssn, can="20260306", pseudo="123456789"):
    rec = {f.name: "" for f in layout()}
    rec.update({"SSN": ssn, "CAN_CD": can, "PSEUDO_SSN": pseudo, "FIRST_NAME": "ANN", "LAST_NAME": "O'NEIL\xc9"})
    return write_record(rec)


def test_layout_is_fixed_width_from_xml():
    fields = layout()
    assert fields[0].name == "SSN" and fields[0].offset == 0 and fields[0].length == 9
    assert record_length() == 500


def test_round_trip_is_byte_identical():
    for ssn in ("123456789", "HEADER   ", "TRAILER  ", "         "):
        raw = _line(ssn)
        assert len(raw) == 500
        assert write_record(parse_record(raw)) == raw
        assert write_record(parse_record(raw + b"\r\n")) == raw


def test_record_type_rules():
    assert [raw_record_type(s) for s in ("HEADER", "TRAILER", "123456789", None)] == ["H", "T", "D", "D"]
    assert record_type_flag("123456789") == "D"
    assert record_type_flag("         ") == "R"  # blank SSN detail -> rejected
    assert record_type_flag(None) == "R"
    assert record_type_flag("HEADER") == "R"  # IIF only keeps 'D' rows in this mapping
