"""Fixed-width PSEUDOSSN file handling ported from XML/Pseudossn (wf_Pseudossn).

m_EHRP2BIIS_UPDATE itself does not mask SSNs: NWK_ACTION_PRIMARY_TBL.SSN is a straight
pass-through of PS_GVT_PERS_NID.NATIONAL_ID. Masking lives in the Pseudossn workflow,
which loads the SSN <-> PSEUDOSSN crosswalk from the agency fixed-width file. This
module ports the parts the downstream contract depends on byte-for-byte:

* the record layout (offsets/precisions read from the XML, Latin-1, no delimiter)
* the record-type rules of exp_Determine_Record_Type:
      v_RECORD_TYPE_FLAG = DECODE(TRUE, SUBSTR(SSN,1,6)='HEADER','H', SUBSTR(SSN,1,7)='TRAILER','T','D')
      o_RECORD_TYPE_FLAG = IIF(v='D' AND LENGTH(LTRIM(RTRIM(SSN))) > 0, v, 'R')
* lossless parse -> write (fields are kept as raw, untrimmed strings)
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from functools import cache
from pathlib import Path

XML_PATH = Path(__file__).resolve().parents[1] / "XML" / "Pseudossn"
ENCODING = "latin-1"


@dataclass(frozen=True)
class Field:
    name: str
    offset: int
    length: int


@cache
def layout(source: str = "PSEUDOSSN_FILE") -> tuple[Field, ...]:
    root = ET.parse(XML_PATH).getroot()
    src = next(s for s in root.iter("SOURCE") if s.get("NAME") == source)
    return tuple(Field(f.get("NAME"), int(f.get("PHYSICALOFFSET")), int(f.get("PHYSICALLENGTH") or f.get("PRECISION")))
                 for f in src.iter("SOURCEFIELD"))


def record_length(source: str = "PSEUDOSSN_FILE") -> int:
    return max(f.offset + f.length for f in layout(source))


def parse_record(line: bytes, source: str = "PSEUDOSSN_FILE") -> dict[str, str]:
    text = line.rstrip(b"\r\n").decode(ENCODING).ljust(record_length(source))
    return {f.name: text[f.offset:f.offset + f.length] for f in layout(source)}


def write_record(rec: dict[str, str], source: str = "PSEUDOSSN_FILE") -> bytes:
    buf = [" "] * record_length(source)
    for f in layout(source):
        val = (rec.get(f.name) or "")[: f.length].ljust(f.length)
        buf[f.offset:f.offset + f.length] = list(val)
    return "".join(buf).encode(ENCODING)


def raw_record_type(ssn: str | None) -> str:
    """v_RECORD_TYPE_FLAG. NULL SSN: both SUBSTR tests are NULL (not TRUE) -> 'D'."""
    if ssn is not None and ssn[:6] == "HEADER":
        return "H"
    if ssn is not None and ssn[:7] == "TRAILER":
        return "T"
    return "D"


def record_type_flag(ssn: str | None) -> str:
    """o_RECORD_TYPE_FLAG: detail rows with a blank/NULL SSN become 'R' (rejected)."""
    v = raw_record_type(ssn)
    has_ssn = ssn is not None and len(ssn.strip(" ")) > 0
    return v if v == "D" and has_ssn else "R"
