# """
# parser.py
# Core parsing layer for the Dynamic ETL Pipeline evaluator.
#
# Output of parse_file(text):
# {
#     "fragments": List[DetectedBlock],
#     "records": List[Dict],        # normalized data units
#     "fields": List[SchemaField],  # inferred schema fields
#     "summary": {...}              # counts by type
# }
# """
#
# from __future__ import annotations
# import re
# import json
# import csv
# from io import StringIO
# from dataclasses import dataclass, field
# from typing import Any, List, Dict, Optional
# from bs4 import BeautifulSoup
#
#
# # ============================================================
# # LAYER 0 — DATA CLASSES
# # ============================================================
#
# @dataclass
# class DetectedBlock:
#     format_type: str
#     start: int
#     end: int
#     text: str
#     confidence: float
#     meta: Dict[str, Any] = field(default_factory=dict)
#
#
# @dataclass
# class NormalizedRecord:
#     source_fragment: str
#     record: Dict[str, Any]
#     offsets: Dict[str, Any]
#
#
# @dataclass
# class SchemaField:
#     name: str
#     path: str
#     type: str
#     nullable: bool
#     example: Any
#     confidence: float
#     source_offsets: List[Dict[str, Any]] = field(default_factory=list)
#
#
# # ============================================================
# # LAYER 1 — FRAGMENT DETECTOR (Robust, nested, no crash)
# # ============================================================
#
# class FragmentDetector:
#     def __init__(self, text: str):
#         self.text = text
#
#     # -------- JSON DETECTION WITH BRACE COUNTING --------
#     def detect_json_blocks(self) -> List[DetectedBlock]:
#         blocks = []
#         stack = []
#         start_idx = None
#
#         for i, ch in enumerate(self.text):
#             if ch == '{':
#                 if not stack:
#                     start_idx = i
#                 stack.append('{')
#             elif ch == '}' and stack:
#                 stack.pop()
#                 if not stack and start_idx is not None:
#                     snippet = self.text[start_idx:i+1]
#                     blocks.append(
#                         DetectedBlock(
#                             format_type="JSON",
#                             start=start_idx,
#                             end=i+1,
#                             text=snippet,
#                             confidence=0.95
#                         )
#                     )
#                     start_idx = None
#
#         return blocks
#
#     # ------------- MALFORMED JSON (no closing brace) -------------
#     def detect_malformed_json(self) -> List[DetectedBlock]:
#         blocks = []
#         # Look for '{' that never gets a closing brace
#         for m in re.finditer(r'\{\s*"[A-Za-z0-9_]', self.text):
#             start = m.start()
#             # Up to next section divider or two blank lines
#             tail = self.text[start:start+1500]
#             if '}' not in tail:
#                 end = start + len(tail.split("\n\n")[0])
#                 snippet = self.text[start:end]
#                 blocks.append(
#                     DetectedBlock(
#                         "MALFORMED_JSON",
#                         start,
#                         end,
#                         snippet,
#                         0.55
#                     )
#                 )
#         return blocks
#
#     # ------------- HTML / TABLE DETECTION --------------
#     def detect_html(self) -> List[DetectedBlock]:
#         blocks = []
#         for m in re.finditer(r'<(html|div|table|section|article|body)\b', self.text, re.IGNORECASE):
#             start = m.start()
#             end = self.text.find("</", start)
#             if end == -1:
#                 continue
#             end = self.text.find(">", end)
#             snippet = self.text[start:end+1]
#             blocks.append(
#                 DetectedBlock("HTML", start, end+1, snippet, 0.8)
#             )
#         return blocks
#
#     def detect_html_tables(self) -> List[DetectedBlock]:
#         blocks = []
#         for m in re.finditer(r'<table[^>]*>[\s\S]*?</table>', self.text, re.IGNORECASE):
#             start, end = m.start(), m.end()
#             snippet = self.text[start:end]
#             blocks.append(
#                 DetectedBlock("HTML_TABLE", start, end, snippet, 0.95)
#             )
#         return blocks
#
#     # ------------- CSV DETECTION --------------
#     def detect_csv(self) -> List[DetectedBlock]:
#         blocks = []
#         lines = self.text.splitlines()
#         n = len(lines)
#
#         i = 0
#         while i < n:
#             if not lines[i].strip():
#                 i += 1
#                 continue
#
#             comma = lines[i].count(',')
#             if comma < 1:
#                 i += 1
#                 continue
#
#             chunk = [lines[i]]
#             j = i + 1
#             while j < n and lines[j].count(',') == comma:
#                 chunk.append(lines[j])
#                 j += 1
#
#             if len(chunk) >= 2:
#                 start = self._line_pos(i)
#                 end = self._line_pos(j-1) + len(lines[j-1])
#                 snippet = self.text[start:end]
#                 has_header = self._looks_like_header(chunk[0])
#                 blocks.append(
#                     DetectedBlock(
#                         "CSV" if has_header else "CSV_NO_HEADER",
#                         start,
#                         end,
#                         snippet,
#                         0.85 if has_header else 0.7
#                     )
#                 )
#                 i = j
#                 continue
#             i += 1
#         return blocks
#
#     def _looks_like_header(self, line: str) -> bool:
#         return bool(re.search(r'[A-Za-z]', line)) and ',' in line
#
#     def _line_pos(self, index: int) -> int:
#         return sum(len(l) + 1 for l in self.text.splitlines()[:index])
#
#     # ------------- YAML FRONTMATTER --------------
#     def detect_yaml(self):
#         blocks = []
#         pattern = r'^---\s*\n([\s\S]{5,2000}?)\n---'
#         for m in re.finditer(pattern, self.text, re.MULTILINE):
#             start = m.start(1)
#             end = m.end(1)
#             snippet = self.text[start:end]
#             blocks.append(
#                 DetectedBlock("YAML", start, end, snippet, 0.9)
#             )
#         return blocks
#
#     # ------------- KEY VALUE ---------------------
#     def detect_kv(self):
#         blocks = []
#         lines = self.text.splitlines()
#         n = len(lines)
#         i = 0
#         while i < n:
#             if re.match(r'^\w[\w -]*: ', lines[i]):
#                 j = i
#                 block = []
#                 while j < n and re.match(r'^\w[\w -]*: ', lines[j]):
#                     block.append(lines[j])
#                     j += 1
#                 if len(block) >= 2:
#                     start = self._line_pos(i)
#                     end = self._line_pos(j-1) + len(lines[j-1])
#                     snippet = self.text[start:end]
#                     blocks.append(
#                         DetectedBlock("KEY_VALUE", start, end, snippet, 0.88)
#                     )
#                 i = j
#                 continue
#             i += 1
#         return blocks
#
#     # ------------- SQL ---------------------
#     def detect_sql(self):
#         blocks = []
#         for m in re.finditer(r'(SELECT|INSERT|DELETE|UPDATE)\s+[\s\S]{5,300}?\;', self.text, re.IGNORECASE):
#             blocks.append(
#                 DetectedBlock("SQL", m.start(), m.end(), self.text[m.start():m.end()], 0.9)
#             )
#         return blocks
#
#     # ------------- JS OBJECT ---------------------
#     def detect_js(self):
#         blocks = []
#         pattern = r'(var|let|const)\s+\w+\s*=\s*\{[\s\S]{2,500}?\};'
#         for m in re.finditer(pattern, self.text):
#             blocks.append(
#                 DetectedBlock("JS_OBJECT", m.start(), m.end(), m.group(0), 0.9)
#             )
#         return blocks
#
#     # ------------- RAW TEXT PARAGRAPHS --------------
#     def detect_raw_text(self):
#         blocks = []
#         for m in re.finditer(r'[A-Za-z][\s\S]{20,1200}?(\n\n|$)', self.text):
#             start, end = m.start(), m.end()
#             blocks.append(
#                 DetectedBlock("RAW_TEXT", start, end, self.text[start:end], 0.4)
#             )
#         return blocks
#
#     # -------- MASTER DETECTOR --------
#     def detect_all(self) -> List[DetectedBlock]:
#         blocks = []
#         blocks += self.detect_yaml()
#         blocks += self.detect_html_tables()
#         blocks += self.detect_html()
#         blocks += self.detect_json_blocks()
#         blocks += self.detect_malformed_json()
#         blocks += self.detect_csv()
#         blocks += self.detect_kv()
#         blocks += self.detect_js()
#         blocks += self.detect_sql()
#         blocks += self.detect_raw_text()
#
#         # sort and dedupe
#         blocks = sorted(blocks, key=lambda b: b.start)
#         return blocks
#
#
# # ============================================================
# # LAYER 2 — NORMALIZER: Convert each fragment → structured dict
# # ============================================================
#
# class Normalizer:
#     def normalize(self, block: DetectedBlock) -> Optional[Dict[str, Any]]:
#         t = block.format_type
#         txt = block.text
#
#         try:
#             if t == "JSON":
#                 return json.loads(txt)
#             if t == "MALFORMED_JSON":
#                 return self._repair_json(txt)
#             if t == "JSON_LD":
#                 return json.loads(txt)
#             if t == "HTML_TABLE":
#                 return self._html_table_to_rows(txt)
#             if t in ("CSV", "CSV_NO_HEADER"):
#                 return self._parse_csv(txt)
#             if t == "KEY_VALUE":
#                 return self._parse_kv(txt)
#         except:
#             return None
#
#         return None
#
#     def _repair_json(self, txt):
#         txt = re.sub(r',\s*}', '}', txt)
#         txt = re.sub(r',\s*\]', ']', txt)
#         txt = txt.replace("'", '"')
#         m = re.search(r'\{[\s\S]*\}', txt)
#         if m:
#             try:
#                 return json.loads(m.group(0))
#             except:
#                 pass
#         return None
#
#     def _html_table_to_rows(self, txt):
#         soup = BeautifulSoup(txt, "html.parser")
#         rows = []
#         table = soup.find("table")
#         if not table:
#             return None
#
#         headers = [th.get_text(strip=True) for th in table.find_all("th")]
#         for tr in table.find_all("tr"):
#             cells = [td.get_text(strip=True) for td in tr.find_all("td")]
#             if len(cells) == len(headers):
#                 rows.append(dict(zip(headers, cells)))
#         return rows or None
#
#     def _parse_csv(self, txt):
#         sio = StringIO(txt.strip())
#         reader = csv.reader(sio)
#         rows = list(reader)
#         if not rows:
#             return None
#         if self._all_numeric_headers(rows[0]):
#             # synthetic headers
#             h = [f"col_{i}" for i in range(len(rows[0]))]
#             return [dict(zip(h, r)) for r in rows]
#         return [dict(zip(rows[0], r)) for r in rows[1:]]
#
#     def _all_numeric_headers(self, row):
#         return all(not re.search('[A-Za-z]', cell) for cell in row)
#
#     def _parse_kv(self, txt):
#         out = {}
#         for line in txt.splitlines():
#             if ":" in line:
#                 k, v = line.split(":", 1)
#                 out[k.strip()] = v.strip().strip('"')
#         return out
#
#
# # ============================================================
# # LAYER 3 — FLATTENER (extract all field paths)
# # ============================================================
#
# class Flattener:
#     def flatten(self, obj: Any, prefix="") -> List[SchemaField]:
#         fields = []
#         if isinstance(obj, dict):
#             for k, v in obj.items():
#                 p = f"{prefix}.{k}" if prefix else k
#                 fields.extend(self.flatten(v, p))
#         elif isinstance(obj, list):
#             for i, v in enumerate(obj):
#                 p = f"{prefix}[{i}]"
#                 fields.extend(self.flatten(v, p))
#         else:
#             fields.append(
#                 SchemaField(
#                     name=prefix.split(".")[-1],
#                     path=prefix,
#                     type=self._infer_type(obj),
#                     nullable=obj is None,
#                     example=obj,
#                     confidence=0.95,
#                     source_offsets=[]
#                 )
#             )
#         return fields
#
#     def _infer_type(self, v):
#         if isinstance(v, bool):
#             return "boolean"
#         if isinstance(v, int):
#             return "integer"
#         if isinstance(v, float):
#             return "number"
#         if isinstance(v, str):
#             if re.match(r'\d{4}-\d{2}-\d{2}', v):
#                 return "date"
#             return "string"
#         if v is None:
#             return "null"
#         return "string"
#
#
# # ============================================================
# # LAYER 4 — MAIN PARSER PIPELINE
# # ============================================================
#
# def parse_file(text: str) -> Dict[str, Any]:
#     detector = FragmentDetector(text)
#     blocks = detector.detect_all()
#
#     normalizer = Normalizer()
#     flattener = Flattener()
#
#     structured_records = []
#     fields = []
#
#     for b in blocks:
#         data = normalizer.normalize(b)
#         if data is not None:
#             if isinstance(data, list):
#                 for rec in data:
#                     structured_records.append(rec)
#                     fields.extend(flattener.flatten(rec))
#             else:
#                 structured_records.append(data)
#                 fields.extend(flattener.flatten(data))
#
#     summary = {}
#     for b in blocks:
#         summary[b.format_type] = summary.get(b.format_type, 0) + 1
#
#     return {
#         "fragments": blocks,
#         "records": structured_records,
#         "fields": fields,
#         "summary": summary
#     }

#!/usr/bin/env python3
"""
etl_parser.py
Section-aware, robust fragment parser for Dynamic ETL Pipeline evaluator.

Usage:
    from etl_parser import parse_file
    result = parse_file(text)  # returns dict with 'fragments', 'summary', 'records' (normalized)
"""

from __future__ import annotations
import re
import json
import csv
from io import StringIO
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from bs4 import BeautifulSoup

# ---------- Data classes ----------
@dataclass
class DetectedBlock:
    format_type: str
    start_index: int
    end_index: int
    confidence: float
    text: str
    meta: Dict[str, Any] = field(default_factory=dict)

# ---------- Priority list (lower index = higher priority) ----------
FORMAT_PRIORITY = [
    "JSON_LD",
    "JSON",
    "MALFORMED_JSON",
    "HTML_TABLE",
    "HTML",
    "YAML_FRONTMATTER",
    "CSV",
    "CSV_NO_HEADER",
    "KEY_VALUE",
    "JS_OBJECT",
    "SQL",
    "RAW_TEXT",
]

# ---------- Utilities ----------
def clamp_conf(c: float) -> float:
    return max(0.0, min(1.0, float(c)))

def contains_any(chars: str, needles: List[str]) -> bool:
    return any(n in chars for n in needles)

# ---------- Safe JSON span finder (brace-counting with string awareness) ----------
def find_json_span(text: str, start_pos: int, max_len: int = 200000) -> Optional[Tuple[int,int]]:
    """
    If a '{' at start_pos (or later) begins a JSON object, return (start, end)
    where end is index of the matching '}' +1. If cannot find matching brace within limits, return None.
    This function handles string quoting and escapes to avoid being fooled by braces inside strings.
    """
    n = len(text)
    i = start_pos
    # move forward to first '{'
    while i < n and text[i] != '{':
        i += 1
    if i >= n:
        return None
    start = i
    depth = 0
    in_string = False
    escape = False
    string_char = ''
    j = i
    limit = min(n, i + max_len)
    while j < limit:
        ch = text[j]
        if in_string:
            if escape:
                escape = False
            elif ch == '\\':
                escape = True
            elif ch == string_char:
                in_string = False
        else:
            if ch == '"' or ch == "'":
                in_string = True
                string_char = ch
            elif ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return (start, j+1)
        j += 1
    # didn't find closing brace within limit
    return None

# ---------- Section-splitting helpers ----------
SECTION_DIV_RE = re.compile(r'^(---\s*[\w \-()/:]*\n)', re.MULTILINE)  # captures '--- HEADER\n'
VARIANT_START_RE = re.compile(r'^(===\s*VARIANT\b[\s\S]*?===\s*VARIANT\b.*?===)', re.MULTILINE)

# ---------- Detector core ----------
class ETLFragmentDetector:
    def __init__(self, text: str):
        self.text = text
        self.n = len(text)
        self.blocks: List[DetectedBlock] = []
        self.occupied = []  # list of (start,end) reserved by high-priority blocks

    def mark_occupied(self, start: int, end: int):
        self.occupied.append((start, end))

    def is_occupied(self, start: int, end: int) -> bool:
        for a,b in self.occupied:
            # if overlap
            if not (end <= a or start >= b):
                return True
        return False

    def add_block(self, block: DetectedBlock):
        self.blocks.append(block)
        # Reserve area for high priority types to avoid low-priority collisions
        if block.format_type in ("JSON_LD","JSON","MALFORMED_JSON","HTML_TABLE","HTML","YAML_FRONTMATTER"):
            self.mark_occupied(block.start_index, block.end_index)

    # ---------- 1. JSON-LD (script type=application/ld+json) ----------
    def detect_json_ld(self):
        try:
            for m in re.finditer(r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>', self.text, flags=re.IGNORECASE):
                start = m.start(1)
                end = m.end(1)
                snippet = m.group(1).strip()
                conf = 0.9
                # try strict parse
                try:
                    parsed = json.loads(snippet)
                    conf = 0.99
                except Exception:
                    conf = 0.6
                self.add_block(DetectedBlock("JSON_LD", start, end, clamp_conf(conf), self.text[start:end], {"parsed": conf>0.9}))
        except Exception as e:
            # fail safe: don't crash detector
            pass

    # ---------- 2. YAML frontmatter (--- ... ---) ----------
    def detect_yaml_frontmatter(self):
        try:
            for m in re.finditer(r'(^|\n)---\s*\n([\s\S]{0,2000}?)\n---', self.text, flags=re.MULTILINE):
                start = m.start(2)
                end = m.end(2)
                snippet = m.group(2)
                # heuristic: many lines with ':'
                lines = [ln for ln in snippet.splitlines() if ln.strip()]
                colon_ratio = sum(1 for ln in lines if ':' in ln) / max(1, len(lines))
                conf = 0.95 if colon_ratio > 0.5 else 0.6
                if not self.is_occupied(start, end):
                    self.add_block(DetectedBlock("YAML_FRONTMATTER", start, end, clamp_conf(conf), self.text[start:end], {"colon_ratio": colon_ratio}))
        except Exception:
            pass

    # ---------- 3. Section-aware explicit JSON markers (--- INLINE JSON / MALFORMED JSON) ----------
    def detect_sectioned_jsons(self):
        # If file uses '--- INLINE JSON' style, use that boundary
        # Find lines that start with '---' and check header text
        try:
            for m in re.finditer(r'(^|\n)---\s*([A-Z0-9 _\-()]+)\s*\n', self.text, flags=re.IGNORECASE):
                header = m.group(2).strip().upper()
                # section body starts after match
                body_start = m.end()
                # find next section divider or EOF
                next_div = re.search(r'\n---\s*[\w \-()/:]*\n', self.text[body_start:], flags=re.IGNORECASE)
                if next_div:
                    body_end = body_start + next_div.start()
                else:
                    body_end = self.n
                body = self.text[body_start:body_end].strip()
                if not body:
                    continue
                if "JSON" in header and not self.is_occupied(body_start, body_end):
                    # try to find JSON inside body (brace counting)
                    js_span = find_json_span(self.text, body_start)
                    if js_span:
                        s,e = js_span
                        try:
                            json.loads(self.text[s:e])
                            conf=0.99
                            ftype="JSON"
                        except Exception:
                            conf=0.45
                            ftype="MALFORMED_JSON"
                        self.add_block(DetectedBlock(ftype, s, e, clamp_conf(conf), self.text[s:e], {"section_header": header}))
                    else:
                        # No matching closing brace -> treat as MALFORMED_JSON covering body
                        if not self.is_occupied(body_start, body_end):
                            self.add_block(DetectedBlock("MALFORMED_JSON", body_start, body_end, 0.4, self.text[body_start:body_end], {"section_header": header}))
        except Exception:
            pass

    # ---------- 4. JSON (strict) & MALFORMED JSON (brace heuristics) ----------
    def detect_jsons_global(self):
        i = 0
        text = self.text
        n = self.n
        while True:
            # find next '{' from i
            m = re.search(r'\{', text[i:])
            if not m:
                break
            pos = i + m.start()
            # if occupied by higher-priority block, skip ahead
            if self.is_occupied(pos, pos+1):
                i = pos + 1
                continue
            span = find_json_span(text, pos, max_len=200000)
            if span:
                s,e = span
                # If this region overlaps existing occupied high-priority, skip
                if self.is_occupied(s,e):
                    i = e
                    continue
                snippet = text[s:e]
                # strict parse
                try:
                    _ = json.loads(snippet)
                    conf = 0.98
                    self.add_block(DetectedBlock("JSON", s, e, conf, snippet, {}))
                except Exception:
                    # try smaller window (maybe JSON-LD or partial)
                    # If snippet has many key:value patterns, mark MALFORMED_JSON with moderate confidence
                    kv_like = len(re.findall(r'"\w+"\s*:', snippet)) + len(re.findall(r'\w+\s*:', snippet))
                    conf = 0.5 if kv_like >= 2 else 0.25
                    self.add_block(DetectedBlock("MALFORMED_JSON", s, e, clamp_conf(conf), snippet, {"kv_like": kv_like}))
                i = e
            else:
                # No closing brace within limit -> treat up to next blank line or 1000 chars as malformed
                tail_end = min(n, pos + 2000)
                # try to extend until double newline
                remainder = text[pos:tail_end]
                dn = re.search(r'\n\s*\n', remainder)
                if dn:
                    end = pos + dn.start()
                else:
                    end = tail_end
                if not self.is_occupied(pos,end):
                    snippet = text[pos:end]
                    # heuristics: if snippet contains colon or quotes, mark MALFORMED_JSON
                    if re.search(r'["\']\w+["\']\s*:', snippet) or re.search(r'\w+\s*:\s*', snippet):
                        self.add_block(DetectedBlock("MALFORMED_JSON", pos, end, 0.35, snippet, {"note":"unclosed"}))
                i = end

    # ---------- 5. HTML TABLES and HTML (use BeautifulSoup for reliability) ----------
    def detect_html_tables_and_blocks(self):
        # tables first
        try:
            for m in re.finditer(r'<table\b', self.text, flags=re.IGNORECASE):
                # find end via regex '</table>' from here
                start = m.start()
                if self.is_occupied(start, start+1):
                    continue
                end_tag = re.search(r'</table\s*>', self.text[start:], flags=re.IGNORECASE)
                if end_tag:
                    end = start + end_tag.end()
                    snippet = self.text[start:end]
                    # parse quickly with BeautifulSoup to count rows/cols
                    try:
                        soup = BeautifulSoup(snippet, "html.parser")
                        rows = soup.find_all("tr")
                        cols = max((len(r.find_all(['td','th'])) for r in rows), default=0)
                        conf = 0.95 if rows and cols>=1 else 0.6
                    except Exception:
                        conf = 0.6
                    if not self.is_occupied(start,end):
                        self.add_block(DetectedBlock("HTML_TABLE", start, end, clamp_conf(conf), snippet, {"rows": len(rows) if 'rows' in locals() else None, "cols": cols if 'cols' in locals() else None}))
        except Exception:
            pass

        # generic HTML blocks (div/section/script etc). Keep only reasonable-size blocks.
        try:
            for m in re.finditer(r'<(div|section|article|header|footer|main|nav|body)\b', self.text, flags=re.IGNORECASE):
                start = m.start()
                if self.is_occupied(start, start+1):
                    continue
                # try to find matching closing tag by name
                tag = m.group(1)
                close_re = re.compile(r'</%s\s*>'%re.escape(tag), flags=re.IGNORECASE)
                close_match = close_re.search(self.text[start:])
                if close_match:
                    end = start + close_match.end()
                    if end - start > 20 and not self.is_occupied(start,end):
                        snippet = self.text[start:end]
                        # compute crude confidence by tag density
                        tag_count = len(re.findall(r'<[A-Za-z]+', snippet))
                        close_count = len(re.findall(r'</', snippet))
                        conf = 0.5 + min(0.4, (min(tag_count, close_count) * 0.03))
                        self.add_block(DetectedBlock("HTML", start, end, clamp_conf(conf), snippet, {"tag_count":tag_count}))
        except Exception:
            pass

    # ---------- 6. CSV detection (robust, avoid JSON) ----------
    def detect_csv_blocks(self):
        lines = self.text.splitlines()
        n = len(lines)
        # compute cumulative char position for quick mapping
        char_pos = [0]
        for ln in lines:
            char_pos.append(char_pos[-1] + len(ln) + 1)  # +1 for newline
        i = 0
        while i < n:
            # skip short or obviously non-csv
            if lines[i].strip()=="":
                i += 1
                continue
            # detect candidate delimiter among , or \t or ;
            cand = None
            for d in (',','\t',';'):
                # require at least 1 delimiter in first line and not JSON-like
                if d in lines[i] and '{' not in lines[i] and '}' not in lines[i]:
                    cand = d
                    break
            if not cand:
                i += 1
                continue
            # collect block where delimiter count stays consistent (allow some variance)
            counts = [lines[i].count(cand)]
            j = i+1
            max_lines = 200
            while j < n and j - i < max_lines and lines[j].strip() and lines[j].count(cand) > 0:
                counts.append(lines[j].count(cand))
                j += 1
            if len(counts) >= 2:
                # consistency check: most rows should have same count
                from collections import Counter
                ctr = Counter(counts)
                common_count, freq = ctr.most_common(1)[0]
                if freq >= max(1, len(counts)//2):
                    start = char_pos[i]
                    end = char_pos[j-1] + len(lines[j-1]) if j-1 < n else char_pos[-1]
                    if not self.is_occupied(start,end):
                        # header detection via alphabetic tokens in first row
                        first = lines[i]
                        has_header = bool(re.search(r'[A-Za-z]', first.split(cand)[0]))
                        ftype = "CSV" if has_header else "CSV_NO_HEADER"
                        conf = 0.9 if has_header else 0.7
                        self.add_block(DetectedBlock(ftype, start, end, conf, self.text[start:end], {"delimiter":cand, "rows": len(counts)}))
                        i = j
                        continue
            i += 1

    # ---------- 7. Key-Value detection ----------
    def detect_key_values(self):
        lines = self.text.splitlines()
        n = len(lines)
        char_pos = [0]
        for ln in lines:
            char_pos.append(char_pos[-1] + len(ln) + 1)
        i = 0
        while i < n:
            if re.match(r'^\s*[#\-]*\s*[\w\-\s]{1,80}\s*[:=]\s*.+', lines[i]):
                j = i
                kv_count = 0
                while j < n and re.match(r'^\s*[\w\-\s]{1,80}\s*[:=]\s*.+', lines[j]):
                    kv_count += 1
                    j += 1
                if kv_count >= 2:
                    start = char_pos[i]
                    end = char_pos[j-1] + len(lines[j-1])
                    if not self.is_occupied(start,end):
                        self.add_block(DetectedBlock("KEY_VALUE", start, end, 0.9, self.text[start:end], {"pairs": kv_count}))
                        i = j
                        continue
            i += 1

    # ---------- 8. JS Object detection ----------
    def detect_js_objects(self):
        try:
            for m in re.finditer(r'\b(var|let|const)\s+([A-Za-z0-9_$]+)\s*=\s*\{', self.text):
                start = m.start(0)
                if self.is_occupied(start, start+1):
                    continue
                span = find_json_span(self.text, m.start(0)+m.group(0).rfind('{'))
                if span:
                    s,e = span
                    if not self.is_occupied(s,e):
                        snippet = self.text[start:e]
                        self.add_block(DetectedBlock("JS_OBJECT", start, e, 0.88, snippet, {"var_name": m.group(2)}))
        except Exception:
            pass

    # ---------- 9. SQL detection ----------
    def detect_sql(self):
        # detect queries like SELECT ... ; or lines starting with -- comment then SELECT
        try:
            for m in re.finditer(r'(--[^\n]*\n\s*)?(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)\b[\s\S]{0,400}?\;', self.text, flags=re.IGNORECASE):
                start,end = m.start(), m.end()
                if not self.is_occupied(start,end):
                    self.add_block(DetectedBlock("SQL", start, end, 0.9, self.text[start:end], {}))
        except Exception:
            pass

    # ---------- 10. RAW text paragraphs (catch any remaining) ----------
    def detect_raw_text(self):
        # Partition by occupied spans and capture leftover textual chunks as RAW_TEXT
        spans = [(0,self.n)]
        # subtract occupied intervals
        for a,b in sorted(self.occupied):
            new_spans = []
            for s,e in spans:
                if b <= s or a >= e:
                    new_spans.append((s,e))
                else:
                    if s < a:
                        new_spans.append((s,a))
                    if b < e:
                        new_spans.append((b,e))
            spans = new_spans
        for s,e in spans:
            seg = self.text[s:e].strip()
            if len(seg) >= 20:
                # split into paragraphs of reasonable size
                parts = re.split(r'\n\s*\n', seg)
                pos = s
                for p in parts:
                    p = p.strip()
                    if not p:
                        pos += len(p) + 2
                        continue
                    start = self.text.find(p, pos, e)
                    if start == -1:
                        continue
                    end = start + len(p)
                    # don't create RAW_TEXT overlapping existing blocks
                    if not self.is_occupied(start,end):
                        self.add_block(DetectedBlock("RAW_TEXT", start, end, 0.35, self.text[start:end], {}))
                    pos = end

    # ---------- orchestrator ----------
    def run_all(self):
        # order matters: highest-confidence, containment-sensitive detectors first
        self.detect_json_ld()
        self.detect_yaml_frontmatter()
        self.detect_sectioned_jsons()
        self.detect_jsons_global()
        self.detect_html_tables_and_blocks()
        self.detect_js_objects()
        self.detect_csv_blocks()
        self.detect_key_values()
        self.detect_sql()
        self.detect_raw_text()
        # final sort by start_index
        self.blocks.sort(key=lambda b: b.start_index)
        # dedupe & prioritize
        self.blocks = self._dedupe_prioritize(self.blocks)
        return self.blocks

    def _dedupe_prioritize(self, blocks: List[DetectedBlock]) -> List[DetectedBlock]:
        """Remove low-priority blocks fully contained by higher-priority ones.
           Keep children (e.g., HTML_TABLE inside HTML) but avoid duplicates that provide no value.
        """
        kept: List[DetectedBlock] = []
        for b in sorted(blocks, key=lambda x: (x.start_index, -(x.end_index - x.start_index))):
            contained_by = None
            for k in kept:
                if b.start_index >= k.start_index and b.end_index <= k.end_index:
                    # b is inside k. Decide based on priority
                    try:
                        p_k = FORMAT_PRIORITY.index(k.format_type)
                    except ValueError:
                        p_k = len(FORMAT_PRIORITY)
                    try:
                        p_b = FORMAT_PRIORITY.index(b.format_type)
                    except ValueError:
                        p_b = len(FORMAT_PRIORITY)
                    if p_k <= p_b:
                        contained_by = k
                        break
            if contained_by is None:
                kept.append(b)
            else:
                # keep both only if child has strictly higher priority (lower index) than parent?
                # we prefer to keep both if child is higher-priority than parent
                try:
                    if FORMAT_PRIORITY.index(b.format_type) < FORMAT_PRIORITY.index(contained_by.format_type):
                        # replace parent with child+parent (child kept, parent kept) -> keep both
                        kept.append(b)
                    else:
                        # drop b
                        pass
                except Exception:
                    pass
        # final sort
        kept.sort(key=lambda x: x.start_index)
        # clamp confidences
        for k in kept:
            k.confidence = clamp_conf(k.confidence)
        return kept

# ---------- Normalizer (simple, safe) ----------
class Normalizer:
    """Convert block.text to structured python objects when possible (safe)."""
    @staticmethod
    def normalize(block: DetectedBlock) -> Optional[Any]:
        t = block.format_type
        s = block.text.strip()
        try:
            if t == "JSON" or t == "JSON_LD":
                # strict parse
                return json.loads(s)
            if t == "MALFORMED_JSON":
                # attempt conservative repairs
                repaired = _attempt_repair_json(s)
                if repaired is not None:
                    try:
                        return json.loads(repaired)
                    except Exception:
                        return _extract_kv_pairs(s)
                else:
                    return _extract_kv_pairs(s)
            if t in ("CSV","CSV_NO_HEADER"):
                return _safe_parse_csv(s, t=="CSV_NO_HEADER")
            if t == "HTML_TABLE":
                return _html_table_to_rows(s)
            if t == "KEY_VALUE":
                return _parse_kv(s)
            if t == "JS_OBJECT":
                # strip var/let/const assignment
                m = re.search(r'=\s*(\{[\s\S]*\})\s*;?$', s)
                if m:
                    obj = m.group(1)
                    # attempt quick conversion: single quotes -> double
                    obj2 = re.sub(r"'", '"', obj)
                    try:
                        return json.loads(obj2)
                    except:
                        # fallback to kv extraction
                        return _extract_kv_pairs(obj)
            if t == "SQL":
                return {"sql": s}
        except Exception:
            return None
        return None

# ---------- small helpers for Normalizer ----------
def _attempt_repair_json(s: str) -> Optional[str]:
    try:
        # remove trailing commas before } or ]
        s2 = re.sub(r',\s*(?=[}\]])', '', s)
        # convert single quotes around keys/values to double quotes (naive)
        s2 = re.sub(r"(?<=[:\s])'([^']*)'", r'"\1"', s2)
        # quote unquoted keys in simple cases: { key: -> { "key":
        s2 = re.sub(r'(?P<prefix>[\{,\s])(?P<key>[A-Za-z0-9_\-]+)\s*:', r'\g<prefix>"\g<key>":', s2)
        return s2
    except Exception:
        return None

def _extract_kv_pairs(s: str) -> Dict[str,str]:
    out = {}
    for k,v in re.findall(r'([A-Za-z0-9_\- ]{1,60})\s*[:=]\s*("[^"]*"|\'[^\']*\'|[^,\n]+)', s):
        val = v.strip().strip('"').strip("'")
        out[k.strip()] = val.strip()
    return out

def _parse_kv(s: str) -> Dict[str,Any]:
    out = {}
    for ln in s.splitlines():
        if ':' in ln:
            k,v = ln.split(':',1)
            out[k.strip()] = v.strip().strip('"')
    return out

def _html_table_to_rows(s: str) -> Optional[List[Dict[str,str]]]:
    try:
        soup = BeautifulSoup(s, "html.parser")
        table = soup.find("table")
        if not table:
            return None
        headers = []
        thead = table.find("thead")
        if thead:
            ths = thead.find_all("th")
            headers = [th.get_text(strip=True) for th in ths]
        rows = []
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td","th"])
            cells = [td.get_text(strip=True) for td in tds]
            if headers and len(cells)==len(headers):
                rows.append(dict(zip(headers,cells)))
            elif not headers and cells:
                # create synthetic headers
                rows.append({f"col_{i}": cells[i] for i in range(len(cells))})
        return rows or None
    except Exception:
        return None

def _safe_parse_csv(text: str, no_header: bool=False) -> Optional[List[Dict[str,str]]]:
    try:
        sio = StringIO(text.strip())
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(text.splitlines()[0]) if text.strip() else None
        reader = csv.reader(sio, dialect=dialect) if dialect else csv.reader(sio)
        rows = list(reader)
        if not rows:
            return None
        if no_header or len(rows) < 2:
            # create synthetic headers
            header = [f"col_{i}" for i in range(len(rows[0]))]
            return [dict(zip(header, r)) for r in rows]
        headers = rows[0]
        return [dict(zip(headers, r)) for r in rows[1:]]
    except Exception:
        # fallback: naive comma split
        try:
            rows = [line.split(',') for line in text.strip().splitlines() if line.strip()]
            if not rows:
                return None
            if len(rows) < 2:
                header = [f"col_{i}" for i in range(len(rows[0]))]
                return [dict(zip(header, r)) for r in rows]
            headers = rows[0]
            return [dict(zip(headers, r)) for r in rows[1:]]
        except Exception:
            return None

# ---------- Top-level parse_file API ----------
def parse_file(text: str) -> Dict[str,Any]:
    """
    Returns:
      {
        "fragments": List[DetectedBlock],
        "summary": dict counts,
        "records": List[Any]  # normalized outputs where possible
      }
    """
    detector = ETLFragmentDetector(text)
    blocks = detector.run_all()

    # Normalize detected blocks
    normalizer = Normalizer()
    records = []
    for b in blocks:
        try:
            rec = normalizer.normalize(b)
            if rec is not None:
                records.append({"format": b.format_type, "start": b.start_index, "end": b.end_index, "data": rec})
        except Exception:
            # defensive
            records.append({"format": b.format_type, "start": b.start_index, "end": b.end_index, "data": None})

    # summary
    summary = {}
    for b in blocks:
        summary[b.format_type] = summary.get(b.format_type, 0) + 1

    return {"fragments": blocks, "summary": summary, "records": records}

# ---------- If run as script, quick demo on file path ----------
if __name__ == "__main__":
    import sys, os
    if len(sys.argv) < 2:
        print("Usage: python etl_parser.py <input.txt>")
        sys.exit(1)
    path = sys.argv[1]
    if not os.path.exists(path):
        print("File not found:", path); sys.exit(1)
    with open(path, 'r', encoding='utf-8') as f:
        txt = f.read()
    out = parse_file(txt)
    print("\n=== FRAGMENTS DETECTED ===")
    for b in out["fragments"]:
        snippet = b.text.replace("\n"," ")[:180]
        print(f"{b.format_type} [{b.start_index}:{b.end_index}] conf={b.confidence:.2f}")
        print("  ", snippet, "\n")
    print("=== SUMMARY ===")
    print(out["summary"])
    print("=== NORMALIZED RECORDS COUNT ===", len(out["records"]))
