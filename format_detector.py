# #!/usr/bin/env python3
# """
# format_detector.py
# Detect structured-format blocks inside unstructured text (no ML).
# Returns: List[DetectedBlock] with start/end indices and confidence.
# Dependencies: beautifulsoup4
# """
#
# from __future__ import annotations
# import re
# import json
# import csv
# from io import StringIO
# from dataclasses import dataclass, asdict
# from typing import List, Optional, Dict, Any, Tuple
# from bs4 import BeautifulSoup
#
# # -------------------------
# # Public types
# # -------------------------
# @dataclass
# class DetectedBlock:
#     format_type: str
#     start_index: int
#     end_index: int
#     confidence: float  # 0.0 .. 1.0
#     text: str
#     meta: Dict[str, Any]
#
# # -------------------------
# # Constants / priorities
# # -------------------------
# FORMAT_PRIORITY = [
#     "JSON_LD",
#     "JSON",
#     "MALFORMED_JSON",
#     "HTML_TABLE",
#     "HTML",
#     "YAML_FRONTMATTER",
#     "CSV",
#     "CSV_NO_HEADER",
#     "KEY_VALUE",
#     "JS_OBJECT",
#     "SQL",
#     "RAW_TEXT",
# ]
#
# # -------------------------
# # Helpers: safe JSON fixes
# # -------------------------
# def _try_parse_json(s: str) -> Tuple[Optional[Any], float, str]:
#     """
#     Try to parse s into JSON. Returns (obj or None, confidence (0-1), cleaned_string_used)
#     Confidence heuristics:
#       - parse success as-is: 0.98
#       - parse after small fixes: 0.7-0.85
#       - partial key-value extraction: 0.4
#     """
#     raw = s.strip()
#     # Try exact parse first
#     try:
#         obj = json.loads(raw)
#         return obj, 0.98, raw
#     except Exception:
#         pass
#
#     # Extract largest {...} substring (protective)
#     m = re.search(r'\{[\s\S]*\}', raw)
#     if not m:
#         return None, 0.0, ""
#     candidate = m.group(0)
#
#     # Apply conservative fixes:
#     cleaned = candidate
#
#     # 1) remove trailing commas before } or ]
#     cleaned = re.sub(r',\s*(?=[}\]])', '', cleaned)
#
#     # 2) convert single quotes to double quotes when used for values (but be careful with apostrophes)
#     # Basic approach: replace single-quoted property values when they are not inner apostrophes
#     cleaned = re.sub(r"(?<=:)\s*'([^']*)'", r' "\1"', cleaned)
#     cleaned = re.sub(r"'(\w+)'\s*:", r'"\1":', cleaned)  # keys in single quotes
#
#     # 3) add missing comma between "value"}" patterns where a property is followed by another key without comma
#     cleaned = re.sub(r'"\s*}\s*"\s*:', r'"},"', cleaned)  # unlikely helpful but safe attempt
#
#     # 4) quote unquoted keys simple pattern: { key: -> { "key":
#     cleaned = re.sub(r'(?P<prefix>[{,]\s*)(?P<key>[A-Za-z_][A-Za-z0-9_ -]*)\s*:', lambda m: f'{m.group("prefix")}"{m.group("key").strip()}" :', cleaned)
#
#     # Now try parse again
#     try:
#         obj = json.loads(cleaned)
#         return obj, 0.82, cleaned
#     except Exception:
#         # Fall back to extracting key:value pairs conservatively
#         pairs = {}
#         # match "key": "value" or key: "value" or "key": value
#         for k, v in re.findall(r'"?([A-Za-z0-9_\- ]{1,60})"?\s*:\s*(?:"([^"]*)"|\'([^\']*)\'|([0-9.\-]+)|(true|false|null))', cleaned, flags=re.IGNORECASE):
#             value = v or None
#             if not value:
#                 # check groups
#                 groups = re.search(rf'{re.escape(k)}\s*:\s*(?:"([^"]*)"|\'([^\']*)\'|([0-9.\-]+)|(true|false|null))', cleaned, flags=re.IGNORECASE)
#                 if groups:
#                     # pick first non-empty group
#                     for item in groups.groups()[1:]:
#                         if item:
#                             value = item
#                             break
#             pairs[k.strip()] = value
#         if pairs:
#             return pairs, 0.38, cleaned
#         return None, 0.0, cleaned
#
# # -------------------------
# # Detectors (return list of DetectedBlock)
# # -------------------------
# def detect_json_ld(text: str) -> List[DetectedBlock]:
#     """Find <script type="application/ld+json"> blocks using regex + BeautifulSoup"""
#     blocks = []
#     # Use regex to find script tags to get indices reliably
#     for m in re.finditer(r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>', text, flags=re.IGNORECASE):
#         content = m.group(1).strip()
#         start, end = m.start(1), m.end(1)
#         parsed, conf, used = _try_parse_json(content)
#         meta = {"note": "json-ld", "parsed": isinstance(parsed, (dict, list)), "parse_confidence": conf}
#         blocks.append(DetectedBlock("JSON_LD", start, end, min(conf + 0.02, 0.99), text[start:end], meta))
#     return blocks
#
# def detect_html_tables_and_html(text: str) -> List[DetectedBlock]:
#     """Find <table> spans (high priority) and general HTML spans"""
#     blocks = []
#     # Tables: find each <table ...>...</table>
#     for m in re.finditer(r'(<table\b[\s\S]*?>[\s\S]*?<\/table>)', text, flags=re.IGNORECASE):
#         block_text = m.group(1)
#         start, end = m.start(1), m.end(1)
#         # parse table via BeautifulSoup to see how many rows/cols
#         try:
#             soup = BeautifulSoup(block_text, "html.parser")
#             rows = soup.find_all("tr")
#             cols = max((len(r.find_all(["td","th"])) for r in rows), default=0)
#             confidence = 0.9 if rows and cols >= 1 else 0.6
#             meta = {"rows": len(rows), "cols_estimate": cols}
#         except Exception as e:
#             confidence = 0.5
#             meta = {"error": str(e)}
#         blocks.append(DetectedBlock("HTML_TABLE", start, end, confidence, block_text, meta))
#
#     # HTML blocks: heuristics - look for <div|<section|<html|<body|<script|<a|<p etc.
#     for m in re.finditer(r'(<(?:div|section|html|body|script|header|footer|article|main|nav|aside)\b[\s\S]*?>[\s\S]*?<\/(?:div|section|html|body|script|header|footer|article|main|nav|aside)>)', text, flags=re.IGNORECASE):
#         # exclude those table regions we already captured (we will dedupe later)
#         start, end = m.start(1), m.end(1)
#         snippet = text[start:end]
#         # confidence heuristic: presence of tags + balanced tags
#         tag_count = len(re.findall(r'<[A-Za-z]', snippet))
#         closing_count = len(re.findall(r'</', snippet))
#         confidence = min(0.9, 0.4 + min(tag_count, closing_count) * 0.05)
#         blocks.append(DetectedBlock("HTML", start, end, confidence, snippet, {"tag_count": tag_count, "closing_count": closing_count}))
#
#     return blocks
#
# def detect_yaml_frontmatter(text: str) -> List[DetectedBlock]:
#     """Detect YAML frontmatter blocks like: ---\nkey: value\n---"""
#     blocks = []
#     for m in re.finditer(r'(^|\n)---\s*\n([\s\S]{0,2000}?)\n---', text, flags=re.MULTILINE):
#         inner = m.group(2)
#         start, end = m.start(2), m.end(2)
#         # simple heuristic: lines with ':'
#         lines = [l for l in inner.splitlines() if l.strip()]
#         colon_ratio = sum(1 for l in lines if ':' in l) / max(1, len(lines))
#         confidence = 0.95 if colon_ratio > 0.5 else 0.6
#         blocks.append(DetectedBlock("YAML_FRONTMATTER", start, end, confidence, inner, {"colon_ratio": colon_ratio}))
#     return blocks
#
# def detect_csv_like(text: str, max_lines_candidate: int = 80) -> List[DetectedBlock]:
#     """Detect CSV-like sequences (comma or tab separated). Use csv.Sniffer to confirm.
#        Returns CSV (with headers) or CSV_NO_HEADER.
#     """
#     blocks = []
#     # Roughly find sequences of lines that contain repeated separators
#     lines = text.splitlines()
#     n = len(lines)
#     i = 0
#     while i < n:
#         # skip blank lines
#         if not lines[i].strip():
#             i += 1
#             continue
#         # consider chunk up to max_lines_candidate
#         j = i
#         chunk = []
#         while j < n and len(chunk) < max_lines_candidate and lines[j].strip():
#             chunk.append(lines[j])
#             j += 1
#         if len(chunk) < 2:
#             i = j + 1
#             continue
#         candidate = "\n".join(chunk)
#         # check for likely delimiter
#         comma_score = sum(1 for row in chunk[:10] if row.count(',') >= 1)
#         tab_score = sum(1 for row in chunk[:10] if '\t' in row)
#         if comma_score + tab_score < 2:
#             i = j + 1
#             continue
#
#         # feed to csv.Sniffer to deduce dialect
#         try:
#             sniff = csv.Sniffer()
#             dialect = sniff.sniff(candidate)
#             has_header = sniff.has_header(candidate)
#             delimiter = dialect.delimiter
#             # calculate start/end indices in original text string
#             start_pos = _pos_of_line_in_text(text, i)
#             end_pos = _pos_of_line_in_text(text, j-1) + len(lines[j-1]) if j-1 < n else len(text)
#             # parse rows safely
#             reader = csv.reader(StringIO(candidate), delimiter=delimiter)
#             parsed = [r for r in reader]
#             # handle no header
#             if has_header and parsed:
#                 header = parsed[0]
#                 rows = parsed[1:]
#                 meta = {"delimiter": delimiter, "has_header": True, "header": header, "rows": len(rows)}
#                 conf = 0.88
#                 ftype = "CSV"
#             else:
#                 # create synthetic headers like col_0,.. if row length is consistent
#                 widths = [len(r) for r in parsed]
#                 most_common_width = max(set(widths), key=widths.count) if widths else 0
#                 if most_common_width >= 1:
#                     header = [f"col_{i}" for i in range(most_common_width)]
#                     # normalize rows to width
#                     rows_norm = [r + [""]*(most_common_width - len(r)) if len(r) < most_common_width else r[:most_common_width] for r in parsed]
#                     meta = {"delimiter": delimiter, "has_header": False, "synthetic_header": header, "rows": len(rows_norm)}
#                     conf = 0.7
#                     ftype = "CSV_NO_HEADER"
#                 else:
#                     i = j + 1
#                     continue
#             blocks.append(DetectedBlock(ftype, start_pos, end_pos, conf, text[start_pos:end_pos], meta))
#             # move pointer beyond chunk
#             i = j + 1
#         except Exception:
#             # Not a CSV
#             i = j + 1
#     return blocks
#
# def detect_key_value_blocks(text: str) -> List[DetectedBlock]:
#     """Detect contiguous key:value lines"""
#     blocks = []
#     # group contiguous lines having "key: value" or "key = value"
#     lines = text.splitlines()
#     n = len(lines)
#     i = 0
#     while i < n:
#         if re.match(r'^\s*(#|//)', lines[i]) or not lines[i].strip():
#             i += 1
#             continue
#         if re.match(r'^\s*[\w\-\s]{1,60}\s*[:=]\s*.+$', lines[i]):
#             j = i
#             kv_lines = []
#             while j < n and re.match(r'^\s*[\w\-\s]{1,60}\s*[:=]\s*.+$', lines[j]):
#                 kv_lines.append(lines[j])
#                 j += 1
#             # only consider block if enough density
#             ratio = len(kv_lines) / max(1, (j - i))
#             if ratio >= 0.6 and len(kv_lines) >= 2:
#                 start_pos = _pos_of_line_in_text(text, i)
#                 end_pos = _pos_of_line_in_text(text, j-1) + len(lines[j-1])
#                 # quick parse of key-values
#                 kv = {}
#                 for l in kv_lines:
#                     try:
#                         k, v = re.split(r'\s*[:=]\s*', l, maxsplit=1)
#                         kv[k.strip()] = v.strip()
#                     except Exception:
#                         continue
#                 blocks.append(DetectedBlock("KEY_VALUE", start_pos, end_pos, 0.86, text[start_pos:end_pos], {"pairs": len(kv)}))
#                 i = j
#                 continue
#         i += 1
#     return blocks
#
# def detect_sql_snippets(text: str) -> List[DetectedBlock]:
#     """Detect SQL-like code blocks (but never execute). Use regex for typical keywords."""
#     blocks = []
#     # code fences with SQL comment style or BEGIN...; or SELECT ...;
#     for m in re.finditer(r'(--[^\n]*\n)?\s*(?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)\b[\s\S]{0,1000}?(?:;|\n)', text, flags=re.IGNORECASE):
#         s, e = m.start(), m.end()
#         snippet = text[s:e]
#         # heuristics: presence of SELECT or FROM increases confidence
#         conf = 0.9 if re.search(r'\bSELECT\b', snippet, flags=re.IGNORECASE) and re.search(r'\bFROM\b', snippet, flags=re.IGNORECASE) else 0.6
#         blocks.append(DetectedBlock("SQL", s, e, conf, snippet, {}))
#     return blocks
#
# def detect_js_objects(text: str) -> List[DetectedBlock]:
#     """Detect JS var/const/let assignment of object literals e.g. var x = { ... }; and inline script objects"""
#     blocks = []
#     for m in re.finditer(r'\b(?:var|let|const)\s+([A-Za-z0-9_\$]+)\s*=\s*(\{[\s\S]{0,2000}?\})\s*;', text, flags=re.IGNORECASE):
#         name = m.group(1)
#         obj_text = m.group(2)
#         s, e = m.start(2), m.end(2)
#         # convert to JSON-like and try parse
#         candidate = re.sub(r'([A-Za-z0-9_\$]+)\s*:', r'"\1":', obj_text)
#         candidate = candidate.replace("'", '"')
#         try:
#             parsed = json.loads(candidate)
#             conf = 0.88
#             meta = {"var_name": name, "parsed": True}
#         except Exception:
#             parsed = None
#             conf = 0.45
#             meta = {"var_name": name, "parsed": False}
#         blocks.append(DetectedBlock("JS_OBJECT", s, e, conf, obj_text, meta))
#     return blocks
#
# def detect_json_blocks(text: str) -> List[DetectedBlock]:
#     """Find large {...} JSON-like blocks. Mark as JSON or MALFORMED_JSON based on parse success."""
#     blocks = []
#     for m in re.finditer(r'(\{[\s\S]{10,5000}?\})', text):
#         snippet = m.group(1)
#         start, end = m.start(1), m.end(1)
#         parsed, conf, used = _try_parse_json(snippet)
#         if parsed is not None:
#             # If parsed and top-level is dict/list
#             if isinstance(parsed, (dict, list)):
#                 ftype = "JSON" if conf > 0.9 else "JSON"
#                 blocks.append(DetectedBlock(ftype, start, end, min(conf, 0.98), snippet, {"len": len(snippet)}))
#             else:
#                 blocks.append(DetectedBlock("JSON", start, end, conf, snippet, {"note": "parsed non-dict top-level"}))
#         else:
#             # Candidate but not parsed -> mark MALFORMED_JSON with low-medium confidence
#             # Only mark if it contains quotes/colons (heuristic)
#             if re.search(r'"\s*:\s*', snippet) or re.search(r'\w+\s*:\s*', snippet):
#                 blocks.append(DetectedBlock("MALFORMED_JSON", start, end, 0.35, snippet, {"note": "failed_parse"}))
#     return blocks
#
# # -------------------------
# # Utilities
# # -------------------------
# def _pos_of_line_in_text(text: str, line_idx: int) -> int:
#     """Return character index in text for the start of line number `line_idx` (0-based)."""
#     if line_idx <= 0:
#         return 0
#     lines = text.splitlines(keepends=True)
#     if line_idx >= len(lines):
#         return len(text)
#     pos = sum(len(lines[i]) for i in range(line_idx))
#     return pos
#
# def _dedupe_and_prioritize(blocks: List[DetectedBlock]) -> List[DetectedBlock]:
#     """Remove lower-priority blocks that are fully contained in higher-priority ones
#        and collapse small overlaps. Uses FORMAT_PRIORITY order.
#     """
#     if not blocks:
#         return []
#     # sort by start then by -confidence then by priority index
#     def prio(b: DetectedBlock):
#         try:
#             p = FORMAT_PRIORITY.index(b.format_type)
#         except ValueError:
#             p = len(FORMAT_PRIORITY)
#         return (b.start_index, -b.confidence, p, - (b.end_index - b.start_index))
#     blocks_sorted = sorted(blocks, key=prio)
#
#     kept: List[DetectedBlock] = []
#     for b in blocks_sorted:
#         overlap = False
#         for k in kept:
#             if b.start_index >= k.start_index and b.end_index <= k.end_index:
#                 # b fully inside k -> drop b if k has higher priority or similar confidence
#                 try:
#                     if FORMAT_PRIORITY.index(k.format_type) <= FORMAT_PRIORITY.index(b.format_type) and k.confidence >= b.confidence - 0.15:
#                         overlap = True
#                         break
#                 except ValueError:
#                     pass
#             # if partial overlap and b has much higher confidence, replace
#             if not overlap and not (b.end_index <= k.start_index or b.start_index >= k.end_index):
#                 inter = max(0, min(b.end_index, k.end_index) - max(b.start_index, k.start_index))
#                 if inter > 0 and b.confidence > k.confidence + 0.2:
#                     # drop k
#                     kept.remove(k)
#                 elif inter > 0 and k.confidence >= b.confidence:
#                     overlap = True
#                     break
#         if not overlap:
#             kept.append(b)
#     # final sort by start position
#     return sorted(kept, key=lambda x: x.start_index)
#
# # -------------------------
# # Top-level API
# # -------------------------
# def detect_formats(text: str) -> List[DetectedBlock]:
#     """
#     Detect various format blocks in `text`.
#     Returns: List[DetectedBlock] (may be empty)
#     """
#     if not isinstance(text, str):
#         raise TypeError("text must be a str")
#
#     all_blocks: List[DetectedBlock] = []
#
#     try:
#         # 1. JSON-LD in script tags
#         all_blocks.extend(detect_json_ld(text))
#         # 2. HTML tables and generic HTML blocks
#         all_blocks.extend(detect_html_tables_and_html(text))
#         # 3. YAML frontmatter
#         all_blocks.extend(detect_yaml_frontmatter(text))
#         # 4. CSV-like sequences
#         all_blocks.extend(detect_csv_like(text))
#         # 5. Key-value blocks
#         all_blocks.extend(detect_key_value_blocks(text))
#         # 6. JS object assignments
#         all_blocks.extend(detect_js_objects(text))
#         # 7. SQL-ish snippets
#         all_blocks.extend(detect_sql_snippets(text))
#         # 8. JSON blocks (including malformed)
#         all_blocks.extend(detect_json_blocks(text))
#     except Exception as exc:
#         # defensive: return what we have plus an error block
#         all_blocks.append(DetectedBlock("ERROR", 0, 0, 0.0, "", {"error": str(exc)}))
#
#     # If nothing found, return a RAW_TEXT block with low confidence
#     if not all_blocks:
#         all_blocks.append(DetectedBlock("RAW_TEXT", 0, len(text), 0.2, text, {}))
#
#     # dedupe/resolve overlaps
#     final = _dedupe_and_prioritize(all_blocks)
#
#     # ensure confidence bounds
#     for b in final:
#         if b.confidence < 0.0:
#             b.confidence = 0.0
#         if b.confidence > 1.0:
#             b.confidence = 1.0
#
#     return final
#
# # -------------------------
# # Example / quick test
# # -------------------------
# if __name__ == "__main__":
#     # quick smoke test using user's example (shortened for demo); you can paste the full sample.
#     sample_text = r"""
# --- INLINE JSON (well-formed)
# {
#   "id": "prod-1001",
#   "title": "Widget A",
#   "pricing": { "price_usd": "9.99", "inventory": 120 }
# }
#
# --- MALFORMED JSON FRAGMENT (common in scraped text)
# { "id": "prod-1001-b", "title": "Widget B", "specs": { "color": "red", "weight": "0.5kg", }  "notes": "missing comma and trailing comma issues"
#
# <div class="reviews">
#   <table><tr><th>author</th><th>rating</th></tr><tr><td>Alice</td><td>5</td></tr></table>
# </div>
#
# author,rating,date
# Dave,4,2025-10-18
# Eve,2,18-10-2025
#
# <script type="application/ld+json">
# {"@context":"http://schema.org","@type":"Product","name":"Widget A","sku":"WA-1001"}
# </script>
#
# -- SQL comment
# SELECT id, title FROM products WHERE price < 20;
# """
#     detected = detect_formats(sample_text)
#     for d in detected:
#         print(f"{d.format_type} [{d.start_index}:{d.end_index}] conf={d.confidence:.2f} meta={d.meta}")
#         snippet = d.text.strip().replace("\n", " ")[:120]
#         print(f"  -> {snippet}...\n")

