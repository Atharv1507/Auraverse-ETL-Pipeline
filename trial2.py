from etl_parser import parse_file

with open("input.txt", "r", encoding="utf-8") as f:
    text = f.read()

result = parse_file(text)

print("\n=== FRAGMENTS DETECTED ===")
for b in result["fragments"]:
    print(f"{b.format_type} [{b.start_index}:{b.end_index}] conf={b.confidence}")
    print("  ", b.text[:80].replace("\n", " ") + "...")
    print()

print("\n=== SUMMARY ===")
print(result["summary"])

print("\n=== NORMALIZED RECORDS (first 3) ===")
for r in result["records"][:3]:
    print(r)

print("\n=== SCHEMA FIELDS (first 10) ===")
for f in result["fields"][:10]:
    print(f.path, "->", f.type, "(example:", f.example, ")")
