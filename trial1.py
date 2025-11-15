from format_detector import detect_formats
with open("input.txt", "r", encoding="utf-8") as f:
    text = f.read()
blocks = detect_formats(text)
for b in blocks:
    print(b.format_type, b.start_index, b.end_index, b.confidence)
    # b.text contains the raw block
