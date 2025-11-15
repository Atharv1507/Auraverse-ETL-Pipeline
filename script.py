#!/usr/bin/env python3
"""
Universal Data to JSON Converter
Parses complex scraped data with multiple formats into clean JSON
Usage: python converter.py <input_file> [output_file]
"""

import json
import re
import csv
import sys
from io import StringIO
from pathlib import Path
from html.parser import HTMLParser


# ============================================================================
# HTML TABLE PARSER
# ============================================================================

class HTMLTableParser(HTMLParser):
    """Extracts tables from HTML"""
    
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.is_header = False
        self.headers = []
        self.rows = []
        self.current_row = []
        self.current_cell = ""
    
    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.in_table = True
        elif tag == 'tr':
            self.in_row = True
            self.current_row = []
        elif tag == 'th':
            self.in_cell = True
            self.is_header = True
        elif tag == 'td':
            self.in_cell = True
            self.is_header = False
    
    def handle_endtag(self, tag):
        if tag == 'table':
            self.in_table = False
        elif tag == 'tr':
            self.in_row = False
            if self.is_header:
                self.headers = self.current_row
            else:
                self.rows.append(self.current_row)
        elif tag in ['th', 'td']:
            self.in_cell = False
            self.current_row.append(self.current_cell.strip())
            self.current_cell = ""
    
    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data
    
    def get_table_data(self):
        """Return parsed table as list of dicts"""
        if not self.headers or not self.rows:
            return []
        
        result = []
        for row in self.rows:
            if len(row) == len(self.headers):
                row_dict = dict(zip(self.headers, row))
                result.append(row_dict)
        return result


# ============================================================================
# MAIN CONVERTER CLASS
# ============================================================================

class DataConverter:
    """Converts various data formats to JSON"""
    
    def __init__(self):
        self.result = {}
    
    # ------------------------------------------------------------------------
    # MAIN PARSING LOGIC
    # ------------------------------------------------------------------------
    
    def parse(self, input_text):
        """Main entry point - parse input and return JSON dict"""
        input_text = input_text.strip()
        
        if not input_text:
            return {}
        
        # Check if input has section markers (---)
        if '---' in input_text:
            return self.parse_sections(input_text)
        else:
            return self.parse_single_format(input_text)
    
    def parse_sections(self, text):
        """Parse data with --- section dividers"""
        sections = re.split(r'\n---\s*', text)
        result = {}
        
        for i, section in enumerate(sections):
            section = section.strip()
            if not section:
                continue
            
            # Get section title and content
            lines = section.split('\n', 1)
            title = lines[0].strip() if lines else f"section_{i}"
            content = lines[1].strip() if len(lines) > 1 else section
            
            # Parse the section
            parsed = self.parse_single_format(content)
            
            # Merge based on section type
            result = self.merge_section(result, title, parsed)
        
        return self.flatten_single_keys(result)
    
    def merge_section(self, result, title, parsed):
        """Merge parsed section into result based on title"""
        title_upper = title.upper()
        
        # Metadata and key-value sections - merge to root
        if 'METADATA' in title_upper or 'KEY-VALUE' in title_upper:
            result.update(parsed)
        
        # JSON sections - merge to root
        elif 'JSON' in title_upper:
            result.update(parsed)
        
        # HTML/Table sections - add to arrays
        elif 'HTML' in title_upper or 'TABLE' in title_upper:
            if 'table_data' in parsed:
                if 'tables' not in result:
                    result['tables'] = []
                result['tables'].extend(parsed['table_data'])
        
        # CSV sections
        elif 'CSV' in title_upper:
            if 'table_data' in parsed:
                if 'csv_data' not in result:
                    result['csv_data'] = []
                result['csv_data'].extend(parsed['table_data'])
        
        # SQL/Code sections
        elif 'SQL' in title_upper or 'CODE' in title_upper:
            result['code_snippet'] = parsed.get('text', '')
        
        # OCR sections
        elif 'OCR' in title_upper:
            result.update(parsed)
        
        # Everything else - store under section name
        else:
            section_key = self.clean_key(title)
            result[section_key] = parsed
        
        return result
    
    # ------------------------------------------------------------------------
    # FORMAT DETECTION & PARSING
    # ------------------------------------------------------------------------
    
    def parse_single_format(self, text):
        """Detect format and parse accordingly"""
        text = text.strip()
        
        # JSON-LD in script tags
        if '<script' in text and 'application/ld+json' in text:
            return self.parse_json_ld(text)
        
        # Try to fix and parse JSON
        if '{' in text and '"' in text:
            json_data = self.try_parse_json(text)
            if json_data:
                return json_data
        
        # HTML tables
        if '<table' in text.lower():
            return self.parse_html_table(text)
        
        # CSV format
        if self.looks_like_csv(text):
            return self.parse_csv(text)
        
        # Key-value pairs
        if self.looks_like_key_value(text):
            return self.parse_key_value(text)
        
        # JavaScript variables
        if 'var ' in text and '{' in text:
            return self.parse_javascript(text)
        
        # Plain text (extract what we can)
        return self.parse_text(text)
    
    # ------------------------------------------------------------------------
    # JSON PARSING
    # ------------------------------------------------------------------------
    
    def try_parse_json(self, text):
        """Try to parse JSON, fixing common errors"""
        # Try as-is first
        try:
            return json.loads(text)
        except:
            pass
        
        # Extract JSON object
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if not match:
            return None
        
        json_str = match.group()
        
        # Apply common fixes
        json_str = self.fix_json(json_str)
        
        # Try parsing again
        try:
            return json.loads(json_str)
        except:
            # Last resort: extract key-value pairs manually
            return self.extract_json_pairs(json_str)
    
    def fix_json(self, json_str):
        """Fix common JSON syntax errors"""
        # Remove trailing commas
        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
        
        # Add missing commas between properties
        json_str = re.sub(r'"\s*\n\s*"', '",\n"', json_str)
        
        # Convert single quotes to double quotes
        json_str = re.sub(r"'([^']*)'", r'"\1"', json_str)
        
        # Quote unquoted keys (simple cases)
        json_str = re.sub(r'(\w+):', r'"\1":', json_str)
        
        return json_str
    
    def extract_json_pairs(self, json_str):
        """Extract key-value pairs from broken JSON"""
        result = {}
        
        # Match different value types
        patterns = [
            r'"(\w+)":\s*"([^"]*)"',      # String values
            r'"(\w+)":\s*(\d+\.?\d*)',    # Numbers
            r'"(\w+)":\s*(true|false)',   # Booleans
        ]
        
        for pattern in patterns:
            for key, value in re.findall(pattern, json_str):
                result[key] = self.convert_type(value)
        
        return result
    
    def parse_json_ld(self, text):
        """Extract JSON-LD from script tags"""
        pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        
        if not match:
            return {}
        
        try:
            data = json.loads(match.group(1))
            # Remove @context and @type
            return {k: v for k, v in data.items() if not k.startswith('@')}
        except:
            return {}
    
    # ------------------------------------------------------------------------
    # HTML TABLE PARSING
    # ------------------------------------------------------------------------
    
    def parse_html_table(self, text):
        """Parse HTML tables"""
        parser = HTMLTableParser()
        parser.feed(text)
        
        table_data = parser.get_table_data()
        if table_data:
            # Auto-convert types in each row
            for row in table_data:
                for key, value in row.items():
                    row[key] = self.convert_type(value)
            
            return {'table_data': table_data}
        
        return {}
    
    # ------------------------------------------------------------------------
    # CSV PARSING
    # ------------------------------------------------------------------------
    
    def looks_like_csv(self, text):
        """Check if text looks like CSV"""
        lines = [l for l in text.split('\n') if l.strip()]
        
        if len(lines) < 2:
            return False
        
        # Check consistent separators
        sep_count = lines[0].count(',') or lines[0].count('\t')
        if sep_count == 0:
            return False
        
        return all(
            l.count(',') == sep_count or l.count('\t') == sep_count
            for l in lines[:3]
        )
    
    def parse_csv(self, text):
        """Parse CSV data"""
        delimiter = ',' if ',' in text.split('\n')[0] else '\t'
        
        try:
            reader = csv.DictReader(StringIO(text), delimiter=delimiter)
            rows = list(reader)
            
            # Convert types
            for row in rows:
                for key, value in row.items():
                    row[key] = self.convert_type(value)
            
            # Single row = flat dict, multiple rows = array
            if len(rows) == 1:
                return rows[0]
            else:
                return {'table_data': rows}
        except:
            return {}
    
    # ------------------------------------------------------------------------
    # KEY-VALUE PARSING
    # ------------------------------------------------------------------------
    
    def looks_like_key_value(self, text):
        """Check if text is key-value format"""
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        
        if not lines:
            return False
        
        # Count lines matching key:value or key=value
        matches = sum(
            1 for line in lines
            if re.match(r'^\w+\s*[:=]\s*.+', line)
        )
        
        return matches > len(lines) * 0.5  # More than 50% match
    
    def parse_key_value(self, text):
        """Parse key:value or key=value pairs"""
        result = {}
        
        for line in text.split('\n'):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#') or line.startswith('//'):
                continue
            
            # Find separator
            if ': ' in line:
                key, value = line.split(': ', 1)
            elif '=' in line:
                key, value = line.split('=', 1)
            elif ':' in line:
                key, value = line.split(':', 1)
            else:
                continue
            
            key = self.clean_key(key)
            value = value.strip()
            
            # Handle semicolon-separated lists (e.g., tags: a;b;c)
            if ';' in value:
                value = [self.convert_type(v.strip()) for v in value.split(';')]
            else:
                value = self.convert_type(value)
            
            result[key] = value
        
        return result
    
    # ------------------------------------------------------------------------
    # JAVASCRIPT PARSING
    # ------------------------------------------------------------------------
    
    def parse_javascript(self, text):
        """Parse JavaScript variable assignments"""
        match = re.search(r'var\s+\w+\s*=\s*(\{[^}]+\})', text)
        
        if not match:
            return {}
        
        obj_str = match.group(1)
        
        # Convert JS syntax to JSON
        obj_str = re.sub(r'(\w+):', r'"\1":', obj_str)  # Quote keys
        obj_str = obj_str.replace("'", '"')             # Single to double quotes
        
        try:
            return json.loads(obj_str)
        except:
            return self.extract_json_pairs(obj_str)
    
    # ------------------------------------------------------------------------
    # TEXT PARSING
    # ------------------------------------------------------------------------
    
    def parse_text(self, text):
        """Extract structured data from plain text"""
        result = {}
        
        # Fix common OCR errors
        text = self.fix_ocr_errors(text)
        
        # Extract phone numbers
        phone = re.search(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', text)
        if phone:
            result['phone'] = phone.group()
        
        # Extract emails
        email = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
        if email:
            result['email'] = email.group()
        
        # Extract URLs
        urls = re.findall(r'https?://[^\s<>"]+', text)
        if urls:
            result['urls'] = urls
        
        # Extract prices
        prices = re.findall(r'\$?\d+[.,]\d{2}', text)
        if prices:
            result['prices'] = [p.replace(',', '.') for p in prices]
        
        # Extract key:value patterns
        for match in re.finditer(r'(\w+):\s*([^\n]+)', text):
            key = self.clean_key(match.group(1))
            value = match.group(2).strip()
            if len(key) > 2:
                result[key] = self.convert_type(value)
        
        # Store full text if we didn't extract much
        if len(result) < 2:
            result['text'] = text
        
        return result
    
    # ------------------------------------------------------------------------
    # HELPER FUNCTIONS
    # ------------------------------------------------------------------------
    
    def fix_ocr_errors(self, text):
        """Fix common OCR mistakes"""
        fixes = [
            (r'l0cation', 'location'),
            (r'\bO\b(?=\d)', '0'),
            (r'one hundred and twenty', '120'),
        ]
        
        for pattern, replacement in fixes:
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
        return text
    
    def convert_type(self, value):
        """Convert string to appropriate type"""
        if not isinstance(value, str):
            return value
        
        value = value.strip().strip('"\'')
        
        # Booleans
        if value.lower() in ('true', 'yes', 'on'):
            return True
        if value.lower() in ('false', 'no', 'off'):
            return False
        
        # Null/None
        if value.lower() in ('null', 'none', 'n/a', 'na'):
            return None
        
        # Numbers
        try:
            if '.' in value:
                return float(value.replace(',', '.'))
            return int(value)
        except ValueError:
            pass
        
        return value
    
    def clean_key(self, key):
        """Clean and normalize dictionary keys"""
        # Remove special characters
        key = re.sub(r'[^\w\s-]', '', key)
        # Replace spaces with underscores
        key = re.sub(r'\s+', '_', key.strip())
        return key.lower()
    
    def flatten_single_keys(self, data):
        """Flatten nested dicts with only one key"""
        if not isinstance(data, dict):
            return data
        
        result = {}
        for key, value in data.items():
            if isinstance(value, dict) and len(value) == 1:
                # Flatten: {user: {name: "John"}} -> {user_name: "John"}
                nested_key, nested_value = next(iter(value.items()))
                result[f"{key}_{nested_key}"] = nested_value
            elif isinstance(value, dict):
                result[key] = self.flatten_single_keys(value)
            else:
                result[key] = value
        
        return result


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main entry point"""
    
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python converter.py <input_file> [output_file]")
        sys.exit(1)
    
    # Get input file
    input_file = Path(sys.argv[1])
    if not input_file.exists():
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)
    
    # Get output file
    if len(sys.argv) > 2:
        output_file = Path(sys.argv[2])
    else:
        output_file = input_file.with_stem(input_file.stem + '_parsed2').with_suffix('.json')
    
    # Read input
    print(f"Reading from: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        input_text = f.read()
    
    # Parse data
    print("Parsing data...")
    converter = DataConverter()
    result = converter.parse(input_text)
    
    # Write output
    print(f"Writing to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Successfully parsed {len(result)} top-level keys")
    print(f"✓ Output saved to: {output_file}")


if __name__ == "__main__":
    main()