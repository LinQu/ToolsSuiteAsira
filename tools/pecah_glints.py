import pandas as pd
import re
import csv
import os
import zipfile
from datetime import datetime

def split_company_field(text):
    if not isinstance(text, str) or not text.strip():
        return [""]
    pattern = re.compile(r'([A-Za-z0-9 /&().\-]+ at [A-Za-z0-9 ,().\-]+, [A-Za-z]{3,9} \d{4} - [A-Za-z]{3,9} \d{4})')
    matches = re.findall(pattern, text)
    if matches:
        return matches
    parts = re.split(r'[\n;|]+', text)
    return [p.strip() for p in parts if p.strip()]

def split_school_field(text):
    if not isinstance(text, str) or not text.strip():
        return [""]
    pattern = re.compile(r'(in [A-Za-z0-9 /&().\-]+ at [A-Za-z0-9 ,().\-]+, [A-Za-z]{3,9} \d{4} - [A-Za-z]{3,9} \d{4})')
    matches = re.findall(pattern, text)
    if matches:
        return matches
    parts = re.split(r'[\n;|]+', text)
    return [p.strip() for p in parts if p.strip()]

def extract_years_months(value):
    match = re.search(r'(?:(\d+)\s*years?)?(?:,\s*)?(?:(\d+)\s*months?)?', str(value))
    if match:
        years = int(match.group(1)) if match.group(1) else 0
        months = int(match.group(2)) if match.group(2) else 0
        return pd.Series([years, months])
    return pd.Series([0, 0])

def process_glints_file(uploaded_file, max_rows):
    ext = os.path.splitext(uploaded_file.name)[1].lower()
    df = pd.read_excel(uploaded_file) if ext in [".xlsx", ".xls"] else pd.read_csv(uploaded_file)
    
    # Expand kolom Company
    expanded_rows = []
    for _, row in df.iterrows():
        for part in split_company_field(row.get("Company", "")):
            new_row = row.copy()
            new_row["Company"] = part
            expanded_rows.append(new_row)
    df_expanded = pd.DataFrame(expanded_rows)

    # Expand kolom School
    expanded_rows = []
    for _, row in df_expanded.iterrows():
        for part in split_school_field(row.get("School", "")):
            new_row = row.copy()
            new_row["School"] = part
            expanded_rows.append(new_row)
    df_expanded = pd.DataFrame(expanded_rows)

    # Years of Experience
    if "Years of Experience" in df_expanded.columns:
        df_expanded[["Tahun", "Bulan"]] = df_expanded["Years of Experience"].apply(extract_years_months)
        df_expanded.drop(columns=["Years of Experience"], inplace=True, errors="ignore")

    # Split menjadi beberapa file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"data/glints_split_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    total_rows = len(df_expanded)
    num_parts = (total_rows // max_rows) + (1 if total_rows % max_rows else 0)

    csv_paths = []
    for i in range(num_parts):
        part_df = df_expanded.iloc[i*max_rows:(i+1)*max_rows]
        part_path = os.path.join(output_dir, f"upldtglints_part{i+1}.csv")
        part_df.to_csv(part_path, index=False, sep="\t", header=False)
        csv_paths.append(part_path)

    # Zip hasil
    zip_path = f"{output_dir}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in csv_paths:
            zf.write(path, os.path.basename(path))

    # Statistik ringkas
    summary = pd.DataFrame({
        "File": [os.path.basename(p) for p in csv_paths],
        "Jumlah Baris": [len(pd.read_csv(p, sep="\t", header=None)) for p in csv_paths]
    })

    return zip_path, summary
