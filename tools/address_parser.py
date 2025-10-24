import pandas as pd
import re
import csv
import os
import logging
import difflib

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("address_parser")

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Muat dataset referensi wilayah
wilayah = pd.read_csv("data/data_wilayah.csv")

def parse_address_strict(address):
    if pd.isna(address):
        return None, None

    text = re.sub(r'\s+', ' ', str(address)).strip().lower()

    kec_match, kel_match = None, None
    for _, row in wilayah.iterrows():
        kec = row.get("kecamatan", "")
        kel = row.get("kelurahan", "")

        if isinstance(kec, str) and kec.strip() and kec.lower() in text:
            kec_match = kec

        if isinstance(kel, str) and kel.strip() and kel.lower() in text:
            kel_match = kel

        if kec_match and kel_match:
            break

    if kec_match and not kel_match:
        kel_in_kec = wilayah.loc[wilayah["kecamatan"] == kec_match, "kelurahan"]
        for kel in kel_in_kec:
            if isinstance(kel, str) and kel.lower() in text:
                kel_match = kel
                break

    return kec_match, kel_match

def extract_kecamatan_kelurahan_smart_v2(address):
    if pd.isna(address):
        return None, None
    if not isinstance(address, str):
        address = str(address)

    text = address.strip().replace("\n", " ").replace("\r", " ")
    text = re.sub(r'\s+', ' ', text).strip()

    kel_patterns = [
        r'\bKelurahan\.?\s*([A-Za-z0-9\s\.\'-]+?)(?=[,;]|$)',
        r'\bKel\.?\s*([A-Za-z0-9\s\.\'-]+?)(?=[,;]|$)',
        r'\bDesa\.?\s*([A-Za-z0-9\s\.\'-]+?)(?=[,;]|$)',
        r'\bDs\.?\s*([A-Za-z0-9\s\.\'-]+?)(?=[,;]|$)',
        r'\bGampong\.?\s*([A-Za-z0-9\s\.\'-]+?)(?=[,;]|$)',
        r'\bNagari\.?\s*([A-Za-z0-9\s\.\'-]+?)(?=[,;]|$)',
        r'\bJorong\.?\s*([A-Za-z0-9\s\.\'-]+?)(?=[,;]|$)'
    ]
    kec_patterns = [
        r'\bKecamatan\.?\s*([A-Za-z0-9\s\.\'-]+?)(?=[,;]|$)',
        r'\bKec\.?\s*([A-Za-z0-9\s\.\'-]+?)(?=[,;]|$)',
        r'\bDistrik\.?\s*([A-Za-z0-9\s\.\'-]+?)(?=[,;]|$)'
    ]

    kelurahan, kecamatan = None, None
    for p in kel_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            kelurahan = m.group(1).strip()
            break

    for p in kec_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            kecamatan = m.group(1).strip()
            break

    if not kelurahan or not kecamatan:
        parts = [p.strip() for p in text.split(',') if p.strip()]
        kab_idx = next((i for i, p in enumerate(parts)
                        if re.search(r'\b(Kota|City|Kab(?:\.|upaten)?|Regency)\b', p, re.IGNORECASE)), None)
        if kab_idx is not None:
            if kab_idx - 2 >= 0 and not kelurahan:
                kelurahan = parts[kab_idx - 2]
            if kab_idx - 1 >= 0 and not kecamatan:
                kecamatan = parts[kab_idx - 1]
        elif len(parts) >= 4:
            kelurahan = kelurahan or parts[-4]
            kecamatan = kecamatan or parts[-3]

    def clean_name(s):
        if not s:
            return None
        s = re.sub(r'(?i)\b(Kelurahan|Kel\.?|Desa|Ds\.?|Gampong|Nagari|Jorong|Kecamatan|Kec\.?|Distrik)\b', '', s)
        s = re.sub(r'\s+', ' ', s).strip(" .,-")
        return s.title() if s else None

    return clean_name(kecamatan), clean_name(kelurahan)

def extract_and_refine(address):
        # Langkah 1: Ekstraksi awal dengan regex
        kec, kel = extract_kecamatan_kelurahan_smart_v2(address)

        # Langkah 2: Jika hasil kosong atau tidak akurat, gunakan pencocokan referensi wilayah
        if not kec or not kel:
            strict_kec, strict_kel = parse_address_strict(address)
            kec = kec or strict_kec
            kel = kel or strict_kel

        return pd.Series([kec, kel])

def process_address_file(filepath):
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        sample = f.read(4096)
        try:
            dialect = csv.Sniffer().sniff(sample)
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ","

    df = pd.read_csv(filepath, delimiter=delimiter, on_bad_lines="skip", engine="python")
    address_col = next((c for c in df.columns if c.lower() in ["address", "alamat"]), None)
    if not address_col:
        raise ValueError("Kolom 'Address' atau 'Alamat' tidak ditemukan.")
    


    df[["Kecamatan", "Kelurahan"]] = df[address_col].apply(
        lambda x: pd.Series(extract_and_refine(x))
    )

    output_path = os.path.join(OUTPUT_DIR, "results.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")

    stats = {
        "Total Baris": len(df),
        "Kecamatan Terekstrak": int(df["Kecamatan"].astype(bool).sum()),
        "Kelurahan Terekstrak": int(df["Kelurahan"].astype(bool).sum()),
        "Output File": output_path,
    }

    return df, stats, output_path
