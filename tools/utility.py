import streamlit as st
import pandas as pd
import time
import csv
import requests

SUPSET_BASE_URL = "https://superset.nusantara-sakti.co.id"
LOGIN_URL = f"{SUPSET_BASE_URL}/api/v1/security/login"
SQL_URL = f"{SUPSET_BASE_URL}/api/v1/sqllab/execute/"

USERNAME = "adminadh"
PASSWORD = "Adhadmin234"



@st.cache_data(ttl=600) 
def get_access_token():
    payload = {
        "username": USERNAME,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(LOGIN_URL, headers=headers, json=payload)
    if response.status_code != 200:
        st.error(f"Gagal login ke Superset API. Status: {response.status_code}")
        st.text(response.text)
        return None
    
    data = response.json()
    access_token = data.get("access_token")
    return access_token


def fetch_superset_data(sql: str):
    token = get_access_token()
    if not token:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    payload = {
        "database_id": 3,
        "sql": sql,
        "runAsync": False,
        "expand_data": True,
        "json": True,
        "queryLimit": 100000
    }

    response = requests.post(SQL_URL, headers=headers, json=payload)
    if response.status_code != 200:
        st.error(f"Gagal ambil data dari Superset. Status: {response.status_code}")
        st.text(response.text)
        return None

    data = response.json()
    if "data" in data:
        df = pd.DataFrame(data["data"])
        return df
    else:
        st.warning("Tidak ada data ditemukan.")
        return None
    
def get_progrees_programs():
    sql = """
        SELECT
  psiPR.psikode as 'NIP',
  LEFT (psiGM.psinama, 12) as 'Nama DVH',
  LEFT (psiPM.psinama, 12) as 'Nama PM',
  LEFT (psiPR.psinama, 12) as 'Nama PROGRAMMER',
  cad.cadtot1 AS Target,
  COUNT(ssd.ssdket) AS Actual,
  COUNT(CASE WHEN tlm.tlmsts = 'CLSD' AND tlm.tlmbrdoto = 'REVIEW' THEN tlm.tlmnobuk ELSE NULL END) AS Review,
  COUNT(CASE WHEN tlm.tlmbrdoto = 'USER_UAT' AND tlm.tlmsts != 'VOID' THEN tlm.tlmnobuk ELSE NULL END) AS 'User UAT',
  (SELECT COUNT(*) FROM tgm
    WHERE tgm.tgmthbl = DATE_FORMAT(curdate(), '%y%m')
    AND tgm.tgmtype = 'HK'
    AND tgm.tgmke < 6
  ) AS totalHE,
  
  (SELECT COUNT(*) FROM tgm
    WHERE tgm.tgmtype = 'HK' AND tgm.tgmke < 6
    AND tgm.tgmthbl = DATE_FORMAT(CURDATE(), '%y%m')
    AND tgm.tgmtgl <= CURDATE()
  ) AS HE,
  
    SUM(CASE WHEN ssd.ssdtglbuk >= DATE_FORMAT(CURDATE(), '%Y-%m-01') AND cad.cadthbl = DATE_FORMAT(CURDATE(), '%y%m') THEN tlm.tlmperdisd ELSE 0 END) AS MENITM
  
FROM cad

LEFT JOIN tbt AS tbt ON tbt.tbtdata = "DTTABEL"
  AND tbt.tbtdtmkode = "INFOUMUM" AND tbt.tbttblkode = "PM196"
  AND tbt.tbtkode = cad.cadkode
  
LEFT JOIN psi AS psiGM ON psiGM.psikode = tbt.tbtkolek
  
LEFT JOIN psi AS psiPM ON psiPM.psikode = tbt.tbttbdkode
  
LEFT JOIN psi AS psiPR ON psiPR.psikode = tbt.tbtkode

LEFT JOIN tlm AS tlm ON tlm.tlmdata = "ITR"
  AND tlm.tlmnobbm = tbt.tbtkode

LEFT JOIN ssd AS ssd ON ssd.ssdnobuk = tlm.tlmnobuk 
  AND ssd.ssddata = "HARIAN196" AND ssd.ssdsts = "INPG"
  AND ssd.ssdtglbuk >= DATE_FORMAT(CURDATE(), '%Y-%m-01') AND ssd.ssdket = "CLSD"

WHERE cad.caddata = "TGTIT" AND cad.cadtype = DATE_FORMAT(curdate(), '%y%m')
  AND cad.cadthbl = DATE_FORMAT(curdate(), '%y%m') AND tbt.tbtkolek != tbt.tbtkode
  AND psiGM.psists = "INPG" AND psiPM.psists = "INPG" 
  AND psiPR.psists = "INPG"
  AND tbt.tbtkolek = 1021723
  
GROUP BY cad.cadkode
        """
    df = fetch_superset_data(sql)
    return df

def process_address_file_with_progress(filepath):
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        sample = f.read(4096)
        try:
            dialect = csv.Sniffer().sniff(sample)
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ","
    df = pd.read_csv(filepath, delimiter=delimiter, on_bad_lines="skip", engine="python")
    total = len(df)
    progress = st.progress(0)
    status_text = st.empty()

    results = []
    for i, row in df.iterrows():
        # Simulasi proses parsing
        results.append(row)  # ganti dengan proses asli
        progress_percent = int((i + 1) / total * 100)
        progress.progress(progress_percent)
        status_text.text(f"Memproses baris ke-{i+1}/{total} ({progress_percent}%)")

    return df