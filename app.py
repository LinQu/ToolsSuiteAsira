import streamlit as st
import pandas as pd
import os
import shutil
import zipfile
from tools.address_parser import process_address_file
from tools import convertsql as csv_to_sql
from tools import pecah_glints  # 🔥 modul baru
from tools import utility  # 🔥 modul baru

# ==============================
# KONFIGURASI DASAR HALAMAN
# ==============================
st.set_page_config(
    page_title="Group 106 - Tools Suite",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================
# SIDEBAR
# ==============================
st.sidebar.title("🧭 Group 106 - Tools Suite")
st.sidebar.markdown("---")

# Bagian Beranda
st.sidebar.header("🏠 Beranda")
st.sidebar.info(
    "Selamat datang di **Group 106 - Tools Suite** — kumpulan alat bantu "
    "untuk ekstraksi, konversi, dan pecah data besar."
)

# Bagian Tools (dengan Expander)
st.sidebar.markdown("### 🧰 Pilih Tools")

with st.sidebar.expander("📂 Modul Ekstraksi & Konversi", expanded=True):
    menu = st.radio(
        "Pilih Tools:",
        [
            "🧭 Beranda",
            "🏠 Ekstraksi Alamat",
            "💾 Convert CSV to SQL",
            "📑 Pecah Data Laporan Glints"
        ],
        label_visibility="collapsed"
    )


st.sidebar.markdown("---")
with st.sidebar.expander("ℹ️ Tentang Aplikasi"):
    st.sidebar.markdown("""
    **Group 106 - Tools Suite v1.2**  
    Dibuat oleh: **Asira Dev Studio**  
    - Regex Extraction Engine  
    - Smart CSV & SQL Converter  
    - Glints Data Splitter Automation  
    """)


# ======================================================
# 1️⃣ MENU: Beranda
# ======================================================

if menu == "🧭 Beranda":
    st.title("🧭 Selamat Datang di Group 106 - Tools Suite")
    st.markdown("""
    Selamat datang di **Group 106 - Tools Suite**, kumpulan alat bantu yang dirancang untuk memudahkan pekerjaan Anda dalam mengelola data.  
    Pilih salah satu tools dari sidebar untuk memulai:
    
    1. **🏠 Ekstraksi Alamat**: Ekstrak informasi Kecamatan dan Kelurahan dari data alamat secara otomatis.
    2. **💾 Convert CSV to SQL**: Konversi file CSV menjadi skrip SQL `INSERT IGNORE` dengan mudah.
    3. **📑 Pecah Data Laporan Glints**: Pecah file laporan Glints besar menjadi beberapa file berdasarkan jumlah baris yang ditentukan.
    
    Gunakan tools ini untuk meningkatkan efisiensi kerja Anda dalam mengelola data!
    """)
    with st.spinner("Mengambil data dari Superset..."):
        df = utility.get_progrees_programs()
        if df is not None and not df.empty:
            st.success(f"✅ Data berhasil dimuat ({len(df)} baris)")
            
            # Tampilkan tabel
            st.dataframe(df, use_container_width=True)





# ======================================================
# 1️⃣ MENU: Ekstraksi Alamat
# ======================================================
elif menu == "🏠 Ekstraksi Alamat":
    st.title("📍 Ekstraksi Kecamatan & Kelurahan dari Alamat")
    st.markdown("""
    Unggah file CSV dengan kolom **Address/Alamat**,  
    sistem akan mengekstrak **Kecamatan** dan **Kelurahan** otomatis menggunakan regex.
    """)

    uploaded_file = st.file_uploader("📂 Upload file CSV", type=["csv"])
    
    if uploaded_file is not None:
        os.makedirs("data/uploads", exist_ok=True)
        input_path = os.path.join("data/uploads", uploaded_file.name)
        with open(input_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        with st.spinner("🔍 Memproses data..."):
            try:
                #utility.process_address_file_with_progress(input_path)
                
                df_result, stats, output_path = process_address_file(input_path)
                st.success("✅ Ekstraksi selesai!")

                st.subheader("📊 Statistik Ekstraksi")
                st.write(stats)

                st.subheader("🪄 Preview Hasil")
                st.dataframe(df_result.head(10))

                with open(output_path, "rb") as f:
                    st.download_button(
                        label="💾 Download Hasil CSV",
                        data=f,
                        file_name=os.path.basename(output_path),
                        mime="text/csv"
                    )

            except Exception as e:
                st.error(f"❌ Terjadi kesalahan: {e}")

# ======================================================
# 2️⃣ MENU: Convert CSV to SQL
# ======================================================
elif menu == "💾 Convert CSV to SQL":
    st.title("💾 Convert CSV to SQL")
    st.markdown("Ubah file CSV menjadi skrip SQL `INSERT IGNORE` otomatis.")

    uploaded_csv = st.file_uploader("📂 Unggah file CSV", type=["csv"])
    table_name = st.text_input("🗃️ Nama tabel tujuan", placeholder="contoh: data_customer")

    if uploaded_csv and table_name:
        with st.spinner("🛠 Mengonversi..."):
            try:
                os.makedirs("data/uploads", exist_ok=True)
                input_path = os.path.join("data/uploads", uploaded_csv.name)
                with open(input_path, "wb") as f:
                    f.write(uploaded_csv.getbuffer())
                utility.process_address_file_with_progress(input_path)
                sql_content = csv_to_sql.convert_csv_to_sql(uploaded_csv, table_name)
                st.success("✅ Konversi berhasil dilakukan!")

                st.code("\n".join(sql_content.splitlines()[:5]) + "\n...", language="sql")

                st.download_button(
                    label="📥 Download File SQL",
                    data=sql_content.encode("utf-8"),
                    file_name=f"{table_name}.sql",
                    mime="text/plain"
                )
            except Exception as e:
                st.error(f"❌ Gagal mengonversi CSV: {e}")

# ======================================================
# 3️⃣ MENU: Pecah Data Laporan Glints
# ======================================================
elif menu == "📑 Pecah Data Laporan Glints":
    st.title("📑 Pecah Data Laporan Glints")
    st.markdown("""
    Tools ini memproses file **CSV/Excel hasil export dari Glints**,  
    kemudian memecah datanya berdasarkan jumlah baris maksimum yang kamu tentukan.
    """)

    uploaded_file = st.file_uploader("📂 Upload file CSV atau Excel", type=["csv", "xlsx"])
    max_rows = st.number_input("🔢 Jumlah maksimum baris per file", min_value=100, max_value=1000, value=500, step=50)

    if uploaded_file is not None:
        with st.spinner("🔧 Memproses dan memecah data..."):
            try:
                output_zip, df_summary = pecah_glints.process_glints_file(uploaded_file, max_rows)
                st.success("✅ Pemrosesan selesai!")

                st.subheader("📊 Statistik Hasil")
                st.write(df_summary)

                st.subheader("📦 Download Hasil")
                with open(output_zip, "rb") as f:
                    st.download_button(
                        label="📥 Download Semua File (ZIP)",
                        data=f,
                        file_name=os.path.basename(output_zip),
                        mime="application/zip"
                    )

            except Exception as e:
                st.error(f"❌ Gagal memproses file: {e}")

# ======================================================
# 4️⃣ MENU: Placeholder Tools
# ======================================================
elif menu == "🧩 Placeholder Tools (Coming Soon)":
    st.title("🧩 Tools Lainnya")
    st.info("🚧 Modul ini masih dalam pengembangan.")
    st.markdown("""
    Nantikan pembaruan berikutnya dengan fitur-fitur menarik lainnya!
    """)