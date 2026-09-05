# crosscheck-db

Tool rekonsiliasi data lintas database berkecepatan tinggi. 

Dibuat untuk memverifikasi jutaan data transaksi antara database operasional (OLTP seperti PostgreSQL) dan data warehouse (OLAP seperti BigQuery/Snowflake) secara presisi, tanpa bikin server produksi jebol atau OOM.

---

### Masalah Nyata yang Dipecahkan

Di sistem retail, fintech, dan logistik, data transaksi tersebar di banyak tempat:
1. **PostgreSQL** (catatan transaksi kasir/aplikasi real-time).
2. **BigQuery / Snowflake** (gudang data buat laporan keuangan & audit).

Waktu pipeline ETL atau CDC berjalan, transaksi bisa hilang diam-diam (*silent data drift*) akibat network glitch, race condition, atau schema mismatch. 

* **Kalau dicari manual:** Tim audit butuh berminggu-minggu buat mencocokkan jutaan baris di spreadsheet.
* **Kalau pakai query `SELECT *`:** Menarik jutaan baris mentah dari server produksi bakal bikin database crash dan membakar kuota egress jaringan.

---

### Cara Kerjanya

CrossCheck menggunakan teknik **Push-Down Checksum Hashing** dan **Hierarchical Bucket Verification**:

1. **Partisi ke dalam Bucket**: Baris data dibagi secara logis berdasarkan rentang primary key (misal per 2.500 atau 5.000 ID).
2. **Hitung Hash Lokal di Database (Push-Down)**: Database sumber dan target menghitung checksum lokal memakai fungsi bawaan (`MD5`, `CONCAT_WS`, dan `STRING_AGG`/`GROUP_CONCAT`).
3. **Bandingkan Hash Antar-Bucket ($O(\text{buckets})$)**: Tool hanya membandingkan ringkasan hash bucket. Jika sebuah bucket bernilai identik, ribuan baris di dalamnya terverifikasi 100% konsisten dalam hitungan milidetik tanpa kirim data mentah.
4. **Drill-Down kalau Ada Selisih**: Hanya bucket yang hash-nya beda yang ditarik baris per baris untuk menemukan persis baris ID mana yang hilang, bertambah (*phantom write*), atau beda nominalnya.

---

### Bukti Eksekusi & Quickstart

Clone dan langsung jalankan simulasi tanpa perlu koneksi cloud (menggunakan SQLite adapter bawaan):

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Jalankan simulasi audit 50.000 transaksi
python demo.py
```

#### Snapshot Hasil Eksekusi Terminal:
![CrossCheck Terminal Demo](terminal_demo.svg)

---

### Hasil Deteksi & Benchmark

Dari pengujian 50.000 transaksi dengan 3 data drift yang sengaja disisipkan:

| Key ID | Kolom | Nilai di Source | Nilai di Target | Status Masalah |
|---|---|---|---|---|
| `12450` | *Seluruh Baris* | Ada (Rp50.000) | **Hilang** | Dropped record saat ETL sync |
| `50001` | *Seluruh Baris* | **Tidak Ada** | Ada | Phantom write / duplikasi di target |
| `41820` | `amount` | `220940.0` | `85001.0` | Selisih nilai pembulatan desimal |

* **Performa Audit:** `~215.000 baris/detik`
* **Waktu Eksekusi:** `0.23 detik` untuk memverifikasi 50.000 baris data dan menemukan 3 titik selisih secara akurat.

---

### Menjalankan Unit Tests

```bash
python -m unittest tests/test_reconciliation.py
```

---

### Cara Pakai CLI

#### 1. PostgreSQL ke BigQuery
```bash
python -m crosscheck.cli \
  --source-driver postgres --source-conn "postgresql://user:pass@host:5432/finance" \
  --target-driver bigquery --target-conn "my-gcp-project/analytics_dw" \
  --table transactions \
  --key id \
  --columns "account_id,amount,currency,status" \
  --bucket-size 5000 \
  --where "created_at >= '2026-01-01'"
```

#### 2. Antar-File SQLite Lokal
```bash
python -m crosscheck.cli \
  --source-driver sqlite --source-conn prod.db \
  --target-driver sqlite --target-conn warehouse.db \
  --table transactions \
  --key id \
  --columns "account_id,amount,status" \
  --bucket-size 2500
```

---

### Struktur Project

```
crosscheckdb/
├── crosscheck/
│   ├── config.py                  # Konfigurasi tabel & driver
│   ├── cli.py                     # Interface terminal CLI (Rich formatting)
│   ├── adapters/
│   │   ├── base.py                # Kontrak adapter database
│   │   ├── sqlite.py              # SQLite adapter (buat tes lokal)
│   │   ├── postgres.py            # PostgreSQL adapter (push-down md5/string_agg)
│   │   └── bigquery.py            # BigQuery adapter (DIV/TO_HEX/MD5)
│   └── engine/
│       ├── bucket.py              # Logika rentang bucket
│       └── reconciler.py          # Divide-and-conquer reconciliation engine
├── tests/
│   └── test_reconciliation.py     # Unit test otomatis
├── terminal_demo.svg              # Snapshot visual eksekusi terminal (root level)
├── demo.py                        # Script simulasi data drift
└── requirements.txt               # Dependensi minimal
```
