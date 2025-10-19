# Transjakarta-ETL
ETL pipeline for Transjakarta using Airflow and Google Cloud Platform (BigQuery, GCS)

Berikut adalah Airflow Script untuk melakukan proses ETL 
dari data source yang saya simpan di GCS lalu saya olah di Airflow untuk disimpan kembali di BigQuery

saya tidak menggunakan PostgreSQL karena:
1. saya menggunakan laptop kantor untuk mengerjakan task ini
2. laptop kantor diproteksi untuk instalasi sulit, harus melalui IT Admin dan kebetulan tidak ada PostgreSQL local
3. Laptop kantor juga punya spek yang tanggung, maka saya pakai Docker dan VM di Compute Engine GCP pribadi
