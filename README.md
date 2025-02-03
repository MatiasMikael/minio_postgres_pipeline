# MinIO & PostgreSQL Data Pipeline

## Overview
Tämä projekti on ETL-pipeline, joka hakee urheiludataa The Sports DB API:sta, tallentaa sen MinIO-tiedostovarastoon ja PostgreSQL-tietokantaan. Datalle suoritetaan SQL-kyselyitä analysointia varten.

### **Pipeline-vaiheet:**
1. **Extract:** Haetaan data The Sports DB API:sta (`fetch_data.py`).
2. **Load:** Tallennetaan data MinIO:hon (`sports_data.json`) ja PostgreSQL-tietokantaan (`load_data.py`).
3. **Transform:** Suoritetaan SQL-kyselyitä tietokannassa.

---

## Tools & Libraries
### **Käytetyt teknologiat:**
- **Python** – Datankäsittely ja API-yhteydet
- **MinIO** – S3-yhteensopiva tiedostovarasto
- **PostgreSQL** – Relaatiotietokanta
- **Tableau Public (valinnainen)** – Visualisointiin
- **ChatGPT** – Suunnitteluun ja toteutukseen

### **Python-kirjastot:**
Asenna tarvittavat kirjastot komennolla:
```bash
pip install -r requirements.txt
```

**Kirjastot:**
- `requests` – API-kutsut
- `minio` – MinIO-yhteys
- `psycopg2-binary` – PostgreSQL-yhteys
- `sqlalchemy` – SQL-käsittely
- `pandas` – Datankäsittely
- `python-dotenv` – Ympäristömuuttujien hallinta
- `logging` – Lokitiedostojen hallinta

---

## MIT License
Tätä ohjelmistoa saa käyttää vapaasti, kunhan alkuperäinen tekijä mainitaan.
