# excel-sales-consolidator

**EN:** Second project in a three-part modular portfolio for Excel automation with Python. Consolidates multiple Excel source files with inconsistent formats into a single clean dataset.

**ES:** Segundo proyecto de un portfolio modular de tres partes para automatización de Excel con Python. Consolida múltiples archivos Excel con formatos inconsistentes en un único dataset limpio.

---

## What it does / Qué hace

**EN:** Ingests, normalizes, and consolidates 10 Excel files from different sources into one structured dataset. Normalization includes:
- Standardize column names across all files
- Align schema — map 53 column name variants to 12 standard columns
- Handle duplicated semantic columns via `combine_first()`
- Ensure consistent data types where possible
- Add `source_file` column for row-level traceability

**ES:** Ingesta, normaliza y consolida 10 archivos Excel de distintas fuentes en un único dataset estructurado. La normalización incluye:
- Estandarizar nombres de columna en todos los archivos
- Alinear el esquema — mapear 53 variantes de nombres a 12 columnas estándar
- Gestionar columnas semánticamente duplicadas con `combine_first()`
- Asegurar tipos de datos consistentes donde sea posible
- Añadir columna `source_file` para trazabilidad a nivel de fila

---

## Input / Output

**Input:**
- 10 Excel files with heterogeneous schemas (`data/raw/`)

**Output:**
- 1 consolidated Excel file (`outputs/consolidated.xlsx`)

### Example / Ejemplo

| source_file | id_venta | cliente | fecha_venta | producto | importe |
|-------------|----------|---------|-------------|----------|---------|
| source_01.xlsx | 1102 | Muebles Pérez Polo | 02/01/2024 | Viga | 900 |
| source_02.xlsx | 1255 | Construcciones Norte | 03-02-2024 | Viga laminada | NaN |
| source_03.xlsx | 1276 | NaN | 03/02/2024 | CLT | 480 |

---

## Key Features / Características principales

- Designed for real-world messy Excel inputs
- Multi-file Excel ingestion
- Schema normalization across heterogeneous sources
- Robust consolidation with duplicate detection (no removal at this stage)
- `source_file` traceability column per row
- Reproducible pipeline structure with modular scripts

---

## Pipeline architecture / Arquitectura del pipeline

**EN:** This project is the first stage of a planned end-to-end pipeline:

**ES:** Este proyecto es la primera etapa de un pipeline end-to-end planificado:

```
[excel-sales-consolidator]  →  [excel-data-cleaner]  →  [excel-report-formatter]
   Ingest + Normalize              Deep cleaning              Visual formatting
   Consolidate → .xlsx             Standardize values         Professional output
```

```python
lista_dfs            = load_excels(file_paths)               # ingestion.py
lista_dfs_normalized = normalize_dfs(lista_dfs)              # normalization.py
df_consolidated      = consolidate_excels(lista_dfs_normalized)  # consolidation.py
```

---

## A note on build order / Nota sobre el orden de construcción

**EN:** Projects were built in learning order, not pipeline order. `excel-data-cleaner` was built first as an introduction to pandas, using a specific known dataset. It needs to be refactored to be dataset-agnostic before connecting to this consolidator. That refactoring is planned as part of the unified pipeline project.

**ES:** Los proyectos se construyeron en orden didáctico, no de pipeline. `excel-data-cleaner` se construyó primero como introducción a pandas, con un dataset concreto. Necesita refactorizarse para ser agnóstico al dataset antes de conectarse a este consolidador. Esa refactorización está planificada como parte del pipeline unificado.

---

## Design decisions / Decisiones de diseño

- **Modular architecture** — separation of concerns across `ingestion.py`, `normalization.py`, `consolidation.py`
- **Normalization before consolidation** — ensures identical schema before `pd.concat()`
- **Sampling-based test data generation** — `generate_source_excels.py` produces reproducible dirty files from a canonical dataset
- **Notebooks first, scripts second** — logic explored in Jupyter before being extracted to reusable modules

---

## Limitations / Limitaciones

- Assumes similar underlying schema across all source files
- Does not perform deep data cleaning — handled by `excel-data-cleaner` in the pipeline
- Duplicate rows are detected but not removed at this stage

---

## Project structure / Estructura del proyecto

```
excel-sales-consolidator/
├── data/
│   ├── raw/                    # 10 generated source Excel files
│   └── target/                 # Canonical reference dataset
├── notebooks/
│   ├── 01_data_ingestion.ipynb
│   ├── 02_normalization.ipynb
│   └── 03_consolidation.ipynb
├── outputs/
│   └── consolidated.xlsx
├── src/
│   ├── __init__.py
│   ├── generate_source_excels.py
│   ├── ingestion.py
│   ├── normalization.py
│   └── consolidation.py
├── .gitignore
├── requirements.txt
└── README.md
```

---

## How to run / Cómo ejecutarlo

```powershell
# Clone / Clonar
git clone https://github.com/tu-usuario/excel-sales-consolidator.git
cd excel-sales-consolidator

# Create environment / Crear entorno
conda create -p ./env python=3.11
conda activate ./env

# Install dependencies / Instalar dependencias
pip install -r requirements.txt

# Generate source files / Generar archivos fuente
python src/generate_source_excels.py --input data/target/dirty_sales_data.xlsx --output-dir data/raw

# Run the full pipeline / Ejecutar el pipeline completo
python -m src.consolidation
```

---

## Tech stack

| Tool | Purpose |
|------|---------|
| Python 3.11 | Core language |
| pandas | Data ingestion, normalization, consolidation |
| openpyxl | Excel export |
| xlsxwriter | Excel source file generation |
| Jupyter | Exploratory notebooks |

---

## Part of / Parte de

**Modular Excel automation workflows for cleaning, consolidating, and formatting business reports.**

| Project | Role in pipeline |
|---------|-----------------|
| `excel-sales-consolidator` | ① Ingest + Normalize + Consolidate |
| `excel-data-cleaner` | ② Deep cleaning |
| `excel-report-formatter` | ③ Visual formatting |

---

## Why this name / Por qué este nombre

**EN:** The name reflects the domain (sales) and the core operation (consolidation). Each project has a single responsibility — this one consolidates, it does not clean or format.

**ES:** El nombre refleja el dominio (ventas) y la operación principal (consolidación). Cada proyecto tiene una única responsabilidad — este consolida, no limpia ni formatea.
