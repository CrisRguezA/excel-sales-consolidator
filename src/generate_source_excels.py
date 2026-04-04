"""
generate_source_excels.py

EN:
Generates 10 Excel source files from a canonical dataset.
Each file simulates a different real-world source (department, region, salesperson)
with inconsistent formats: column names, date formats, numeric formats,
missing values, duplicated columns, and extra noise rows.

ES:
Genera 10 archivos Excel fuente a partir de un dataset canónico.
Cada archivo simula una fuente real distinta (departamento, región, comercial)
con formatos inconsistentes: nombres de columnas, formatos de fecha, formatos
numéricos, valores nulos, columnas duplicadas y filas de ruido.

Usage / Uso:
    python generate_source_excels.py --input dirty_sales_data.xlsx --output-dir data/
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path

import numpy as np
import pandas as pd


# =============================================================================
# CONSTANTS / CONSTANTES
# EN: Canonical column names as they appear in the source file.
# ES: Nombres canónicos de columnas tal como aparecen en el archivo fuente.
# =============================================================================

CANONICAL_COLUMNS = [
    " ID Venta ",
    "cliente",
    "Cliente",
    "fecha venta",
    "Fecha_Venta",
    "producto ",
    "tipo_madera",
    "certificacion",
    "cantidad_m3 ",
    "precio_m3",
    "importe ",
    "estado",
    "comercial",
    "pais",
]

# EN: Possible column name variants per canonical column (simulates human inconsistency).
# ES: Variantes posibles de nombre por columna canónica (simula inconsistencia humana).
COLUMN_VARIANTS = {
    " ID Venta ":    [" ID Venta ", "id_venta", "ID_VENTA", "Id venta", "Venta ID"],
    "cliente":       ["cliente", "Cliente", "NOMBRE CLIENTE", "razon_social"],
    "Cliente":       ["Cliente", "CLIENTE", "cliente_alt", "cliente_2"],
    "fecha venta":   ["fecha venta", "fecha_venta", "Fecha Venta", "fecha", "fecha operación"],
    "Fecha_Venta":   ["Fecha_Venta", "FECHA_VENTA", "fecha_venta_alt"],
    "producto ":     ["producto ", "producto", "Producto", "tipo_producto", "articulo"],
    "tipo_madera":   ["tipo_madera", "tipo madera", "TIPO_MADERA", "madera"],
    "certificacion": ["certificacion", "certificación", "certif", "sello"],
    "cantidad_m3 ":  ["cantidad_m3 ", "cantidad_m3", "Cantidad m3", "m3", "volumen_m3"],
    "precio_m3":     ["precio_m3", "precio m3", "Precio_m3", "€/m3"],
    "importe ":      ["importe ", "importe", "Importe", "total", "importe_total"],
    "estado":        ["estado", "Estado", "status", "situacion"],
    "comercial":     ["comercial", "vendedor", "responsable_comercial", "sales_rep"],
    "pais":          ["pais", "País", "country", "mercado"],
}

# EN: Value variants per field to simulate real-world inconsistency.
# ES: Variantes de valores por campo para simular inconsistencia real.
STATUS_VARIANTS = {
    "Cerrada":   ["Cerrada", "cerrada", "ok", "CERRADA", "Completada"],
    "Pendiente": ["Pendiente", "pendiente", "en curso", "PTE", "Open"],
    "Cancelada": ["Cancelada", "cancelada", "CANCEL", "Anulada"],
}

COUNTRY_VARIANTS = {
    "España":   ["España", "ES", "Spain", "espana"],
    "Portugal": ["Portugal", "PT", "portugal"],
    "Francia":  ["Francia", "FR", "France"],
    "Madrid":   ["Madrid"],  # EN: intentionally wrong value / ES: valor incorrecto intencionado
}

PRODUCT_VARIANTS = {
    "Tablón Roble": ["Tablón Roble", "TABLON ROBLE", "tablón roble"],
    "Viga Pino":    ["Viga Pino", "viga pino", "VIGA PINO"],
    "Panel MDF":    ["Panel MDF", "panel mdf", "PANEL MDF"],
    "Chapa Nogal":  ["Chapa Nogal", "chapa nogal", "CHAPA NOGAL"],
}

WOOD_VARIANTS = {
    "Roble": ["Roble", "roble", "ROBLE"],
    "Pino":  ["Pino", "pino", "PINO"],
    "Nogal": ["Nogal", "nogal", "NOGAL"],
    "MDF":   ["MDF", "mdf"],
}

# EN: Row counts per file — variable sizes simulate real-world data sources.
# ES: Número de filas por archivo — tamaños variables simulan fuentes reales.
FILE_ROW_COUNTS = [28, 45, 31, 52, 38, 24, 41, 36, 29, 47]


# =============================================================================
# ARGUMENT PARSING / ARGUMENTOS
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="EN: Generate 10 dirty Excel files. / ES: Genera 10 archivos Excel sucios."
    )
    parser.add_argument("--input",      required=True, help="EN: Canonical Excel path / ES: Ruta Excel canónico")
    parser.add_argument("--output-dir", default="data", help="EN: Output folder / ES: Carpeta de salida")
    parser.add_argument("--seed",       type=int, default=42, help="EN: Random seed / ES: Semilla aleatoria")
    return parser.parse_args()


# =============================================================================
# CHUNK GENERATION / GENERACIÓN DE CHUNKS
# =============================================================================

def generate_variable_chunks(df: pd.DataFrame, seed: int) -> list[pd.DataFrame]:
    """
    EN:
    Creates 10 DataFrames with variable row counts using random sampling with replacement.
    This simulates files sent by different teams with different volumes of data.

    ES:
    Crea 10 DataFrames con número de filas variable usando muestreo con reemplazo.
    Simula archivos enviados por distintos equipos con distintos volúmenes de datos.
    """
    chunks = []
    for i, size in enumerate(FILE_ROW_COUNTS):
        chunk = df.sample(n=size, replace=True, random_state=seed + i)
        chunks.append(chunk.reset_index(drop=True))
    return chunks


# =============================================================================
# NOISE FUNCTIONS / FUNCIONES DE RUIDO
# =============================================================================

def maybe_pick(mapping: dict, value: object, rng: random.Random) -> object:
    """
    EN: Returns a random variant of a value if it exists in the mapping.
    ES: Devuelve una variante aleatoria del valor si existe en el mapping.
    """
    if pd.isna(value):
        return value
    value = str(value)
    return rng.choice(mapping[value]) if value in mapping else value


def randomize_text_fields(df: pd.DataFrame, rng: random.Random) -> pd.DataFrame:
    """
    EN: Applies value-level inconsistencies to text columns (status, country, product, wood type).
    ES: Aplica inconsistencias a nivel de valor en columnas de texto (estado, país, producto, madera).
    """
    out = df.copy()

    for col, mapping in [
        ("estado",      STATUS_VARIANTS),
        ("pais",        COUNTRY_VARIANTS),
        ("producto ",   PRODUCT_VARIANTS),
        ("tipo_madera", WOOD_VARIANTS),
    ]:
        if col in out.columns:
            out[col] = out[col].map(lambda x: maybe_pick(mapping, x, rng))

    # EN: Randomly uppercase some client names / ES: Pone en mayúsculas algunos nombres de cliente
    if "cliente" in out.columns:
        mask = out["cliente"].notna()
        idx = out[mask].sample(frac=0.12, random_state=rng.randint(1, 99999)).index
        out.loc[idx, "cliente"] = out.loc[idx, "cliente"].astype(str).str.upper()

    # EN: Populate duplicate Cliente column in some rows, clear the main column in others.
    # ES: Rellena la columna duplicada Cliente en algunas filas y vacía la principal en otras.
    if "Cliente" in out.columns and "cliente" in out.columns:
        idx = out[out["cliente"].notna()].sample(frac=0.20, random_state=rng.randint(1, 99999)).index
        out.loc[idx, "Cliente"] = out.loc[idx, "cliente"]
        empty_idx = out.loc[idx].sample(frac=0.35, random_state=rng.randint(1, 99999)).index
        out.loc[empty_idx, "cliente"] = np.nan

    return out


def randomize_dates(df: pd.DataFrame, rng: random.Random, variant_id: int) -> pd.DataFrame:
    """
    EN: Applies a different date format per file. Some rows go into the duplicate date column.
    ES: Aplica un formato de fecha distinto por archivo. Algunas filas van a la columna duplicada.
    """
    out = df.copy()
    if "fecha venta" not in out.columns:
        return out

    dt = pd.to_datetime(out["fecha venta"], errors="coerce")
    date_formats = {0: "%Y-%m-%d", 1: "%d/%m/%Y", 2: "%d-%m-%Y", 3: "%m/%d/%Y", 4: "%d.%m.%Y"}
    fmt = date_formats[variant_id % len(date_formats)]
    out["fecha venta"] = dt.dt.strftime(fmt)

    if "Fecha_Venta" in out.columns:
        idx = out.sample(frac=0.28, random_state=rng.randint(1, 99999)).index
        out.loc[idx, "Fecha_Venta"] = dt.loc[idx].dt.strftime("%Y/%m/%d")
        clear_idx = out.loc[idx].sample(frac=0.30, random_state=rng.randint(1, 99999)).index
        out.loc[clear_idx, "fecha venta"] = np.nan

    return out


def randomize_numbers(df: pd.DataFrame, rng: random.Random, variant_id: int) -> pd.DataFrame:
    """
    EN: Applies numeric format inconsistencies: decimal separators, currency symbols, nulls.
    ES: Aplica inconsistencias numéricas: separadores decimales, símbolos de moneda, nulos.
    """
    out = df.copy()

    # EN: cantidad_m3 — three possible formats / ES: cantidad_m3 — tres formatos posibles
    if "cantidad_m3 " in out.columns:
        s = pd.to_numeric(out["cantidad_m3 "], errors="coerce")
        if variant_id % 3 == 0:
            out["cantidad_m3 "] = s.map(lambda x: f"{x:.2f}" if pd.notna(x) else x)
        elif variant_id % 3 == 1:
            out["cantidad_m3 "] = s.map(
                lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notna(x) else x
            )
        else:
            out["cantidad_m3 "] = s.round(2)

    # EN: precio_m3 — alternates between two currency formats / ES: alterna entre dos formatos de moneda
    if "precio_m3" in out.columns:
        s = pd.to_numeric(out["precio_m3"], errors="coerce")
        out["precio_m3"] = s.map(
            lambda x: (f"{x:.2f} €" if variant_id % 2 == 0 else f"EUR {x:.2f}") if pd.notna(x) else x
        )

    # EN: importe — mix of text with currency and nulls / ES: mezcla texto con moneda y nulos
    if "importe " in out.columns:
        s = pd.to_numeric(out["importe "], errors="coerce")
        # EN: Convert to object dtype first to allow mixed types (numbers + strings)
        # ES: Convertir a object primero para permitir tipos mixtos (números + strings)
        out["importe "] = out["importe "].astype(object)
        idx_text = out.sample(frac=0.35, random_state=rng.randint(1, 99999)).index
        out.loc[idx_text, "importe "] = s.loc[idx_text].map(
            lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " €" if pd.notna(x) else x
        )
        idx_null = out.sample(frac=0.10, random_state=rng.randint(1, 99999)).index
        out.loc[idx_null, "importe "] = np.nan

    return out


def add_noise_rows(df: pd.DataFrame, rng: random.Random) -> pd.DataFrame:
    """
    EN: Adds 2 empty rows and 1 duplicate row to simulate real-world file noise.
    ES: Añade 2 filas vacías y 1 fila duplicada para simular ruido de archivos reales.
    """
    out = df.copy()
    empty_rows = pd.DataFrame([{c: np.nan for c in out.columns} for _ in range(2)])
    dup_row = out.sample(n=1, random_state=rng.randint(1, 99999))
    return pd.concat([out, empty_rows, dup_row], ignore_index=True)


def rename_and_reorder_columns(df: pd.DataFrame, rng: random.Random) -> pd.DataFrame:
    """
    EN: Renames columns using random variants and shuffles column order.
    ES: Renombra columnas con variantes aleatorias y desordena el orden de columnas.
    """
    rename_map = {col: rng.choice(COLUMN_VARIANTS.get(col, [col])) for col in df.columns}
    out = df.rename(columns=rename_map)
    cols = list(out.columns)
    rng.shuffle(cols)
    return out[cols]


def maybe_drop_columns(df: pd.DataFrame, rng: random.Random, variant_id: int) -> pd.DataFrame:
    """
    EN: Drops auxiliary/duplicate columns in some files to simulate incomplete sources.
    ES: Elimina columnas auxiliares/duplicadas en algunos archivos para simular fuentes incompletas.
    """
    out = df.copy()
    drop_candidates = []

    if variant_id % 2 == 0:
        drop_candidates.append(next((c for c in out.columns if "cliente" in c.lower() and c != "cliente"), None))
    if variant_id % 3 == 0:
        drop_candidates.append(next((c for c in out.columns if "fecha" in c.lower() and c != "fecha venta"), None))

    for col in drop_candidates:
        if col and col in out.columns:
            out = out.drop(columns=[col])

    return out


# =============================================================================
# EXCEL EXPORT / EXPORTACIÓN EXCEL
# =============================================================================

def write_excel(df: pd.DataFrame, output_path: Path, source_name: str) -> None:
    """
    EN: Writes the DataFrame to Excel with basic formatting and a metadata sheet.
    ES: Escribe el DataFrame en Excel con formato básico y una hoja de metadatos.
    """
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="ventas", index=False)

        wb  = writer.book
        ws  = writer.sheets["ventas"]

        # EN: Define formats / ES: Define formatos
        fmt_header = wb.add_format({"bold": True, "text_wrap": True, "valign": "top", "border": 1})
        fmt_text   = wb.add_format({"border": 1})
        fmt_number = wb.add_format({"border": 1, "num_format": "#,##0.00"})
        fmt_date   = wb.add_format({"border": 1, "num_format": "dd/mm/yyyy"})

        # EN: Write headers with format / ES: Escribe encabezados con formato
        for col_num, col_name in enumerate(df.columns):
            ws.write(0, col_num, col_name, fmt_header)
            width = min(max(len(str(col_name)) + 2, 14), 24)
            ws.set_column(col_num, col_num, width)

        # EN: Apply column format by name / ES: Aplica formato de columna por nombre
        for idx, col in enumerate(df.columns):
            lower = str(col).lower()
            if "fecha" in lower:
                ws.set_column(idx, idx, 15, fmt_date)
            elif any(t in lower for t in ["importe", "precio", "m3", "cantidad", "total"]):
                ws.set_column(idx, idx, 16, fmt_number)
            else:
                ws.set_column(idx, idx, 20, fmt_text)

        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, max(len(df), 1), max(len(df.columns) - 1, 0))

        # EN: Metadata sheet / ES: Hoja de metadatos
        meta = pd.DataFrame({
            "field": ["source_name", "rows_generated", "comment"],
            "value": [
                source_name,
                len(df),
                "EN: Simulated file with heterogeneous formats. / ES: Archivo simulado con formatos heterogéneos.",
            ],
        })
        meta.to_excel(writer, sheet_name="metadata", index=False)


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    args = parse_args()
    rng  = random.Random(args.seed)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # EN: Load canonical file and keep only known columns.
    # ES: Carga el archivo canónico y conserva solo las columnas conocidas.
    df = pd.read_excel(input_path)
    existing_cols = [c for c in CANONICAL_COLUMNS if c in df.columns]
    df = df[existing_cols].copy()

    # EN: Generate 10 variable-size chunks via sampling with replacement.
    # ES: Genera 10 chunks de tamaño variable mediante muestreo con reemplazo.
    chunks = generate_variable_chunks(df, args.seed)

    for i, chunk in enumerate(chunks, start=1):
        out = chunk.copy()
        out = randomize_text_fields(out, rng)
        out = randomize_dates(out, rng, variant_id=i)
        out = randomize_numbers(out, rng, variant_id=i)
        out = add_noise_rows(out, rng)
        out = rename_and_reorder_columns(out, rng)
        out = maybe_drop_columns(out, rng, variant_id=i)

        output_path = output_dir / f"source_{i:02d}.xlsx"
        write_excel(out, output_path, source_name=output_path.name)
        print(f"  ✓ source_{i:02d}.xlsx — {len(out)} filas / rows")

    print(f"\nOK — 10 archivos generados en / files generated in: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
