"""
normalization.py

EN:
Normalizes a list of raw DataFrames by mapping all observed column name
variants to 12 standard column names. Combines duplicate/variant columns
using combine_first() to minimize data loss. Adds a source_file column
for traceability. Returns a list of normalized DataFrames with identical
column structure, ready for consolidation.

Pipeline contract:
    normalize_dfs(lista_dfs: list[pd.DataFrame]) -> list[pd.DataFrame]
    Each returned DataFrame has:
      - 12 standard columns in consistent order
      - source_file column for row-level traceability
      - All values kept as strings (no type conversion)

ES:
Normaliza una lista de DataFrames crudos mapeando todas las variantes
observadas de nombres de columna a 12 nombres de columna estándar. Combina
columnas duplicadas o variantes usando combine_first() para minimizar la
pérdida de datos. Añade una columna source_file para trazabilidad. Devuelve
una lista de DataFrames normalizados con estructura de columnas idéntica,
listos para la consolidación.

Contrato del pipeline:
    normalize_dfs(lista_dfs: list[pd.DataFrame]) -> list[pd.DataFrame]
    Cada DataFrame devuelto tiene:
      - 12 columnas estándar en orden consistente
      - Columna source_file para trazabilidad a nivel de fila
      - Todos los valores mantenidos como strings (sin conversión de tipo)
"""

# ------------------------------------------------------------------
# IMPORTS
# ------------------------------------------------------------------

import pandas as pd
from pathlib import Path
from src.ingestion import load_excels


# ------------------------------------------------------------------
# COLUMN MAP / MAPA DE COLUMNAS
# ------------------------------------------------------------------

# Maps each standard column name to all observed variants across source files
# Construido a partir del inventario completo de 53 variantes únicas
COLUMN_MAP = {
    "id_venta":      ["Venta ID", "ID_VENTA", "ID Venta", "Id venta"],
    "fecha_venta":   ["fecha venta", "fecha_venta_alt", "Fecha_Venta", "fecha operación",
                      "fecha", "fecha_venta", "FECHA_VENTA", "Fecha Venta"],
    "producto":      ["Producto", "tipo_producto", "articulo", "producto"],
    "tipo_madera":   ["TIPO_MADERA", "tipo_madera", "madera", "tipo madera"],
    "certificacion": ["certif", "certificacion", "certificación"],
    "cliente":       ["CLIENTE", "cliente", "Cliente", "cliente_2", "cliente_alt",
                      "NOMBRE CLIENTE", "razon_social"],
    "cantidad_m3":   ["Cantidad m3", "cantidad_m3", "m3", "volumen_m3"],
    "precio_m3":     ["€/m3", "precio m3", "precio_m3", "Precio_m3"],
    "importe":       ["importe", "importe_total", "total"],
    "estado":        ["Estado", "estado", "situacion", "status"],
    "comercial":     ["comercial", "responsable_comercial", "sales_rep", "vendedor"],
    "pais":          ["country", "mercado", "pais", "País"],
}


# ------------------------------------------------------------------
# CORE FUNCTIONS / FUNCIONES PRINCIPALES
# ------------------------------------------------------------------

def normalize_df(df: pd.DataFrame, column_map: dict, source_name: str) -> pd.DataFrame:
    """
    EN: Maps raw column variants to standard names for a single DataFrame.
    ES: Mapea variantes de columnas al nombre estándar para un único DataFrame.

    Args:
        df          : raw DataFrame from a single source file
        column_map  : dict mapping standard column names to lists of known variants
        source_name : filename string added as traceability column

    Returns:
        pd.DataFrame with exactly the standard columns + source_file, in consistent order
    """
    out = pd.DataFrame()

    for standard_col, variants in column_map.items():

        # Find which variants of this standard column exist in the current df
        found = [v for v in variants if v in df.columns]

        if not found:
            # No variant found — fill with NA to keep consistent structure
            out[standard_col] = pd.NA
        elif len(found) == 1:
            # Single variant — direct assignment
            out[standard_col] = df[found[0]]
        else:
            # Multiple variants — combine prioritizing first non-null value per row
            combined = df[found[0]]
            for col in found[1:]:
                combined = combined.combine_first(df[col])
            out[standard_col] = combined

    # Add traceability column to track which source file each row came from
    out["source_file"] = source_name

    return out.reset_index(drop=True)


def normalize_dfs(lista_dfs: list, column_map: dict = COLUMN_MAP) -> list:
    """
    EN: Applies normalize_df() to a list of raw DataFrames.
    ES: Aplica normalize_df() a una lista de DataFrames crudos.

    Args:
        lista_dfs  : list of raw DataFrames — one per source file
        column_map : mapping dict (defaults to COLUMN_MAP defined in this module)

    Returns:
        list of normalized pd.DataFrames with identical column structure
    """
    lista_dfs_normalized = []

    for i, df in enumerate(lista_dfs, start=1):
        source_name = f"source_{i:02d}.xlsx"
        df_norm = normalize_df(df, column_map, source_name)
        lista_dfs_normalized.append(df_norm)
        print(f"  ✓ {source_name} — {df_norm.shape[0]} rows × {df_norm.shape[1]} columns")

    print(f"\n{len(lista_dfs_normalized)} DataFrames normalized successfully")
    return lista_dfs_normalized


# ------------------------------------------------------------------
# STANDALONE EXECUTION / EJECUCIÓN INDEPENDIENTE
# ------------------------------------------------------------------

if __name__ == "__main__":
    # Resolve project root — works whether script runs from /src or project root
    ROOT = Path(__file__).resolve().parent.parent
    DATA_DIR = ROOT / "data" / "raw"

    file_paths = sorted(DATA_DIR.glob("source_*.xlsx"))

    if not file_paths:
        raise FileNotFoundError(f"No source files found in: {DATA_DIR}")

    print(f"Loading {len(file_paths)} files from: {DATA_DIR}\n")
    lista_dfs = load_excels(file_paths)

    print("\nNormalizing DataFrames...\n")
    lista_dfs_normalized = normalize_dfs(lista_dfs)