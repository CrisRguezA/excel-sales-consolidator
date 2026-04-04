"""
consolidation.py

EN:
Consolidates multiple normalized DataFrames into a single Excel file.
Imports load_excels() from ingestion.py and normalize_dfs() from
normalization.py. Concatenates all normalized DataFrames into one and
exports the result to outputs/consolidated.xlsx.
Duplicate detection is included as a diagnostic step only — deep cleaning
is delegated to the excel-data-cleaner module in the full pipeline.

Pipeline contract:
    consolidate_excels(lista_dfs_normalized: list[pd.DataFrame]) -> pd.DataFrame
    The returned DataFrame has:
      - 12 standard columns + source_file in consistent order
      - All rows from all source files
      - No rows removed (duplicates flagged but not dropped)

ES:
Consolida múltiples DataFrames normalizados en un único archivo Excel.
Importa load_excels() de ingestion.py y normalize_dfs() de normalization.py.
Concatena todos los DataFrames normalizados en uno y exporta el resultado
a outputs/consolidated.xlsx.
La detección de duplicados se incluye solo como paso de diagnóstico — la
limpieza profunda se delega al módulo excel-data-cleaner en el pipeline completo.

Contrato del pipeline:
    consolidate_excels(lista_dfs_normalized: list[pd.DataFrame]) -> pd.DataFrame
    El DataFrame devuelto tiene:
      - 12 columnas estándar + source_file en orden consistente
      - Todas las filas de todos los archivos fuente
      - Sin filas eliminadas (duplicados detectados pero no eliminados)
"""

# ------------------------------------------------------------------
# IMPORTS
# ------------------------------------------------------------------

import pandas as pd
from pathlib import Path
from src.ingestion import load_excels
from src.normalization import normalize_dfs


# ------------------------------------------------------------------
# CORE FUNCTION / FUNCIÓN PRINCIPAL
# ------------------------------------------------------------------

def consolidate_excels(lista_dfs_normalized: list) -> pd.DataFrame:
    """
    EN: Concatenates a list of normalized DataFrames into a single DataFrame.
    ES: Concatena una lista de DataFrames normalizados en un único DataFrame.

    Args:
        lista_dfs_normalized : list of normalized DataFrames with identical structure

    Returns:
        pd.DataFrame — single consolidated DataFrame with sequential index
    """
    df_consolidated = pd.concat(lista_dfs_normalized, ignore_index=True)

    # Diagnostic — flag duplicates but do not remove them
    n_duplicates = df_consolidated.duplicated().sum()

    print(f"  Shape     : {df_consolidated.shape[0]} rows × {df_consolidated.shape[1]} columns")
    print(f"  Duplicates: {n_duplicates} rows detected (not removed — delegated to excel-data-cleaner)")

    return df_consolidated


# ------------------------------------------------------------------
# STANDALONE EXECUTION / EJECUCIÓN INDEPENDIENTE
# ------------------------------------------------------------------

if __name__ == "__main__":
    # Resolve project root — works whether script runs from /src or project root
    ROOT = Path(__file__).resolve().parent.parent
    DATA_DIR = ROOT / "data" / "raw"
    OUTPUT_FILE = ROOT / "outputs" / "consolidated.xlsx"

    # --- Load ---
    file_paths = sorted(DATA_DIR.glob("source_*.xlsx"))

    if not file_paths:
        raise FileNotFoundError(f"No source files found in: {DATA_DIR}")

    print(f"Loading {len(file_paths)} files from: {DATA_DIR}\n")
    lista_dfs = load_excels(file_paths)

    # --- Normalize ---
    print("\nNormalizing DataFrames...\n")
    lista_dfs_normalized = normalize_dfs(lista_dfs)

    # --- Consolidate ---
    print("\nConsolidating DataFrames...\n")
    df_consolidated = consolidate_excels(lista_dfs_normalized)

    # --- Export ---
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_consolidated.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

    print(f"\n✓ File exported: {OUTPUT_FILE.name}")
    print(f"  Path: {OUTPUT_FILE}")