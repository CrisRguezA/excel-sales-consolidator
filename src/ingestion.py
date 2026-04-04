"""
ingestion.py
 
EN:
Loads multiple Excel source files into a list of DataFrames.
Strips whitespace from column names immediately on load to avoid downstream
matching issues. Returns a list of raw DataFrames ready for normalization.
 
Pipeline contract:
    load_excels(file_paths: list[Path]) -> list[pd.DataFrame]
    Each returned DataFrame has:
      - All columns read as strings (no early type coercion)
      - Column names stripped of leading/trailing whitespace
 
ES:
Carga múltiples archivos Excel fuente en una lista de DataFrames.
Elimina espacios en los nombres de columna en el momento de la carga para
evitar problemas en pasos posteriores. Devuelve una lista de DataFrames
crudos listos para la normalización.
 
Contrato del pipeline:
    load_excels(file_paths: list[Path]) -> list[pd.DataFrame]
    Cada DataFrame devuelto tiene:
      - Todas las columnas leídas como strings (sin conversión de tipo prematura)
      - Nombres de columna sin espacios iniciales ni finales
"""
 
# ------------------------------------------------------------------
# IMPORTS
# ------------------------------------------------------------------
 
import pandas as pd
from pathlib import Path
 
 
# ------------------------------------------------------------------
# MAIN FUNCTION / FUNCIÓN PRINCIPAL
# ------------------------------------------------------------------
 
def load_excels(file_paths: list) -> list:
    """
    EN: Loads a list of Excel file paths into a list of DataFrames.
    ES: Carga una lista de rutas de archivos Excel en una lista de DataFrames.
 
    Args:
        file_paths : list of Path objects pointing to .xlsx source files
 
    Returns:
        list of pd.DataFrame — one per file, columns stripped, all values as str
    """
    lista_dfs = []
 
    for file_path in file_paths:
        file_path = Path(file_path)
 
        if not file_path.exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")
 
        df = pd.read_excel(file_path, dtype=str)   # Read all columns as string to avoid early type coercion
        df.columns = df.columns.str.strip()         # Strip whitespace from column names immediately on load
        lista_dfs.append(df)
 
        print(f"  ✓ {file_path.name} — {df.shape[0]} rows × {df.shape[1]} columns")
 
    print(f"\n{len(lista_dfs)} files loaded successfully")
    return lista_dfs


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