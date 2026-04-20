# Especificación Funcional — excel-sales-consolidator

## 1. Resumen del proyecto

`excel-sales-consolidator` es un pipeline modular en Python que automatiza la ingesta, normalización y consolidación de múltiples archivos Excel con esquemas heterogéneos en un único dataset estructurado y trazable.

**Problema que resuelve:** Las empresas reciben habitualmente entre 5 y 10 archivos Excel de distintos departamentos o comerciales, todos con datos similares pero con nombres de columna inconsistentes, formatos de fecha dispares, formatos numéricos mixtos y valores nulos. Unificarlos manualmente es costoso, lento y propenso a errores.

**Objetivo principal:** Automatizar la consolidación de esos archivos en un único Excel limpio, estructurado y con trazabilidad de origen por fila.

**Lugar en el pipeline:** Primer módulo de un pipeline end-to-end de tres etapas:
```
[excel-sales-consolidator] → [excel-data-cleaner] → [excel-report-formatter]
```

---

## 1.1 Valor para negocio

- Elimina el trabajo manual de copiar y pegar datos entre archivos
- Garantiza un esquema de columnas consistente independientemente del origen
- Añade trazabilidad por fila (`source_file`) para auditoría y control de calidad
- Reduce el tiempo de preparación de datos de horas a segundos
- Produce un output reproducible y automatizable
- Base estructurada para aplicar limpieza profunda y reportes en etapas posteriores

---

## 2. Usuarios y roles

| Usuario | Descripción | Interacción con el sistema |
|---------|-------------|---------------------------|
| Analista de datos | Recibe archivos Excel de distintas fuentes y necesita unificarlos | Ejecuta el pipeline y obtiene `consolidated.xlsx` |
| Data Engineer | Mantiene y adapta el pipeline a nuevas fuentes | Modifica `COLUMN_MAP` en `normalization.py` |
| Responsable de área | Necesita un informe consolidado periódico | Consume el output del pipeline |

> **Nota:** En la fase actual el sistema se ejecuta desde línea de comandos. En una fase futura podría integrarse con una interfaz Streamlit o una API.

---

## 3. Casos de uso

| ID | Actor | Acción | Resultado esperado |
|----|-------|--------|--------------------|
| CU-01 | Analista | Ejecuta `python -m src.consolidation` | Se generan los DataFrames normalizados y se exporta `consolidated.xlsx` |
| CU-02 | Analista | Añade un nuevo archivo fuente en `data/raw/` | El pipeline lo ingesta y normaliza junto al resto |
| CU-03 | Data Engineer | Añade una nueva variante de columna al `COLUMN_MAP` | El pipeline reconoce la nueva variante en futuras ejecuciones |
| CU-04 | Analista | Ejecuta el pipeline con un archivo faltante | El sistema lanza `FileNotFoundError` con la ruta del archivo no encontrado |
| CU-05 | Analista | El archivo fuente tiene columnas no mapeadas | Las columnas se descartan; se mantiene la estructura estándar |
| CU-06 | Analista | El archivo fuente está vacío | El sistema lo procesa sin fallar y contribuye con 0 filas al consolidado |

---

## 4. Flujos de uso

### Flujo principal: consolidación completa

1. El usuario ejecuta `python -m src.consolidation` desde la raíz del proyecto
2. `load_excels()` lee los archivos `.xlsx` de `data/raw/` preservando el contenido original sin conversión de tipos
3. Se aplica `str.strip()` a los nombres de columna en cada archivo
4. `normalize_dfs()` aplica `COLUMN_MAP` a cada DataFrame:
   - Busca variantes de cada columna estándar
   - Combina variantes con `combine_first()` priorizando el primer valor no nulo
   - Rellena con `pd.NA` si no se encuentra ninguna variante
   - Añade columna `source_file` con el nombre del archivo origen
5. `consolidate_excels()` concatena todos los DataFrames normalizados con `pd.concat()`
6. Se detectan filas duplicadas — definidas como filas con todos los campos iguales — y se reportan en consola sin eliminarlas
7. Se exporta `consolidated.xlsx` a `outputs/`

### Flujo alternativo: archivo fuente no encontrado

1. El usuario ejecuta el pipeline
2. `load_excels()` intenta abrir un archivo que no existe en la lista de rutas
3. El sistema lanza `FileNotFoundError` con la ruta completa del archivo
4. La ejecución se detiene — no se genera output parcial

---

## 5. Requisitos funcionales

### 5.1 Ingesta (`ingestion.py`)

| ID | Requisito |
|----|-----------|
| RF-01 | El sistema debe leer los archivos Excel preservando el contenido original sin conversión de tipos |
| RF-02 | El sistema debe aplicar `str.strip()` a los nombres de columna en el momento de la carga |
| RF-03 | El sistema debe lanzar `FileNotFoundError` si algún archivo de la lista de rutas no existe |
| RF-04 | El sistema debe procesar archivos vacíos sin fallar, contribuyendo con 0 filas al consolidado |
| RF-05 | El sistema debe imprimir en consola el nombre, número de filas y columnas de cada archivo procesado |

### 5.2 Normalización (`normalization.py`)

| ID | Requisito |
|----|-----------|
| RF-06 | El sistema debe mapear todas las variantes de nombres de columna a 12 columnas estándar definidas en `COLUMN_MAP` |
| RF-07 | El sistema debe combinar columnas semánticamente equivalentes con `combine_first()` priorizando el primer valor no nulo |
| RF-08 | El sistema debe rellenar con `pd.NA` las columnas estándar no encontradas en un archivo |
| RF-09 | El sistema debe añadir una columna `source_file` con el nombre del archivo origen |
| RF-10 | El sistema debe garantizar el mismo orden de columnas en todos los DataFrames normalizados |
| RF-11 | El sistema debe verificar que al menos una columna estándar está presente en cada archivo procesado |

### 5.3 Consolidación (`consolidation.py`)

| ID | Requisito |
|----|-----------|
| RF-12 | El sistema debe concatenar todos los DataFrames normalizados en uno único con índice secuencial |
| RF-13 | El sistema debe detectar y reportar filas duplicadas — definidas como filas con todos los campos iguales — sin eliminarlas |
| RF-14 | El sistema debe imprimir en consola: archivos procesados, filas por archivo, total de filas consolidadas y duplicados detectados |
| RF-15 | El sistema debe exportar el DataFrame consolidado a `outputs/consolidated.xlsx` |
| RF-16 | El sistema debe crear el directorio `outputs/` si no existe |

---

## 6. Reglas de negocio

| ID | Regla | Descripción |
|----|-------|-------------|
| RN-01 | Prioridad en columnas duplicadas | Cuando un archivo tiene varias columnas semánticamente equivalentes, se prioriza la primera con valor no nulo por fila (`combine_first`) |
| RN-02 | Columnas no mapeadas se descartan | Cualquier columna del archivo fuente que no aparezca en `COLUMN_MAP` se ignora |
| RN-03 | Columnas ausentes se rellenan con NA | Si un archivo no contiene ninguna variante de una columna estándar, esa columna se rellena con `pd.NA` para mantener el esquema |
| RN-04 | Sin eliminación de duplicados | Los duplicados se detectan y reportan, pero no se eliminan — responsabilidad delegada a `excel-data-cleaner` |
| RN-05 | Todos los valores se preservan sin conversión | No se realiza conversión de tipos en esta etapa para evitar pérdida de información |
| RN-06 | Trazabilidad obligatoria | Cada fila del output lleva la columna `source_file` indicando su archivo de origen |
| RN-07 | Orden de columnas fijo | El orden de columnas del output es fijo y definido por el esquema estándar de `COLUMN_MAP` |

---

## 7. Consideraciones técnicas

- **Input:** lista de rutas `.xlsx` pasada a `load_excels()`, archivos leídos preservando contenido original
- **Output:** `outputs/consolidated.xlsx`, exportado con `openpyxl`
- **Ejecución:** `python -m src.consolidation` desde la raíz del proyecto
- **Estructura modular:** tres scripts independientes en `src/` importables entre sí
- **Importaciones internas:** requiere `src/__init__.py` para que Python reconozca `src` como paquete
- **Entorno:** Python 3.11, conda, Windows/PowerShell
- **Dependencias:** pandas, openpyxl, xlsxwriter (ver `requirements.txt`)
- **Notebooks:** lógica explorada en Jupyter antes de extraerse a scripts reutilizables

---

## 8. Criterios de aceptación

| ID | Criterio |
|----|----------|
| CA-01 | Dado N archivos fuente, el número de filas del output debe ser igual a la suma de filas de todos los inputs incluyendo duplicados |
| CA-02 | Dado un archivo con columnas en distinto orden, el sistema produce un DataFrame con columnas en el orden estándar de `COLUMN_MAP` |
| CA-03 | Dado un archivo con dos variantes de la columna `cliente`, el sistema las combina en una sola columna sin pérdida de valores no nulos |
| CA-04 | Dado un archivo que no contiene ninguna variante de una columna estándar, el sistema rellena esa columna con `pd.NA` |
| CA-05 | Dado un archivo faltante en la lista de rutas, el sistema lanza `FileNotFoundError` con la ruta completa |
| CA-06 | Dado el output consolidado, cada fila contiene el nombre del archivo origen en la columna `source_file` |
| CA-07 | Dado el output consolidado, los duplicados son reportados en consola pero no eliminados |
| CA-08 | Dado un archivo vacío en la lista de rutas, el sistema lo procesa sin fallar |

---

## 9. Futuras mejoras

- Refactorizar `excel-data-cleaner` para que sea agnóstico al dataset y pueda conectarse como segunda etapa del pipeline
- Integrar `excel-report-formatter` como tercera etapa para aplicar formato visual profesional al output
- Construir un pipeline unificado ejecutable con un único comando end-to-end
- Añadir interfaz Streamlit para subida de archivos sin uso de terminal
- Soporte para archivos `.csv` además de `.xlsx`
- Generación de log de ejecución con métricas detalladas: filas por archivo, nulos por columna, duplicados detectados
- Parametrizar `COLUMN_MAP` desde un archivo de configuración externo (`.yaml` o `.json`) para facilitar adaptación a nuevos dominios
- Añadir validación mínima de esquema: advertencia si ninguna columna estándar se encuentra en un archivo

---

## 10. Supuestos

- Los archivos fuente contienen datos del mismo dominio con esquemas similares aunque inconsistentes
- Los nombres de columna de los archivos fuente son variantes conocidas incluidas en `COLUMN_MAP`
- La lista de rutas de archivos se construye correctamente antes de llamar a `load_excels()`
- El entorno tiene instaladas todas las dependencias de `requirements.txt`
- El script se ejecuta desde la raíz del proyecto con `python -m src.consolidation`
- `excel-data-cleaner` recibirá el output de este módulo en una fase futura, pero actualmente requiere refactorización previa
