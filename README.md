# DataPrep Pipeline

> **Sistema Automatizado de Limpieza y Transformación de Datos**  
> Proyecto de Título — Ingeniería de Datos

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://python.org)
[![Pandas](https://img.shields.io/badge/Pandas-2.2-150458?logo=pandas)](https://pandas.pydata.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-017CEE?logo=apacheairflow)](https://airflow.apache.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.35-FF4B4B?logo=streamlit)](https://streamlit.io)
[![Tests](https://img.shields.io/badge/Tests-48%20passed-22c55e?logo=pytest)](tests/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📌 Descripción

**DataPrep Pipeline** es un sistema automatizado que ingiere datos crudos desde múltiples fuentes, detecta problemas de calidad y aplica transformaciones automáticas para generar datasets listos para análisis.

Resuelve un problema real en Data Engineering: los analistas pierden entre el **60–80% de su tiempo** limpiando datos manualmente. Este sistema automatiza ese proceso.

---

## 🏗️ Arquitectura

```
Fuentes de Datos
(CSV / Excel / API REST)
        │
        ▼
  ┌─────────────┐
  │  Ingestion  │  ← src/ingestion.py
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │  Validation │  ← src/validation.py
  │  (Quality   │     • Nulos por columna
  │   Report)   │     • Duplicados
  └──────┬──────┘     • Outliers (IQR)
         │             • Tipos mixtos
         ▼
  ┌─────────────┐
  │   Cleaning  │  ← src/cleaning.py
  │             │     • Drop duplicates
  │             │     • Imputación nulos
  └──────┬──────┘     • Strip strings
         │
         ▼
  ┌──────────────────┐
  │  Transformation  │  ← src/transformation.py
  │                  │     • Parseo de fechas
  │                  │     • Columnas derivadas (total, IVA, categoría)
  └────────┬─────────┘     • Normalización min-max
           │
           ▼
  ┌──────────────┐
  │    Output    │  ← data/processed/
  │  CSV + HTML  │     • Dataset limpio
  │   Report     │     • Reporte de calidad HTML
  └──────────────┘

Orquestación: Apache Airflow (dags/data_pipeline_dag.py)
Dashboard:    Streamlit (app.py)
```

---

## 🗂️ Estructura del Proyecto

```
DataPrep/
├── data/
│   ├── raw/                    ← Datos crudos de entrada
│   └── processed/              ← Datasets limpios generados
├── dags/
│   └── data_pipeline_dag.py    ← DAG de Apache Airflow
├── src/
│   ├── ingestion.py            ← Carga CSV / Excel / API
│   ├── validation.py           ← Detección de problemas de calidad
│   ├── cleaning.py             ← Limpieza automática
│   ├── transformation.py       ← Transformaciones y columnas derivadas
│   ├── report.py               ← Generador de reporte HTML
│   └── logger.py               ← Logging del pipeline
├── config/
│   └── settings.py             ← Configuración global
├── tests/
│   ├── test_ingestion.py
│   ├── test_validation.py
│   ├── test_cleaning.py
│   └── test_transformation.py
├── reports/                    ← Reportes HTML generados
├── logs/                       ← Logs del pipeline
├── generate_dataset.py         ← Generador de dataset de prueba
├── main.py                     ← Runner CLI del pipeline
├── app.py                      ← Dashboard Streamlit
└── requirements.txt
```

---

## ⚡ Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/tu-usuario/DataPrep.git
cd DataPrep

# 2. Crear entorno virtual
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/macOS
source venv/bin/activate

# 3. Instalar dependencias
pip install -r requirements.txt
```

---

## 🚀 Uso

### 1. Generar dataset de prueba

```bash
python generate_dataset.py
```

Crea `data/raw/ventas.csv` con ~1050 filas y problemas reales: nulos, duplicados, fechas malas, tipos mixtos.

### 2. Ejecutar el pipeline (CLI)

```bash
# Con parámetros por defecto
python main.py

# Con parámetros personalizados
python main.py --input data/raw/mi_dataset.csv --output data/processed/resultado.csv
```

**Salida:**
- `data/processed/ventas_cleaned.csv` — dataset limpio y transformado
- `reports/quality_report.html` — reporte visual de calidad
- `logs/pipeline_YYYYMMDD.log` — log completo de la ejecución

### 3. Dashboard interactivo

```bash
streamlit run app.py
```

Abre `http://localhost:8501` con:

| Tab | Contenido |
|-----|-----------|
| 📊 Reporte de Calidad | KPIs, alertas, tabla por columna |
| 👁️ Antes / Después | Comparación de datos crudos vs limpios |
| 📈 Visualizaciones | Gráficos interactivos (Plotly) |
| 💾 Descargar | CSV limpio + Reporte HTML |

### 4. Ejecutar tests

```bash
python -m pytest tests/ -v
```

---

## 🔄 Airflow (Automatización)

```bash
# Instalar Airflow (solo si no está instalado)
pip install apache-airflow

# Inicializar base de datos
airflow db init

# Crear usuario admin
airflow users create --username admin --password admin --firstname Admin \
  --lastname User --role Admin --email admin@example.com

# Copiar el DAG
copy dags\data_pipeline_dag.py %AIRFLOW_HOME%\dags\

# Iniciar servicios
airflow webserver --port 8080
airflow scheduler
```

Accede a `http://localhost:8080` y activa el DAG `dataprep_pipeline`.

**Flujo del DAG:**
```
ingest_data → validate_data → clean_data → transform_data → load_data
```
Schedule: `@daily`

---

## 🛠️ Stack Tecnológico

| Capa | Tecnología |
|------|-----------|
| Lenguaje | Python 3.10+ |
| Procesamiento | Pandas 2.2, NumPy |
| Orquestación | Apache Airflow 2.9 |
| Dashboard | Streamlit + Plotly |
| Templates | Jinja2 |
| Testing | Pytest |
| Formatos de entrada | CSV, Excel (.xlsx), API REST (JSON) |

---

## 🔍 Funcionalidades Detalladas

### Validación automática detecta:
- ✅ Nulos por columna (conteo y porcentaje)
- ✅ Filas duplicadas exactas
- ✅ Outliers estadísticos (método IQR)
- ✅ Columnas con tipos de datos mixtos
- ✅ Alertas configurables por umbral

### Limpieza automática aplica:
- ✅ Normalización de nombres de columnas
- ✅ Eliminación de filas completamente vacías
- ✅ Eliminación de duplicados exactos
- ✅ Strip de espacios en columnas texto
- ✅ Imputación de nulos numéricos (media/mediana/cero)
- ✅ Imputación de nulos categóricos (moda/Unknown)

### Transformaciones:
- ✅ Parseo de columnas de fecha → datetime64
- ✅ Extracción de año, mes, día
- ✅ Columna `total` = precio × cantidad
- ✅ Columna `total_con_iva` (IVA 19%)
- ✅ Categorización de ventas (Bajo/Medio/Alto/Premium)
- ✅ Normalización min-max opcional

---

## 📊 Ejemplo de Reporte de Calidad

```
Antes del pipeline:
  Filas totales : 1,050
  Nulos totales : 213
  Duplicados    : 52 (4.9%)
  Alertas       : 8

Después del pipeline:
  Filas totales : 998
  Nulos totales : 0
  Duplicados    : 0
```

---

## 🎓 Contexto Académico

**Título del proyecto:**  
*"Sistema Automatizado de Limpieza y Transformación de Datos para Preparación de Análisis utilizando Python y Apache Airflow"*

**Área:** Data Engineering / ETL Pipeline  
**Stack:** Python · Pandas · Apache Airflow · Streamlit  
**Salida laboral:** Data Engineer, Analytics Engineer, Data Analyst

---

## 📄 Licencia

MIT License — ver [LICENSE](LICENSE)

---

<div align="center">
  <strong>DataPrep Pipeline</strong> · Construido con Python & 💜
</div>