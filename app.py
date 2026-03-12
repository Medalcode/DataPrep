"""
app.py
------
DataPrep Pipeline — Streamlit Dashboard
Interactive web UI for data quality analysis, cleaning, transformation, and download.

Run:
    streamlit run app.py
"""
import sys
import io
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Project imports ───────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import PIPELINE_VERSION, PIPELINE_NAME, CLEANING_CONFIG, VALIDATION_CONFIG
from src.validation import validate_data
from src.cleaning import clean_data
from src.transformation import transform_data
from src.report import generate_quality_report

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DataPrep Pipeline",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  html, body, [class*="css"]  { font-family: 'Inter', sans-serif; }

  /* KPI cards */
  .kpi-card {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border: 1px solid #334155;
    border-radius: 16px;
    padding: 1.2rem 1rem;
    text-align: center;
    margin: 0.25rem 0;
  }
  .kpi-val   { font-size: 2.2rem; font-weight: 700; color: #818cf8; line-height: 1.1; }
  .kpi-label { font-size: 0.8rem; color: #94a3b8; margin-top: 0.25rem; }
  .kpi-delta { font-size: 0.8rem; margin-top: 0.2rem; }
  .delta-ok   { color: #22c55e; }
  .delta-warn { color: #f59e0b; }
  .delta-bad  { color: #ef4444; }

  /* Section headers */
  .section-title {
    font-size: 1.1rem; font-weight: 600;
    border-left: 3px solid #6366f1;
    padding-left: 0.75rem;
    margin: 1.5rem 0 0.75rem;
    color: #e2e8f0;
  }

  /* Alert boxes */
  .alert-warn {
    background: #1c1012; border-left: 3px solid #f59e0b;
    padding: 0.6rem 1rem; border-radius: 8px;
    font-size: 0.85rem; color: #fbbf24; margin: 0.3rem 0;
  }
  .alert-ok {
    background: #0a1f12; border-left: 3px solid #22c55e;
    padding: 0.6rem 1rem; border-radius: 8px;
    font-size: 0.85rem; color: #4ade80;
  }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def kpi(label: str, value, delta: str = "", delta_class: str = ""):
    delta_html = f'<div class="kpi-delta {delta_class}">{delta}</div>' if delta else ""
    st.markdown(
        f'<div class="kpi-card">'
        f'<div class="kpi-val">{value}</div>'
        f'<div class="kpi-label">{label}</div>'
        f'{delta_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


@st.cache_data
def run_pipeline(df_raw: pd.DataFrame):
    before = validate_data(df_raw, **VALIDATION_CONFIG)
    df_clean    = clean_data(df_raw, config=CLEANING_CONFIG)
    df_final    = transform_data(df_clean)
    after       = validate_data(df_final)
    return df_clean, df_final, before, after


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/color/96/data-configuration.png", width=60)
    st.title("DataPrep Pipeline")
    st.caption(f"v{PIPELINE_VERSION}")
    st.divider()

    st.subheader("📁 Cargar datos")
    uploaded = st.file_uploader("Sube un archivo CSV o Excel", type=["csv", "xlsx", "xls"])

    use_demo = False
    demo_path = Path(__file__).parent / "data" / "raw" / "ventas.csv"
    if demo_path.exists():
        use_demo = st.checkbox("Usar dataset de demostración", value=not bool(uploaded))

    st.divider()
    st.subheader("⚙️ Opciones")
    show_normalize = st.checkbox("Normalizar columnas numéricas (min-max)", value=False)
    null_thresh    = st.slider("Umbral alerta nulos (%)", 5, 50, 20)
    dup_thresh     = st.slider("Umbral alerta duplicados (%)", 1, 20, 5)
    st.divider()
    st.caption("🎓 Proyecto de Título · Ingeniería de Datos")


# ── Load data ─────────────────────────────────────────────────────────────────
df_raw = None

if uploaded:
    try:
        if uploaded.name.endswith(".csv"):
            df_raw = pd.read_csv(uploaded)
        else:
            df_raw = pd.read_excel(uploaded)
        st.sidebar.success(f"✅ {uploaded.name} cargado")
    except Exception as e:
        st.sidebar.error(f"Error al cargar: {e}")

elif use_demo and demo_path.exists():
    df_raw = pd.read_csv(demo_path)
    st.sidebar.info("📊 Usando dataset de demostración")

# ── Main content ──────────────────────────────────────────────────────────────
st.markdown(
    "<h1 style='background:linear-gradient(90deg,#6366f1,#818cf8);-webkit-background-clip:text;"
    "-webkit-text-fill-color:transparent;font-size:2.2rem;font-weight:800;margin-bottom:0'>"
    "⚡ DataPrep Pipeline</h1>",
    unsafe_allow_html=True,
)
st.caption("Sistema Automatizado de Limpieza y Transformación de Datos")
st.divider()

if df_raw is None:
    st.info("👈 **Sube un archivo CSV/Excel** desde el menú lateral, o activa el dataset de demostración.")
    st.markdown("""
    ### ¿Qué hace este sistema?
    
    | Paso | Descripción |
    |------|-------------|
    | 📥 **Ingesta** | Carga datos desde CSV, Excel o API |
    | 🔍 **Validación** | Detecta nulos, duplicados, outliers y tipos mixtos |
    | 🧹 **Limpieza** | Elimina duplicados, imputa nulos, normaliza textos |
    | 🔄 **Transformación** | Parsea fechas, crea columnas derivadas (total, IVA, categoría) |
    | 📊 **Reporte** | Genera reporte HTML de calidad de datos |
    """)
    st.stop()


# ── Run pipeline ──────────────────────────────────────────────────────────────
VALIDATION_CONFIG_OVERRIDE = {
    "null_threshold_pct": null_thresh,
    "duplicate_threshold_pct": dup_thresh,
    "iqr_factor": 1.5,
}

with st.spinner("Ejecutando pipeline..."):
    before_report = validate_data(df_raw, **VALIDATION_CONFIG_OVERRIDE)
    df_clean      = clean_data(df_raw, config=CLEANING_CONFIG)
    df_final      = transform_data(df_clean, normalize=show_normalize)
    after_report  = validate_data(df_final)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Reporte de Calidad",
    "👁️ Datos Antes / Después",
    "📈 Visualizaciones",
    "💾 Descargar",
])

# ────────────────────── TAB 1: Quality Report ────────────────────────────────
with tab1:
    st.markdown('<div class="section-title">📊 Métricas Generales</div>', unsafe_allow_html=True)

    cols = st.columns(6)
    pairs = [
        ("Filas (antes)",     before_report.total_rows, "", ""),
        ("Filas (después)",   after_report.total_rows,
         f"−{before_report.total_rows - after_report.total_rows}", "delta-ok"),
        ("Duplicados",        before_report.duplicate_rows,
         f"{before_report.duplicate_pct:.1f}%",
         "delta-bad" if before_report.duplicate_pct > dup_thresh else "delta-ok"),
        ("Nulos (antes)",     sum(before_report.null_counts.values()), "", "delta-warn"),
        ("Nulos (después)",   sum(after_report.null_counts.values()), "✓ 0", "delta-ok"),
        ("Alertas",           len(before_report.alerts), "", "delta-bad" if before_report.alerts else "delta-ok"),
    ]
    for col, (label, val, delta, cls) in zip(cols, pairs):
        with col:
            kpi(label, val, delta, cls)

    # Alerts
    st.markdown('<div class="section-title">⚠️ Alertas Detectadas</div>', unsafe_allow_html=True)
    if before_report.alerts:
        for alert in before_report.alerts:
            st.markdown(f'<div class="alert-warn">{alert}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="alert-ok">✅ No se detectaron alertas</div>', unsafe_allow_html=True)

    # Per-column table
    st.markdown('<div class="section-title">🔎 Calidad por Columna</div>', unsafe_allow_html=True)
    col_data = []
    for col in before_report.null_counts:
        col_data.append({
            "Columna":        col,
            "Tipo":           before_report.dtypes.get(col, "—"),
            "Nulos (antes)":  before_report.null_counts.get(col, 0),
            "% Nulos":        f"{before_report.null_pcts.get(col, 0):.1f}%",
            "Nulos (después)": after_report.null_counts.get(col, 0),
            "Outliers":       before_report.outlier_counts.get(col, 0),
            "Estado": "✅ OK" if before_report.null_pcts.get(col, 0) == 0
                      else ("⚠ Medio" if before_report.null_pcts.get(col, 0) <= 20 else "🔴 Alto"),
        })
    st.dataframe(pd.DataFrame(col_data), use_container_width=True, hide_index=True)


# ────────────────── TAB 2: Before / After ────────────────────────────────────
with tab2:
    col_before, col_after = st.columns(2)

    with col_before:
        st.markdown("### 📋 Datos Originales (sucios)")
        st.caption(f"{len(df_raw):,} filas × {len(df_raw.columns)} columnas")
        st.dataframe(df_raw.head(50), use_container_width=True, height=400)
        st.write("**Estadísticas:**")
        st.dataframe(df_raw.describe(include="all").T, use_container_width=True)

    with col_after:
        st.markdown("### ✨ Datos Limpios + Transformados")
        st.caption(f"{len(df_final):,} filas × {len(df_final.columns)} columnas")
        st.dataframe(df_final.head(50), use_container_width=True, height=400)
        st.write("**Estadísticas:**")
        st.dataframe(df_final.describe(include="all").T, use_container_width=True)


# ────────────────────── TAB 3: Visualizations ────────────────────────────────
with tab3:
    st.markdown('<div class="section-title">📉 Nulos por Columna (Antes vs Después)</div>',
                unsafe_allow_html=True)

    null_before = pd.Series(before_report.null_counts, name="Antes")
    null_after  = pd.Series({
        c: after_report.null_counts.get(c, 0) for c in before_report.null_counts
    }, name="Después")

    null_df = pd.DataFrame({"Antes": null_before, "Después": null_after}).reset_index()
    null_df.columns = ["Columna", "Antes", "Después"]
    null_df = null_df[null_df["Antes"] > 0].sort_values("Antes", ascending=False)

    if not null_df.empty:
        fig_null = px.bar(
            null_df, x="Columna", y=["Antes", "Después"],
            barmode="group",
            color_discrete_map={"Antes": "#ef4444", "Después": "#22c55e"},
            template="plotly_dark",
            title="Valores Nulos: Antes vs Después del Pipeline",
        )
        fig_null.update_layout(plot_bgcolor="#0f172a", paper_bgcolor="#0f172a")
        st.plotly_chart(fig_null, use_container_width=True)
    else:
        st.info("No había nulos en el dataset original.")

    # Column: Total distribution if exists
    if "total" in df_final.columns:
        st.markdown('<div class="section-title">💰 Distribución de Ventas Totales</div>',
                    unsafe_allow_html=True)
        fig_hist = px.histogram(
            df_final, x="total", nbins=40,
            color_discrete_sequence=["#6366f1"],
            template="plotly_dark",
            title="Distribución de Totales de Venta",
            labels={"total": "Total ($)"},
        )
        fig_hist.update_layout(plot_bgcolor="#0f172a", paper_bgcolor="#0f172a")
        st.plotly_chart(fig_hist, use_container_width=True)

    # Category breakdown
    if "categoria_venta" in df_final.columns:
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown('<div class="section-title">🏷️ Categorías de Venta</div>',
                        unsafe_allow_html=True)
            cat_counts = df_final["categoria_venta"].value_counts().reset_index()
            cat_counts.columns = ["Categoría", "Cantidad"]
            fig_pie = px.pie(
                cat_counts, names="Categoría", values="Cantidad",
                color_discrete_sequence=px.colors.sequential.Plasma_r,
                template="plotly_dark",
            )
            fig_pie.update_layout(paper_bgcolor="#0f172a")
            st.plotly_chart(fig_pie, use_container_width=True)

    # Sales by region
    region_col = next((c for c in df_final.columns if "region" in c), None)
    if region_col and "total" in df_final.columns:
        with col_b:
            st.markdown('<div class="section-title">🗺️ Ventas por Región</div>',
                        unsafe_allow_html=True)
            reg_df = df_final.groupby(region_col)["total"].sum().reset_index()
            reg_df.columns = ["Región", "Total"]
            fig_bar = px.bar(
                reg_df.sort_values("Total", ascending=True),
                x="Total", y="Región", orientation="h",
                color="Total",
                color_continuous_scale="Plasma",
                template="plotly_dark",
            )
            fig_bar.update_layout(plot_bgcolor="#0f172a", paper_bgcolor="#0f172a")
            st.plotly_chart(fig_bar, use_container_width=True)

    # Monthly trend if date parsed
    date_col = next((c for c in df_final.columns if "fecha_year" in c), None)
    if date_col and "total" in df_final.columns:
        st.markdown('<div class="section-title">📅 Ventas por Mes</div>',
                    unsafe_allow_html=True)
        trend_df = df_final.groupby(["fecha_year", "fecha_month"])["total"].sum().reset_index()
        trend_df["periodo"] = (
            trend_df["fecha_year"].astype(str) + "-"
            + trend_df["fecha_month"].astype(str).str.zfill(2)
        )
        fig_line = px.line(
            trend_df.sort_values("periodo"),
            x="periodo", y="total",
            markers=True,
            color_discrete_sequence=["#818cf8"],
            template="plotly_dark",
            title="Tendencia de Ventas Mensuales",
            labels={"periodo": "Período", "total": "Total ($)"},
        )
        fig_line.update_layout(plot_bgcolor="#0f172a", paper_bgcolor="#0f172a")
        st.plotly_chart(fig_line, use_container_width=True)


# ────────────────────── TAB 4: Download ──────────────────────────────────────
with tab4:
    st.markdown('<div class="section-title">💾 Descargar Resultados</div>', unsafe_allow_html=True)
    col_d1, col_d2 = st.columns(2)

    with col_d1:
        st.markdown("#### ✨ Dataset Limpio (CSV)")
        st.caption(f"{len(df_final):,} filas · {len(df_final.columns)} columnas")
        st.download_button(
            label="⬇️ Descargar CSV",
            data=to_csv_bytes(df_final),
            file_name="dataset_cleaned.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with col_d2:
        st.markdown("#### 📄 Reporte HTML de Calidad")
        st.caption("Reporte completo Before/After del pipeline")
        tmp_report = Path("/tmp/quality_report.html")
        generate_quality_report(
            before_report=before_report,
            after_report=after_report,
            report_path=tmp_report,
            version=PIPELINE_VERSION,
        )
        st.download_button(
            label="⬇️ Descargar Reporte HTML",
            data=tmp_report.read_bytes(),
            file_name="quality_report.html",
            mime="text/html",
            use_container_width=True,
        )

    st.divider()
    st.markdown("#### 📋 Dataset Limpio — Vista Completa")
    st.dataframe(df_final, use_container_width=True)
