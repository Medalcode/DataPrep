"""
src/report.py
-------------
HTML data quality report generator for DataPrep Pipeline.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from jinja2 import Template

from src.logger import get_logger
from src.validation import DataQualityReport

logger = get_logger("report")

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>DataPrep — Reporte de Calidad</title>
  <style>
    :root{--accent:#6366f1;--ok:#22c55e;--warn:#f59e0b;--danger:#ef4444;--bg:#0f172a;--card:#1e293b;--text:#e2e8f0;--sub:#94a3b8}
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);padding:2rem}
    h1{font-size:2rem;font-weight:700;margin-bottom:.25rem}
    h1 span{color:var(--accent)}
    .meta{color:var(--sub);font-size:.9rem;margin-bottom:2rem}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:1rem;margin:1.5rem 0}
    .kpi{background:var(--card);border-radius:12px;padding:1.25rem;text-align:center;border:1px solid #334155}
    .kpi .val{font-size:2rem;font-weight:700;color:var(--accent)}
    .kpi .lbl{font-size:.8rem;color:var(--sub);margin-top:.25rem}
    h2{font-size:1.2rem;font-weight:600;margin:2rem 0 .75rem;border-left:3px solid var(--accent);padding-left:.75rem}
    table{width:100%;border-collapse:collapse;background:var(--card);border-radius:12px;overflow:hidden;font-size:.875rem}
    th{background:#334155;padding:.75rem 1rem;text-align:left;font-weight:600;color:var(--sub)}
    td{padding:.65rem 1rem;border-top:1px solid #334155}
    tr:hover td{background:#273148}
    .badge{display:inline-block;padding:.2rem .6rem;border-radius:999px;font-size:.75rem;font-weight:600}
    .ok{background:#14532d;color:var(--ok)}
    .warn{background:#451a03;color:var(--warn)}
    .danger{background:#450a0a;color:var(--danger)}
    .alerts-box{background:var(--card);border:1px solid #334155;border-radius:12px;padding:1rem;margin:1rem 0}
    .alert-item{padding:.5rem;border-left:3px solid var(--warn);margin:.4rem 0;font-size:.875rem;padding-left:.75rem}
    footer{margin-top:3rem;text-align:center;color:var(--sub);font-size:.8rem}
  </style>
</head>
<body>
  <h1>DataPrep <span>Pipeline</span></h1>
  <p class="meta">Reporte de Calidad de Datos — {{ generated_at }}</p>

  <h2>Resumen General</h2>
  <div class="grid">
    <div class="kpi"><div class="val">{{ before.total_rows }}</div><div class="lbl">Filas (antes)</div></div>
    <div class="kpi"><div class="val">{{ after.total_rows }}</div><div class="lbl">Filas (después)</div></div>
    <div class="kpi"><div class="val">{{ before.duplicate_rows }}</div><div class="lbl">Duplicados eliminados</div></div>
    <div class="kpi"><div class="val">{{ total_nulls_before }}</div><div class="lbl">Nulos (antes)</div></div>
    <div class="kpi"><div class="val">{{ total_nulls_after }}</div><div class="lbl">Nulos (después)</div></div>
    <div class="kpi"><div class="val">{{ before.total_cols }}</div><div class="lbl">Columnas</div></div>
  </div>

  {% if before.alerts %}
  <h2>Alertas Detectadas</h2>
  <div class="alerts-box">
    {% for alert in before.alerts %}
    <div class="alert-item">{{ alert }}</div>
    {% endfor %}
  </div>
  {% endif %}

  <h2>Calidad por Columna (Antes vs Después)</h2>
  <table>
    <thead>
      <tr>
        <th>Columna</th>
        <th>Tipo</th>
        <th>Nulos (antes)</th>
        <th>% Nulos (antes)</th>
        <th>Nulos (después)</th>
        <th>Outliers</th>
        <th>Estado</th>
      </tr>
    </thead>
    <tbody>
      {% for col in columns %}
      <tr>
        <td><strong>{{ col }}</strong></td>
        <td>{{ before.dtypes.get(col, '—') }}</td>
        <td>{{ before.null_counts.get(col, 0) }}</td>
        <td>{{ before.null_pcts.get(col, 0) }}%</td>
        <td>{{ after.null_counts.get(col, 0) }}</td>
        <td>{{ before.outlier_counts.get(col, '—') }}</td>
        <td>
          {% if before.null_pcts.get(col, 0) > 20 %}
            <span class="badge danger">⚠ Alto</span>
          {% elif before.null_pcts.get(col, 0) > 0 %}
            <span class="badge warn">~ Medio</span>
          {% else %}
            <span class="badge ok">✓ OK</span>
          {% endif %}
        </td>
      </tr>
      {% endfor %}
    </tbody>
  </table>

  <footer>Generado por DataPrep Pipeline v{{ version }} | {{ generated_at }}</footer>
</body>
</html>
"""


def generate_quality_report(
    before_report: DataQualityReport,
    after_report: DataQualityReport,
    report_path: str | Path,
    version: str = "1.0.0",
) -> Path:
    """
    Render an HTML data quality report comparing before/after pipeline state.

    Args:
        before_report: DataQualityReport from raw data.
        after_report:  DataQualityReport from cleaned data.
        report_path:   Destination path for the HTML report.
        version:       Pipeline version string.

    Returns:
        Path to the generated HTML file.
    """
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    template = Template(HTML_TEMPLATE)
    columns  = list(before_report.null_counts.keys())

    html = template.render(
        before=before_report,
        after=after_report,
        columns=columns,
        total_nulls_before=sum(before_report.null_counts.values()),
        total_nulls_after=sum(after_report.null_counts.values()),
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        version=version,
    )

    report_path.write_text(html, encoding="utf-8")
    logger.info(f"Quality report saved → {report_path}")
    return report_path
