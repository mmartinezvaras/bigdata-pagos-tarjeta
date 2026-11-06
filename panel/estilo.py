"""Aspecto del panel: CSS y piezas de HTML.

Criterios (skill emil-design-eng de Emil Kowalski, github.com/emilkowalski/skills):
  * Lo que se repite a menudo no se anima: el panel se refresca cada 2 s, así que se quitan el
    atenuado de Streamlit y el indicador "Running" (sin parpadeo).
  * Números que cambian con cifras de ancho fijo (tabular-nums) y tarjetas de tamaño fijo:
    nada se desplaza cuando llegan datos.
  * Botones: scale(0.97) al pulsar, ease-out fuerte, < 160 ms; hover solo con ratón;
    objetivo de 44 px; alternativa para prefers-reduced-motion.
  * El color nunca va solo: cada estado lleva también su palabra.
Misma identidad que la presentación: IBM Plex Sans, JetBrains Mono y la misma paleta.
"""
import html

TINTA, SUAVE, GRIS = "#14213D", "#3D4A5C", "#8A93A3"
FONDO, PAPEL, LINEA = "#F6F4EF", "#FDFCF9", "#E2DED4"
NARANJA, AZUL, VERDE, ROJO = "#B5530F", "#2E5FA8", "#2F7D4F", "#B42318"

CSS = """<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
:root{--tinta:#14213D;--suave:#3D4A5C;--gris:#8A93A3;--papel:#FDFCF9;--linea:#E2DED4;--hueco:#ECE8DF;
--naranja:#B5530F;--azul:#2E5FA8;--verde:#2F7D4F;--rojo:#B42318;--ease-out:cubic-bezier(0.23,1,0.32,1);}
.stApp, .stApp p, .stApp li, .stApp label, .stApp button, .stApp h1, .stApp h2, .stApp h3{font-family:'IBM Plex Sans',system-ui,sans-serif;}
.stApp code, .stApp pre, .mono{font-family:'JetBrains Mono',ui-monospace,monospace;}
[data-stale="true"], .stale-element{opacity:1 !important;transition:none !important;}
[data-testid="stStatusWidget"]{visibility:hidden;}
[data-testid="stMetricValue"], .tabular{font-variant-numeric:tabular-nums;}
.block-container{padding-top:2rem;max-width:1400px;}
.stButton > button{min-height:44px;border-radius:10px;border:1px solid var(--linea);font-weight:500;
transition:transform 140ms var(--ease-out), background-color 150ms ease, border-color 150ms ease;}
.stButton > button:active:not(:disabled){transform:scale(0.97);}
@media (hover:hover) and (pointer:fine){.stButton > button:hover:not(:disabled){border-color:var(--naranja);}}
@media (prefers-reduced-motion:reduce){.stButton > button{transition:background-color 150ms ease, border-color 150ms ease;}
.stButton > button:active:not(:disabled){transform:none;}}
.cabecera{margin:0 0 18px;}
.eti{font-size:12px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--gris);}
.cabecera .titulo{font-size:34px;font-weight:700;color:var(--tinta);line-height:1.15;margin-top:4px;}
.cabecera .lema{font-size:15px;color:var(--suave);margin-top:4px;}
.seccion{font-size:18px;font-weight:600;color:var(--tinta);margin:22px 0 10px;}
.tuberia{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:14px;margin:0 0 8px;}
.etapa{position:relative;background:var(--papel);border:1px solid var(--linea);border-top:3px solid var(--linea);
border-radius:14px;padding:12px 16px;min-height:88px;}
.etapa[data-e="ok"]{border-top-color:var(--verde);}
.etapa[data-e="aviso"]{border-top-color:var(--naranja);}
.etapa[data-e="mal"]{border-top-color:var(--rojo);}
.etapa:not(:last-child)::after{content:"→";position:absolute;right:-13px;top:50%;transform:translateY(-50%);color:var(--gris);font-size:15px;}
.etapa .valor{font-size:20px;font-weight:600;color:var(--tinta);margin-top:6px;font-variant-numeric:tabular-nums;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.etapa .detalle{font-size:13px;color:var(--suave);margin-top:2px;}
.rejilla{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;}
.tarjeta{background:var(--papel);border:1px solid var(--linea);border-radius:14px;padding:16px 18px;min-height:150px;
display:flex;flex-direction:column;gap:4px;}
.tarjeta .num{font-size:32px;font-weight:600;color:var(--tinta);line-height:1.15;font-variant-numeric:tabular-nums;}
.tarjeta .sub{font-size:13px;color:var(--suave);}
.barra{height:8px;border-radius:999px;background:var(--hueco);overflow:hidden;margin-top:auto;}
.relleno{height:100%;width:100%;background:var(--naranja);transform-origin:left center;}
.pie{display:flex;justify-content:space-between;align-items:center;gap:8px;font-size:13px;color:var(--suave);margin-top:6px;font-variant-numeric:tabular-nums;}
.pastilla{display:inline-flex;align-items:center;gap:6px;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:600;
font-variant-numeric:tabular-nums;white-space:nowrap;}
.pastilla::before{content:"";width:7px;height:7px;border-radius:50%;background:currentColor;flex-shrink:0;}
.p-ok{background:#E4F1E8;color:var(--verde);}
.p-aviso{background:#FBE9DC;color:var(--naranja);}
.p-mal{background:#FBE4E2;color:var(--rojo);}
.p-off{background:var(--hueco);color:var(--suave);}
.nota{font-size:13px;color:var(--suave);margin:6px 0 0;}
.ficheros{width:100%;border-collapse:collapse;font-size:13px;}
.ficheros td{padding:7px 10px;border-top:1px solid var(--linea);color:var(--suave);font-variant-numeric:tabular-nums;}
.ficheros td.mono{color:var(--tinta);}
.ficheros tr:first-child td{border-top:none;}
.caja{background:var(--papel);border:1px solid var(--linea);border-radius:14px;padding:6px;}
.vacio{display:flex;align-items:center;justify-content:center;min-height:120px;color:var(--gris);font-size:14px;text-align:center;padding:0 20px;}
.resultado{border-radius:16px;padding:18px 22px;display:flex;flex-direction:column;gap:4px;border:1px solid var(--linea);}
.r-bien{background:#E4F1E8;border-color:#BFDCC8;}
.r-mal{background:#FBE4E2;border-color:#F1C2BD;}
.r-neutro{background:var(--papel);}
.resultado .grande{font-size:20px;font-weight:700;color:var(--tinta);}
.resultado .peq{font-size:14px;color:var(--suave);}
.kpis{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:14px;margin:14px 0 4px;}
.kpi{background:var(--papel);border:1px solid var(--linea);border-radius:14px;padding:14px 16px;min-height:96px;}
.kpi .v{font-size:28px;font-weight:600;color:var(--tinta);font-variant-numeric:tabular-nums;margin-top:4px;}
.kpi .d{font-size:12px;color:var(--suave);}
@media (max-width:1000px){.tuberia,.rejilla,.kpis{grid-template-columns:repeat(2,minmax(0,1fr));}.etapa::after{display:none;}}
</style>"""


def e(texto) -> str:
    return html.escape(str(texto))


def miles(n) -> str:
    return "—" if n is None else f"{int(n):,}".replace(",", ".")


def pastilla(texto: str, tono: str) -> str:
    """tono: ok | aviso | mal | off. El texto siempre dice el estado (no solo el color)."""
    return f'<span class="pastilla p-{tono}">{e(texto)}</span>'


def cabecera() -> str:
    return ('<div class="cabecera"><div class="eti">Proyecto individual · Sistemas de Big Data</div>'
            '<div class="titulo">Pagos con tarjeta · panel del sistema</div>'
            '<div class="lema">Lo que ves sale del sistema en este momento. Las respuestas son las que da Hive.</div></div>')


def seccion(texto: str) -> str:
    return f'<div class="seccion">{e(texto)}</div>'


def tuberia(etapas: list[tuple[str, str, str, str]]) -> str:
    """etapas: (nombre, valor, detalle, estado ok|aviso|mal|off)."""
    partes = "".join(
        f'<div class="etapa" data-e="{estado}"><div class="eti">{e(nombre)}</div>'
        f'<div class="valor">{e(valor)}</div><div class="detalle">{e(detalle)}</div></div>'
        for nombre, valor, detalle, estado in etapas)
    return f'<div class="tuberia">{partes}</div>'


def tarjeta_particion(p: dict) -> str:
    fin, conf, lag = p["fin"], p["confirmado"], p["lag"]
    proporcion = 0 if not fin or conf is None else min(conf / fin, 1)
    if lag is None:
        estado = pastilla("sin commit" if fin else "vacía", "off")
    elif lag == 0:
        estado = pastilla("al día", "ok")
    else:
        estado = pastilla(f"{miles(lag)} pendientes", "aviso")
    return (f'<div class="tarjeta"><div class="eti">Partición {p["p"]}</div>'
            f'<div class="num">{miles(fin)}</div><div class="sub">mensajes en Kafka (offset final)</div>'
            f'<div class="barra"><div class="relleno" style="transform:scaleX({proporcion:.4f})"></div></div>'
            f'<div class="pie"><span>archivados {miles(conf or 0)}</span>{estado}</div></div>')


def tarjeta_total(lag, archivador: str) -> str:
    if lag is None:
        estado = pastilla("esperando el primer commit", "off")
    elif lag == 0:
        estado = pastilla("todo archivado", "ok")
    else:
        estado = pastilla("archivador muerto" if "muerto" in archivador else "poniéndose al día",
                          "mal" if "muerto" in archivador else "aviso")
    return (f'<div class="tarjeta"><div class="eti">LAG total</div>'
            f'<div class="num">{miles(lag)}</div><div class="sub">mensajes pendientes de archivar</div>'
            f'<div class="pie" style="margin-top:auto">{estado}</div></div>')


def rejilla(tarjetas: list[str]) -> str:
    return f'<div class="rejilla">{"".join(tarjetas)}</div>'


def tabla_ficheros(filas: list[dict]) -> str:
    if not filas:
        return '<div class="caja"><div class="vacio">Aún no hay ficheros en raw.</div></div>'
    cuerpo = "".join(
        f'<tr><td class="mono">{e(f["fichero"])}</td><td>{e(f["carpeta"])}</td>'
        f'<td style="text-align:right">{miles(f["bytes"])} B</td><td style="text-align:right">{e(f["hora"])}</td></tr>'
        for f in filas)
    return f'<div class="caja"><table class="ficheros">{cuerpo}</table></div>'


def vacio(texto: str) -> str:
    return f'<div class="caja"><div class="vacio">{e(texto)}</div></div>'


def resultado(tono: str, grande: str, peq: str) -> str:
    """tono: bien | mal | neutro."""
    return f'<div class="resultado r-{tono}"><div class="grande">{e(grande)}</div><div class="peq">{e(peq)}</div></div>'


def kpis(items: list[tuple[str, str, str]]) -> str:
    partes = "".join(f'<div class="kpi"><div class="eti">{e(t)}</div><div class="v">{e(v)}</div>'
                     f'<div class="d">{e(d)}</div></div>' for t, v, d in items)
    return f'<div class="kpis">{partes}</div>'


def grafico(chart):
    """Mismo aspecto en todos los gráficos de Altair."""
    fuente = "IBM Plex Sans"
    return (chart.configure_view(strokeWidth=0)
            .configure_axis(labelFont=fuente, titleFont=fuente, labelColor=SUAVE, titleColor=SUAVE,
                            gridColor=LINEA, domainColor=LINEA, tickColor=LINEA, labelFontSize=12,
                            titleFontSize=12, titleFontWeight=500)
            .configure_legend(labelFont=fuente, titleFont=fuente, labelColor=SUAVE, titleColor=SUAVE,
                              labelFontSize=12, orient="bottom")
            .configure_title(font=fuente, color=TINTA, fontSize=14, fontWeight=600, anchor="start"))
