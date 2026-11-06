"""Panel del sistema de pagos: una ventana visual a Kafka, HDFS y Hive.

    ./pagos panel        →  http://localhost:8501

Todo lo que enseña sale del sistema en ese momento. Las respuestas de negocio son las que da
Hive con los mismos .sql de hive/consultas/; el panel no calcula nada por su cuenta.
El aspecto (estilo.py) sigue la skill emil-design-eng de Emil Kowalski.
"""
import time
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

import estilo as es
from sistema import HIVE_OUT, PROYECTO, PUBLICADO, Gestor

st.set_page_config(page_title="Pagos con tarjeta · Panel", page_icon="💳", layout="wide")
st.markdown(es.CSS, unsafe_allow_html=True)


@st.cache_resource
def gestor() -> Gestor:
    return Gestor()           # uno solo para todas las pestañas del navegador


g = gestor()


def html(trozo: str) -> None:
    st.markdown(trozo, unsafe_allow_html=True)


# ------------------------------------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------------------------------------
def leer_tsv(nombre: str) -> pd.DataFrame | None:
    """Salida de beeline (TSV con cabecera) → tabla. None si aún no existe."""
    ruta = HIVE_OUT / f"{nombre}.tsv"
    if not ruta.exists():
        return None
    lineas = [l.split("\t") for l in ruta.read_text(encoding="utf-8").splitlines() if l.strip()]
    if not lineas:
        return pd.DataFrame()
    cabecera = [c.split(".")[-1] for c in lineas[0]]
    df = pd.DataFrame([l for l in lineas[1:] if len(l) == len(cabecera)], columns=cabecera)
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass
    return df


def sql(nombre: str) -> str:
    return (PROYECTO / "hive" / "consultas" / f"{nombre}.sql").read_text(encoding="utf-8")


def bonito(valor):
    """27.0 → 27 (beeline lo da todo como texto y pandas lo convierte a número)."""
    if isinstance(valor, float) and valor.is_integer():
        return int(valor)
    return valor


def n_lineas(ruta: Path) -> int:
    if not ruta.exists():
        return 0
    with open(ruta, encoding="utf-8") as f:
        return sum(1 for l in f if l.strip())


def estado_comparacion() -> str:
    ruta = HIVE_OUT.parent / "comparacion.txt"
    texto = ruta.read_text(encoding="utf-8") if ruta.exists() else ""
    if "HIVE DA EXACTAMENTE LO ESPERADO" in texto:
        return "bien"
    if "HAY DIFERENCIAS" in texto:
        return "mal"
    return "sin"


# ------------------------------------------------------------------------------------------
# Cabecera
# ------------------------------------------------------------------------------------------
html(es.cabecera())

# ------------------------------------------------------------------------------------------
# Barra lateral: acciones
# ------------------------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### Acciones")
    ocupado = g.ocupado()

    html('<div class="eti">1 · Enviar pagos a Kafka</div>')
    if st.button("📤  Publicar la muestra (27)", disabled=ocupado, use_container_width=True,
                 help="La muestra de referencia. Publicarla otra vez = 27 duplicados (mismos tx_id)."):
        g.lanzar("Publicar la muestra", g.publicar, "tests/muestra/eventos.jsonl")
        st.toast("Publicando la muestra…")
    with st.expander("Generar y publicar más pagos"):
        n = st.number_input("Pagos", min_value=10, max_value=100_000, value=3000, step=500)
        ritmo = st.number_input("Pagos por segundo (0 = lo más rápido posible)",
                                min_value=0, max_value=5000, value=60, step=10)
        semilla = st.number_input("Semilla", min_value=0, value=7, step=1)
        if st.button("🎲  Generar y publicar", disabled=ocupado, use_container_width=True):
            g.lanzar(f"Generar y publicar {int(n)} pagos", g.generar_y_publicar,
                     int(n), float(ritmo), int(semilla))
            st.toast("Generando y publicando…")

    html('<div class="eti" style="margin-top:14px">2 · Archivador (Kafka → HDFS)</div>')
    if st.button("▶️  Arrancar", use_container_width=True, help="Sigue desde su último commit"):
        st.toast(g.archivador("arrancar"))
    if st.button("⏸  Parar (ordenado)", use_container_width=True,
                 help="SIGTERM: termina el lote en curso y sale"):
        st.toast(g.archivador("parar"))
    if st.button("💀  Matar en seco", use_container_width=True,
                 help="SIGKILL: muere sin avisar, como un corte de luz"):
        st.toast(g.archivador("matar"))

    html('<div class="eti" style="margin-top:14px">3 · Hive</div>')
    if st.button("🧮  Limpiar en Hive y responder", type="primary", disabled=ocupado,
                 use_container_width=True,
                 help="02_limpiar.sql + P1, P2, P3 + comparación con lo esperado (≈ 1 min)"):
        g.lanzar("Hive: limpiar y responder", g.responder)
        st.toast("Hive trabajando…")

    st.divider()
    confirmar = st.checkbox("Sí, quiero vaciar todo")
    if st.button("🧹  Vaciar el sistema", disabled=ocupado or not confirmar, use_container_width=True,
                 help="Igual que ./pagos reset-todo: topic, offsets, HDFS y particiones de Hive"):
        g.lanzar("Vaciar el sistema", g.vaciar)

    @st.fragment(run_every=1)
    def estado_tarea():
        tarea, ultimo = g.tarea, g.ultimo
        if tarea:
            html(es.resultado("neutro", f"⏳ {tarea['nombre']}",
                              f"{tarea['paso']} · {time.time() - tarea['inicio']:.0f} s"))
        elif ultimo:
            html(es.resultado("bien" if ultimo["ok"] else "mal", ultimo["nombre"],
                              f"{ultimo['mensaje']} · {ultimo['segundos']:.0f} s"))
        # Al terminar una acción, redibujar la página para reactivar los botones.
        if st.session_state.get("habia_tarea") and not tarea:
            st.session_state["habia_tarea"] = False
            st.rerun()
        st.session_state["habia_tarea"] = bool(tarea)

    st.divider()
    estado_tarea()


directo, respuestas, como = st.tabs(["En directo", "Respuestas de Hive", "Cómo funciona"])

# ------------------------------------------------------------------------------------------
# Pestaña 1: en directo
# ------------------------------------------------------------------------------------------
with directo:
    @st.fragment(run_every=2)
    def en_directo():
        foto, historial = g.ultima_foto()
        if not foto:
            html(es.vacio("Haciendo la primera foto del sistema…"))
            return

        particiones = foto.get("particiones", [])
        total = sum(p["fin"] for p in particiones)
        archivador = foto.get("archivador", "—")
        comp = estado_comparacion()
        html(es.tuberia([
            ("Simulador", f"{es.miles(n_lineas(PUBLICADO))} publicados", "desde el panel, tras vaciar",
             "ok" if PUBLICADO.exists() else "off"),
            ("Kafka", f"{es.miles(total)} mensajes", f"{len(particiones)} particiones · clave card_id",
             "ok" if foto.get("kafka_ok") else "mal"),
            ("Archivador", archivador, f"grupo {foto.get('grupo_estado', '—').lower()}",
             "ok" if archivador == "vivo" else "mal" if "muerto" in archivador else "aviso"),
            ("HDFS raw", f"{es.miles(foto.get('hdfs_n', 0))} ficheros",
             f"{foto.get('hdfs_bytes', 0) / 1e6:.2f} MB · replicación 2",
             "ok" if foto.get("hdfs_ok") else "mal"),
            ("Hive", "= esperado" if comp == "bien" else "no coincide" if comp == "mal" else
             ("listo" if foto.get("hive_ok") else "parado"),
             "P1 · P2 · P3 sobre Parquet",
             "ok" if comp == "bien" else "mal" if comp == "mal" or not foto.get("hive_ok") else "off"),
        ]))

        html(es.seccion("Kafka · topic card-payments"))
        if not particiones:
            html(es.vacio("El topic no existe o Kafka no responde."))
        else:
            html(es.rejilla([es.tarjeta_particion(p) for p in particiones]
                            + [es.tarjeta_total(foto.get("lag_total"), archivador)]))
            html('<p class="nota">La barra naranja es lo que el archivador ha confirmado (commit) sobre lo que '
                 'hay en la partición. El commit apunta al siguiente mensaje a leer y solo se hace '
                 'después de escribir en HDFS.</p>')

            izquierda, derecha = st.columns(2)
            filas = []
            for p in particiones:
                filas.append({"Partición": str(p["p"]), "Serie": "En Kafka", "Mensajes": p["fin"]})
                filas.append({"Partición": str(p["p"]), "Serie": "Archivado (commit)",
                              "Mensajes": p["confirmado"] or 0})
            barras = alt.Chart(pd.DataFrame(filas), title="En Kafka frente a archivado").mark_bar(
                cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
                x=alt.X("Partición:N", title="Partición", axis=alt.Axis(labelAngle=0)),
                xOffset="Serie:N",
                y=alt.Y("Mensajes:Q", title="Mensajes"),
                color=alt.Color("Serie:N", title=None,
                                scale=alt.Scale(domain=["En Kafka", "Archivado (commit)"],
                                                range=[es.AZUL, es.NARANJA])),
                tooltip=["Partición", "Serie", "Mensajes"],
            ).properties(height=260)
            izquierda.altair_chart(es.grafico(barras), use_container_width=True)

            if historial:
                df = pd.DataFrame(historial, columns=["Hora", "LAG", "En Kafka"])
                area = alt.Chart(df, title="LAG total en los últimos minutos").mark_area(
                    line={"color": es.NARANJA, "strokeWidth": 2}, color=es.NARANJA, opacity=0.18,
                    interpolate="step-after").encode(
                    x=alt.X("Hora:T", title=None, axis=alt.Axis(format="%H:%M:%S")),
                    y=alt.Y("LAG:Q", title="Mensajes pendientes"),
                    tooltip=[alt.Tooltip("Hora:T", format="%H:%M:%S"), "LAG", "En Kafka"],
                ).properties(height=260)
                derecha.altair_chart(es.grafico(area), use_container_width=True)
            else:
                with derecha:
                    html(es.vacio("La gráfica del LAG empieza cuando el archivador confirma su primer lote."))

        html(es.seccion("HDFS · últimos ficheros en /pagos/raw/card-payments"))
        html(es.tabla_ficheros(foto.get("hdfs_ultimos", [])))
        html('<p class="nota">Nombre = partición · offset inicial · offset final. Si el archivador vuelve '
             'tras una caída, los ficheros nuevos empiezan donde acabaron los anteriores.</p>')

        with st.expander("Últimas líneas del archivador"):
            st.code(foto.get("archivador_logs") or "(sin registro)", language=None)
        if foto.get("avisos"):
            with st.expander("⚠️ Avisos"):
                for aviso in foto["avisos"]:
                    st.write(aviso)

    en_directo()

# ------------------------------------------------------------------------------------------
# Pestaña 2: respuestas de Hive
# ------------------------------------------------------------------------------------------
with respuestas:
    @st.fragment(run_every=4)
    def respuestas_hive():
        control = leer_tsv("control")
        if control is None or control.empty:
            html(es.vacio("Todavía no hay respuestas. Envía pagos, deja que el archivador los guarde "
                          "y pulsa «Limpiar en Hive y responder»."))
            return

        comp = estado_comparacion()
        publicados = es.miles(n_lineas(PUBLICADO))
        if comp == "bien":
            html(es.resultado("bien", "✓ Hive da exactamente lo esperado",
                              f"P1, P2, P3 y la cuarentena coinciden, celda a celda, con el oráculo en "
                              f"Python sobre los {publicados} mensajes publicados desde el panel."))
        elif comp == "mal":
            html(es.resultado("mal", "✗ Hive no coincide con lo esperado",
                              "Mira el detalle de la comparación debajo."))
        else:
            html(es.resultado("neutro", "Sin comparación",
                              "Vacía el sistema y publica desde el panel para que sepa qué debería salir."))
        ruta = HIVE_OUT.parent / "comparacion.txt"
        if ruta.exists():
            with st.expander("Detalle de la comparación"):
                st.code(ruta.read_text(encoding="utf-8"), language=None)

        m = {str(k): bonito(v) for k, v in zip(control["metrica"], control["valor"])}
        cuar = {k.removeprefix("cuarentena_"): v for k, v in m.items() if k.startswith("cuarentena_")}
        html(es.kpis([
            ("Filas en raw", es.miles(m.get("filas_raw")), "mensajes archivados"),
            ("Pagos distintos", es.miles(m.get("pagos_distintos")), "tras deduplicar por tx_id"),
            ("Válidos", es.miles(m.get("validos")), "en pagos_limpios"),
            ("Cuarentena", es.miles(sum(int(v) for v in cuar.values())), "uno o más por regla"),
            ("Relecturas", es.miles(m.get("relecturas_consumidor")), "misma partición y offset"),
        ]))
        html(f'<p class="nota">Gasto aprobado contando los duplicados: '
             f'<b class="tabular">{m.get("importe_aprobado_sin_deduplicar", "—")} €</b> · deduplicado: '
             f'<b class="tabular">{m.get("importe_aprobado_deduplicado", "—")} €</b></p>')

        html(es.seccion("P1 · Gasto aprobado por día y categoría"))
        p1 = leer_tsv("p1_gasto_dia_categoria")
        if p1 is not None and not p1.empty:
            st.altair_chart(es.grafico(alt.Chart(p1).mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
                x=alt.X("fecha:N", title="Día (hora de Madrid)", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("importe_total_eur:Q", title="Gasto aprobado (€)", stack="zero"),
                color=alt.Color("categoria:N", title="Categoría", scale=alt.Scale(scheme="tableau10")),
                tooltip=["fecha", "categoria", "n_pagos", "importe_total_eur", "ticket_medio_eur"],
            ).properties(height=320)), use_container_width=True)
            with st.expander("Tabla y consulta"):
                st.dataframe(p1, hide_index=True, use_container_width=True)
                st.code(sql("p1_gasto_dia_categoria"), language="sql")

        html(es.seccion("P2 · Pagos por hora del día y parte online"))
        p2 = leer_tsv("p2_franja_horaria")
        if p2 is not None and not p2.empty:
            largo = pd.concat([
                pd.DataFrame({"hora": p2["hora"], "Canal": "Online", "Pagos": p2["n_online"]}),
                pd.DataFrame({"hora": p2["hora"], "Canal": "Físico", "Pagos": p2["n_pagos"] - p2["n_online"]}),
            ])
            st.altair_chart(es.grafico(alt.Chart(largo).mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
                x=alt.X("hora:O", title="Hora (Madrid)", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("Pagos:Q", title="Intentos válidos", stack="zero"),
                color=alt.Color("Canal:N", title=None,
                                scale=alt.Scale(domain=["Online", "Físico"], range=[es.AZUL, es.GRIS])),
                tooltip=["hora", "Canal", "Pagos"],
            ).properties(height=280)), use_container_width=True)
            with st.expander("Tabla y consulta"):
                st.dataframe(p2, hide_index=True, use_container_width=True)
                st.code(sql("p2_franja_horaria"), language="sql")

        html(es.seccion("P3 · Tarjetas que pagan en 2 o más países el mismo día"))
        p3 = leer_tsv("p3_tarjetas_sospechosas")
        if p3 is not None:
            if p3.empty:
                html(es.vacio("Ninguna tarjeta ha pagado en dos países el mismo día."))
            else:
                st.dataframe(p3, hide_index=True, use_container_width=True)
            with st.expander("Consulta"):
                st.code(sql("p3_tarjetas_sospechosas"), language="sql")

        html(es.seccion("Cuarentena · pagos rechazados y su motivo"))
        if cuar:
            df = pd.DataFrame({"Regla": list(cuar), "Pagos": [int(v) for v in cuar.values()]})
            st.altair_chart(es.grafico(alt.Chart(df).mark_bar(color=es.NARANJA, cornerRadiusTopRight=3,
                                                              cornerRadiusBottomRight=3).encode(
                x=alt.X("Pagos:Q", title="Pagos rechazados"),
                y=alt.Y("Regla:N", title=None, sort="-x"),
                tooltip=["Regla", "Pagos"],
            ).properties(height=40 * len(df) + 40)), use_container_width=True)
        else:
            html(es.vacio("Ningún pago en cuarentena."))
        detalle = leer_tsv("cuarentena")
        if detalle is not None and not detalle.empty:
            with st.expander("Primeros pagos en cuarentena (con su origen en Kafka)"):
                st.dataframe(detalle, hide_index=True, use_container_width=True)

    respuestas_hive()

# ------------------------------------------------------------------------------------------
# Pestaña 3: cómo funciona
# ------------------------------------------------------------------------------------------
with como:
    diagrama = PROYECTO / "docs" / "arquitectura.png"
    if diagrama.exists():
        st.image(str(diagrama), use_container_width=True)
    st.markdown("""
**Recorrido de un pago**

1. El **simulador** genera los pagos (con semilla: misma semilla, mismos datos) e inyecta
   anomalías, duplicados y tarjetas sospechosas a propósito.
2. El **productor** los publica en Kafka con la **tarjeta como clave**: los pagos de una
   tarjeta van siempre a la misma partición y mantienen su orden.
3. El **archivador** los lee por lotes, **primero los escribe en HDFS y después confirma el
   offset**. Si muere entre medias, relee: puede repetir, pero nunca pierde.
4. **Hive** deduplica por `tx_id`, aplica las reglas R1-R5 y guarda los válidos en Parquet,
   particionados por día; el resto va a cuarentena con su motivo.
5. Las respuestas se **comparan** con un cálculo independiente en Python.

**Glosario**

| Término | Qué es |
|---|---|
| Partición | Un "carril" del topic. Kafka solo garantiza el orden dentro de cada una |
| Offset | La posición de un mensaje dentro de su partición (0, 1, 2…) |
| Commit | El offset que el grupo da por procesado: "he guardado hasta aquí" |
| LAG | Offset final − offset confirmado: mensajes pendientes de archivar |
| At-least-once | Cada mensaje se archiva al menos una vez: puede repetirse, nunca perderse |

**Prueba en vivo:** 💀 mata el archivador, 📤 publica la muestra y mira cómo sube el LAG.
Luego ▶️ arráncalo: el LAG vuelve a 0 y no falta nada.
""")
