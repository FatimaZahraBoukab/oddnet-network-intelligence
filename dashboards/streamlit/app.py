import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import os
from datetime import datetime



# ============================================================
# CONFIGURATION DE LA PAGE
# ============================================================
st.set_page_config(
    page_title="ODDnet | Intelligence Réseau",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# CONSTANTES DE STYLE
# ============================================================
COULEUR_PRIMAIRE = "#3B82F6"
COULEUR_SUCCES = "#10B981"
COULEUR_ALERTE = "#EF4444"
PALETTE_GRAPHIQUES = ["#3B82F6", "#10B981", "#F59E0B", "#8B5CF6", "#EC4899", "#06B6D4"]

def theme_graphique(fig, hauteur=380):
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#F1F5F9", size=12),
        legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=-0.15),
        margin=dict(l=10, r=10, t=40, b=10),
        height=hauteur,
        title_font_size=14
    )
    fig.update_xaxes(gridcolor="#1E3A5F", showline=False)
    fig.update_yaxes(gridcolor="#1E3A5F", showline=False)
    return fig

def theme_facettes(fig, hauteur=600):
    """Theme special pour les graphiques en petits multiples (facet)."""
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#F1F5F9", size=12),
        showlegend=False,
        margin=dict(l=10, r=10, t=30, b=10),
        height=hauteur
    )
    fig.update_xaxes(gridcolor="#1E3A5F", showline=False)
    fig.update_yaxes(gridcolor="#1E3A5F", showline=False, matches=None)
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1], font=dict(size=12)))
    return fig

# ============================================================
# CONNEXION BASE DE DONNÉES
# ============================================================
@st.cache_resource
def get_connection():
    return create_engine("postgresql://dashboard:dashboard@postgres-dashboard:5432/oddnet_dashboard")

@st.cache_data(ttl=60)
def charger(table):
    return pd.read_sql(f"SELECT * FROM {table}", get_connection())

df_gold = charger("gold_resume_client")
df_silver = charger("silver_kpis")
df_ml = charger("ml_anomalies")
df_pred = charger("ml_predictions")

# ============================================================
# SIDEBAR — LOGO, FILTRES, NAVIGATION
# ============================================================
with st.sidebar:
    logo_path = "assets/logo_oddnet.png"
    if os.path.exists(logo_path):
        st.image(logo_path, width=160)

    st.markdown("---")

    st.markdown("##### 🔍 Filtres")
    clients_disponibles = ["Tous les clients"] + sorted(df_gold["client_id"].unique().tolist())
    client_selectionne = st.selectbox("Client", clients_disponibles, label_visibility="collapsed")

    st.markdown("---")

    page = option_menu(
        menu_title=None,
        options=["Vue d'ensemble", "Supervision réseau", "Détection d'anomalies", "Maintenance prédictive"],
        icons=["speedometer2", "activity", "exclamation-triangle", "graph-up-arrow"],
        default_index=0,
        styles={
            "container": {"padding": "0", "background-color": "transparent"},
            "icon": {"color": "#94A3B8", "font-size": "16px"},
            "nav-link": {
                "font-size": "14px",
                "text-align": "left",
                "margin": "3px 0",
                "padding": "10px 12px",
                "border-radius": "8px",
                "color": "#F1F5F9",
                "background-color": "transparent"
            },
            "nav-link-selected": {
                "background-color": "#3B82F6",
                "color": "#FFFFFF",
                "font-weight": "600"
            }
        }
    )

    
    
    

# Appliquer le filtre client
if client_selectionne != "Tous les clients":
    df_silver_f = df_silver[df_silver["client_id"] == client_selectionne]
    df_ml_f = df_ml[df_ml["client_id"] == client_selectionne]
    df_pred_f = df_pred[df_pred["client_id"] == client_selectionne]
    df_gold_f = df_gold[df_gold["client_id"] == client_selectionne]
else:
    df_silver_f = df_silver
    df_ml_f = df_ml
    df_pred_f = df_pred
    df_gold_f = df_gold

# ============================================================
# EN-TÊTE (commun a toutes les pages)
# ============================================================
col_logo, col_titre, col_date = st.columns([1, 4, 1.3])

with col_logo:
    logo_path = "assets/logo_oddnet.png"
    if os.path.exists(logo_path):
        st.image(logo_path, width=130)

with col_titre:
    st.markdown("## Plateforme d'Intelligence Réseau")
    st.markdown(
        f"<span style='color:{COULEUR_PRIMAIRE}; font-weight:600; letter-spacing:2px; font-size:0.8rem;'>"
        f"ALWAYS A STEP AHEAD</span>",
        unsafe_allow_html=True
    )

with col_date:
    st.markdown(
        f"<div style='text-align:right; padding-top:10px; color:#94A3B8; font-size:0.85rem;'>"
        f"Dernière mise à jour<br><b style='color:#F1F5F9;'>{datetime.now().strftime('%d/%m/%Y %H:%M')}</b></div>",
        unsafe_allow_html=True
    )

st.markdown(f"<h3 style='color:#94A3B8; font-weight:400; margin-top:10px;'>{page}</h3>", unsafe_allow_html=True)
st.markdown("<hr style='border-color:#1E3A5F; margin-top:0px;'>", unsafe_allow_html=True)

# ============================================================
# PAGE 1 — VUE D'ENSEMBLE
# ============================================================
if page == "Vue d'ensemble":

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latence moyenne", f"{df_gold_f['latence_moyenne_globale'].mean():.1f} ms")
    c2.metric("Débit moyen", f"{df_gold_f['debit_moyen_global'].mean():.1f} Mbps")
    c3.metric("Disponibilité", f"{df_gold_f['disponibilite_moyenne_globale'].mean():.1f} %")
    c4.metric("Anomalies cumulées", f"{df_gold_f['total_anomalies'].sum():.0f}")

    st.write("")
    col_gauche, col_droite = st.columns([3, 2])

    with col_gauche:
        st.markdown("#### Résumé par client")
        st.dataframe(
            df_gold_f.rename(columns={
                "client_id": "Client",
                "latence_moyenne_globale": "Latence (ms)",
                "debit_moyen_global": "Débit (Mbps)",
                "disponibilite_moyenne_globale": "Disponibilité (%)",
                "total_anomalies": "Anomalies",
                "total_mesures": "Mesures"
            })[["Client", "Latence (ms)", "Débit (Mbps)", "Disponibilité (%)", "Anomalies", "Mesures"]],
            use_container_width=True, hide_index=True, height=280
        )

    with col_droite:
        st.markdown("#### Répartition du débit")
        fig = px.pie(
            df_gold_f, names="client_id", values="debit_moyen_global",
            hole=0.55, color_discrete_sequence=PALETTE_GRAPHIQUES
        )
        fig.update_traces(textposition="outside", textinfo="percent")
        st.plotly_chart(theme_graphique(fig, 280), use_container_width=True)

    st.markdown("#### Disponibilité par client")
    fig_dispo_bar = px.bar(
        df_gold_f.sort_values("disponibilite_moyenne_globale"),
        x="disponibilite_moyenne_globale", y="client_id", orientation="h",
        color="disponibilite_moyenne_globale",
        color_continuous_scale=["#EF4444", "#F59E0B", "#10B981"],
        range_x=[80, 100]
    )
    fig_dispo_bar.update_layout(coloraxis_showscale=False, yaxis_title="", xaxis_title="Disponibilité (%)")
    st.plotly_chart(theme_graphique(fig_dispo_bar, 260), use_container_width=True)

# ============================================================
# PAGE 2 — SUPERVISION RÉSEAU (Silver) — GRAPHIQUES EN FACETTES
# ============================================================
elif page == "Supervision réseau":

    metrique = st.radio(
        "Métrique à afficher",
        ["Latence (ms)", "Débit (Mbps)", "Disponibilité (%)"],
        horizontal=True, label_visibility="collapsed"
    )

    colonne_map = {
        "Latence (ms)": ("latence_moyenne", "Latence (ms)"),
        "Débit (Mbps)": ("debit_moyen", "Débit (Mbps)"),
        "Disponibilité (%)": ("disponibilite_moyenne", "Disponibilité (%)")
    }
    colonne, label_axe = colonne_map[metrique]

    st.caption("Un graphique distinct par équipement, pour une lecture claire sans lignes superposées")

    fig_evo = px.line(
        df_silver_f.sort_values("timestamp"),
        x="timestamp", y=colonne,
        facet_row="equipement_id",
        color="equipement_id",
        color_discrete_sequence=PALETTE_GRAPHIQUES
    )
    fig_evo.update_traces(line=dict(width=2))
    fig_evo.update_yaxes(title_text="")
    fig_evo.update_xaxes(title_text="")
    st.plotly_chart(theme_facettes(fig_evo, 720), use_container_width=True)

    st.write("")
    st.markdown("#### Statistiques par équipement")
    stats_equip = df_silver_f.groupby("equipement_id").agg(
        latence_moyenne=("latence_moyenne", "mean"),
        debit_moyen=("debit_moyen", "mean"),
        disponibilite_moyenne=("disponibilite_moyenne", "mean"),
        nb_anomalies=("nb_anomalies", "sum")
    ).reset_index().round(1)

    st.dataframe(
        stats_equip.rename(columns={
            "equipement_id": "Équipement", "latence_moyenne": "Latence (ms)",
            "debit_moyen": "Débit (Mbps)", "disponibilite_moyenne": "Disponibilité (%)",
            "nb_anomalies": "Anomalies"
        }),
        use_container_width=True, hide_index=True
    )

# ============================================================
# PAGE 3 — DÉTECTION D'ANOMALIES (Isolation Forest)
# ============================================================
elif page == "Détection d'anomalies":

    nb_anomalies = int(df_ml_f["est_anomalie_predite"].sum())
    taux = (nb_anomalies / len(df_ml_f) * 100) if len(df_ml_f) > 0 else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Anomalies détectées", nb_anomalies)
    c2.metric("Taux d'anomalies", f"{taux:.1f} %")
    c3.metric("Fenêtres analysées", len(df_ml_f))

    st.write("")
    col_g, col_d = st.columns([2, 3])

    with col_g:
        st.markdown("##### Anomalies par équipement")
        par_equip = df_ml_f[df_ml_f["est_anomalie_predite"]].groupby(
            "equipement_id"
        ).size().reset_index(name="nombre").sort_values("nombre")

        fig_bar = px.bar(
            par_equip, x="nombre", y="equipement_id", orientation="h",
            color_discrete_sequence=[COULEUR_ALERTE]
        )
        fig_bar.update_layout(yaxis_title="", xaxis_title="")
        st.plotly_chart(theme_graphique(fig_bar, 320), use_container_width=True)

    with col_d:
        st.markdown("##### Détail des anomalies détectées")
        detail = df_ml_f[df_ml_f["est_anomalie_predite"]][[
            "client_id", "equipement_id", "latence_moyenne",
            "debit_moyen", "disponibilite_moyenne", "nb_anomalies"
        ]].rename(columns={
            "client_id": "Client", "equipement_id": "Équipement",
            "latence_moyenne": "Latence", "debit_moyen": "Débit",
            "disponibilite_moyenne": "Dispo.", "nb_anomalies": "Nb réel"
        }).round(1)
        st.dataframe(detail, use_container_width=True, hide_index=True, height=320)

    st.markdown("##### Nuage de points — Latence vs Débit")
    fig_scatter = px.scatter(
        df_ml_f, x="latence_moyenne", y="debit_moyen",
        color="est_anomalie_predite",
        color_discrete_map={True: COULEUR_ALERTE, False: COULEUR_SUCCES},
        labels={"est_anomalie_predite": "Anomalie", "latence_moyenne": "Latence (ms)", "debit_moyen": "Débit (Mbps)"},
        opacity=0.7
    )
    st.plotly_chart(theme_graphique(fig_scatter, 380), use_container_width=True)

# ============================================================
# PAGE 4 — MAINTENANCE PRÉDICTIVE (Prophet) — GRAPHIQUES EN FACETTES
# ============================================================
elif page == "Maintenance prédictive":

    st.caption("Projection sur les 30 prochaines minutes, par équipement")

    nb_alertes = int(df_pred_f["alerte_globale"].sum())
    if nb_alertes > 0:
        st.error(f"⚠️  **{nb_alertes} alerte(s)** prévue(s) dans les 30 prochaines minutes")
    else:
        st.success("✅  Aucune alerte prévue — fonctionnement normal anticipé")

    st.write("")

    metrique_pred = st.radio(
        "Métrique prédite",
        ["Latence", "Débit", "Disponibilité"],
        horizontal=True, label_visibility="collapsed"
    )

    colonne_pred_map = {
        "Latence": "latence_predite",
        "Débit": "debit_predit",
        "Disponibilité": "disponibilite_predite"
    }
    colonne_pred = colonne_pred_map[metrique_pred]

    fig_pred = px.line(
        df_pred_f.sort_values("timestamp_predit"),
        x="timestamp_predit", y=colonne_pred,
        facet_row="equipement_id",
        color="equipement_id",
        color_discrete_sequence=PALETTE_GRAPHIQUES
    )
    fig_pred.update_traces(line=dict(width=2))
    fig_pred.update_yaxes(title_text="")
    fig_pred.update_xaxes(title_text="")
    st.plotly_chart(theme_facettes(fig_pred, 720), use_container_width=True)

# ============================================================
# PIED DE PAGE
# ============================================================
st.markdown("<hr style='border-color:#1E3A5F;'>", unsafe_allow_html=True)
st.markdown(
    "<div style='text-align:center; color:#64748B; font-size:0.8rem;'>"
    "© 2026 ODDnet SARL — Technopark, Casablanca, Maroc &nbsp; "
    "</div>",
    unsafe_allow_html=True
)