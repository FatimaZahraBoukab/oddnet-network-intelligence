{{
    config(
        post_hook="COPY (SELECT * FROM {{ this }}) TO 's3://gold/resume_par_client.parquet' (FORMAT PARQUET);"
    )
}}

-- Ce modele lit les donnees Silver (deja agregees par Spark)
-- et calcule un resume par client, pret pour les rapports.

SELECT
    client_id,
    ROUND(AVG(latence_moyenne), 2)       AS latence_moyenne_globale,
    ROUND(AVG(debit_moyen), 2)           AS debit_moyen_global,
    ROUND(AVG(disponibilite_moyenne), 2) AS disponibilite_moyenne_globale,
    SUM(nb_anomalies)                     AS total_anomalies,
    SUM(nb_mesures)                       AS total_mesures,
    CURRENT_TIMESTAMP                     AS date_calcul

FROM read_parquet('s3://silver/network-kpis-agrege/*.parquet')

GROUP BY client_id