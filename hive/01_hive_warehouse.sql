-- ============================================================
-- MODULE 1 — ENTREPÔT DE DONNÉES HIVE
-- Projet : Analyse Prédictive Consommation Énergétique ASHRAE
-- Plateforme : HDP 3.0 — Apache Hive
-- ============================================================

-- Interpréteur : %jdbc(hive)

-- ============================================================
-- ÉTAPE 1 : CRÉATION DE LA BASE DE DONNÉES
-- ============================================================

-- Création de la base dédiée au projet
CREATE DATABASE IF NOT EXISTS energy_db
COMMENT 'Base de données pour l analyse prédictive de la consommation énergétique';

-- Sélection de la base
USE energy_db;

-- Vérification
SHOW DATABASES;

-- ============================================================
-- ÉTAPE 2 : CRÉATION DES TABLES EXTERNES
-- ============================================================

-- TABLE EXTERNE 1 : Relevés des compteurs
-- timestamp est un mot réservé Hive, on utilise des backticks
CREATE EXTERNAL TABLE IF NOT EXISTS ext_meter_readings (
    building_id     INT     COMMENT 'Identifiant unique du bâtiment',
    meter           INT     COMMENT 'Type de compteur (0=électricité, 1=eau chaude, 2=vapeur, 3=eau froide)',
    `timestamp`     STRING  COMMENT 'Horodatage de la mesure',
    meter_reading   DOUBLE  COMMENT 'Consommation mesurée en kWh'
)
COMMENT 'Table externe des relevés horaires bruts des compteurs 2016-2018'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/energy_project/raw/meter_readings/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- TABLE EXTERNE 2 : Métadonnées des bâtiments
CREATE EXTERNAL TABLE IF NOT EXISTS ext_building_metadata (
    site_id         INT     COMMENT 'Identifiant du site géographique',
    building_id     INT     COMMENT 'Identifiant unique du bâtiment',
    primary_use     STRING  COMMENT 'Usage principal du bâtiment',
    square_feet     INT     COMMENT 'Surface en pieds carrés',
    year_built      INT     COMMENT 'Année de construction',
    floor_count     INT     COMMENT 'Nombre d étages'
)
COMMENT 'Table externe des caractéristiques des bâtiments'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/energy_project/raw/building_metadata/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- TABLE EXTERNE 3 : Données météorologiques
CREATE EXTERNAL TABLE IF NOT EXISTS ext_weather (
    site_id             INT     COMMENT 'Identifiant du site géographique',
    `timestamp`         STRING  COMMENT 'Horodatage de la mesure météo',
    air_temperature     DOUBLE  COMMENT 'Température de l air en °C',
    cloud_coverage      DOUBLE  COMMENT 'Couverture nuageuse (0-9)',
    dew_temperature     DOUBLE  COMMENT 'Point de rosée en °C',
    precip_depth_1_hr   DOUBLE  COMMENT 'Précipitations en mm',
    sea_level_pressure  DOUBLE  COMMENT 'Pression atmosphérique en hPa',
    wind_direction      DOUBLE  COMMENT 'Direction du vent en degrés',
    wind_speed          DOUBLE  COMMENT 'Vitesse du vent en m/s'
)
COMMENT 'Table externe des données météorologiques par site'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/energy_project/raw/weather/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Vérification des tables créées
SHOW TABLES;

-- ============================================================
-- ÉTAPE 3 : VÉRIFICATION DES DONNÉES
-- ============================================================

-- Vérifier le nombre de lignes dans chaque table externe
SELECT COUNT(*) AS nb_meter_readings FROM ext_meter_readings;
SELECT COUNT(*) AS nb_batiments FROM ext_building_metadata;
SELECT COUNT(*) AS nb_weather FROM ext_weather;

-- ============================================================
-- ÉTAPE 4 : TABLE MANAGÉE PARTITIONNÉE AU FORMAT PARQUET
-- ============================================================

-- TABLE MANAGÉE PARTITIONNÉE AU FORMAT PARQUET
-- Parquet = stockage colonaire, compression SNAPPY
-- Partitionnement par primary_use pour accélérer les requêtes
CREATE TABLE IF NOT EXISTS fact_energy_consumption (
    building_id         INT     COMMENT 'Identifiant unique du bâtiment',
    meter               INT     COMMENT 'Type de compteur (0=électricité)',
    reading_timestamp   STRING  COMMENT 'Horodatage de la mesure',
    meter_reading       DOUBLE  COMMENT 'Consommation en kWh',
    site_id             INT     COMMENT 'Identifiant du site géographique',
    year_built          INT     COMMENT 'Année de construction du bâtiment',
    square_feet         INT     COMMENT 'Surface du bâtiment en pieds carrés',
    floor_count         INT     COMMENT 'Nombre d étages',
    air_temperature     DOUBLE  COMMENT 'Température de l air en °C',
    wind_speed          DOUBLE  COMMENT 'Vitesse du vent en m/s',
    precip_depth_1_hr   DOUBLE  COMMENT 'Précipitations en mm'
)
COMMENT 'Table de faits managée partitionnée par type de bâtiment au format Parquet'
PARTITIONED BY (primary_use STRING COMMENT 'Usage principal du bâtiment')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'parquet.block.size'='134217728'
);

-- Vérification structure
SHOW TABLES;
DESCRIBE FORMATTED fact_energy_consumption;

-- ============================================================
-- ÉTAPE 5 : CHARGEMENT DES DONNÉES DANS LA TABLE PARQUET
-- ============================================================

-- Configuration pour le partitionnement dynamique
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000;
SET hive.tez.container.size=2048;
SET hive.tez.java.opts=-Xmx1638m;
SET hive.auto.convert.join=false;
SET hive.mapjoin.smalltable.filesize=500000;
SET hive.mapjoin.optimized.hashtable=false;

-- IMPORTANT OPTIMISATION : uniquement les compteurs électricité (meter=0)
INSERT INTO TABLE fact_energy_consumption
PARTITION (primary_use)
SELECT
    m.building_id,
    m.meter,
    m.`timestamp` AS reading_timestamp,
    m.meter_reading,
    b.site_id,
    b.year_built,
    b.square_feet,
    b.floor_count,
    w.air_temperature,
    w.wind_speed,
    w.precip_depth_1_hr,
    b.primary_use
FROM ext_meter_readings m
JOIN ext_building_metadata b ON (m.building_id = b.building_id)
LEFT JOIN ext_weather w ON (b.site_id = w.site_id AND m.`timestamp` = w.`timestamp`)
WHERE m.meter = 0
AND m.meter_reading IS NOT NULL
AND m.meter_reading >= 0;

-- Vérification du chargement
SELECT COUNT(*) AS nb_enregistrements FROM fact_energy_consumption;
SELECT primary_use, COUNT(*) AS nb_enregistrements
FROM fact_energy_consumption
GROUP BY primary_use
ORDER BY nb_enregistrements DESC;

-- ============================================================
-- ÉTAPE 6 : VUES MÉTIER
-- ============================================================

-- VUE MÉTIER 1 : Consommation moyenne par type de bâtiment et par mois
CREATE VIEW IF NOT EXISTS vue_conso_par_type AS
SELECT
    primary_use,
    SUBSTR(reading_timestamp, 1, 7)     AS mois,
    ROUND(AVG(meter_reading), 2)        AS conso_moyenne,
    COUNT(*)                            AS nb_mesures
FROM fact_energy_consumption
WHERE meter_reading >= 0
GROUP BY primary_use, SUBSTR(reading_timestamp, 1, 7)
ORDER BY mois, conso_moyenne DESC;

-- VUE MÉTIER 2 : Détection des bâtiments anormaux (>2x la moyenne)
CREATE VIEW IF NOT EXISTS vue_anomalies_batiments AS
SELECT
    f.building_id,
    f.primary_use,
    ROUND(AVG(f.meter_reading), 2)      AS conso_moyenne_batiment,
    ROUND(m.conso_moyenne_type, 2)      AS conso_moyenne_type,
    ROUND(AVG(f.meter_reading) / m.conso_moyenne_type, 2) AS ratio
FROM fact_energy_consumption f
JOIN (
    SELECT primary_use, AVG(meter_reading) AS conso_moyenne_type
    FROM fact_energy_consumption
    GROUP BY primary_use
) m ON f.primary_use = m.primary_use
GROUP BY f.building_id, f.primary_use, m.conso_moyenne_type
HAVING AVG(f.meter_reading) > 2 * m.conso_moyenne_type
ORDER BY ratio DESC;

-- VUE MÉTIER 3 : Pics mensuels par site
CREATE VIEW IF NOT EXISTS vue_pics_mensuels AS
SELECT
    site_id,
    SUBSTR(reading_timestamp, 1, 7)     AS mois,
    ROUND(MAX(meter_reading), 2)        AS pic_mensuel,
    ROUND(AVG(meter_reading), 2)        AS conso_moyenne,
    COUNT(*)                            AS nb_mesures
FROM fact_energy_consumption
GROUP BY site_id, SUBSTR(reading_timestamp, 1, 7)
ORDER BY mois, pic_mensuel DESC;

-- Vérification des vues
SHOW VIEWS;

-- Test des vues
SELECT * FROM vue_conso_par_type LIMIT 10;
SELECT * FROM vue_anomalies_batiments LIMIT 10;
SELECT * FROM vue_pics_mensuels LIMIT 10;

-- ============================================================
-- ÉTAPE 7 : VUE MATÉRIALISÉE + REBUILD
-- ============================================================

-- VUE MATÉRIALISÉE : Résumé mensuel pré-calculé
-- Permet des requêtes analytiques rapides sans recalcul
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_resume_mensuel
DISABLE REWRITE
AS
SELECT
    primary_use,
    SUBSTR(reading_timestamp, 1, 7)     AS mois,
    ROUND(AVG(meter_reading), 2)        AS conso_moyenne,
    ROUND(SUM(meter_reading), 2)        AS conso_totale,
    ROUND(MAX(meter_reading), 2)        AS conso_max,
    COUNT(*)                            AS nb_mesures
FROM fact_energy_consumption
GROUP BY primary_use, SUBSTR(reading_timestamp, 1, 7);

-- Rafraîchissement de la vue matérialisée
ALTER MATERIALIZED VIEW mv_resume_mensuel REBUILD;

-- Vérification
SELECT * FROM mv_resume_mensuel LIMIT 10;

-- ============================================================
-- ÉTAPE 8 : ANALYSE DES PERFORMANCES VIA EXPLAIN
-- ============================================================

-- Analyse du plan d'exécution d'une requête analytique
EXPLAIN
SELECT
    primary_use,
    SUBSTR(reading_timestamp, 1, 7)     AS mois,
    ROUND(AVG(meter_reading), 2)        AS conso_moyenne,
    COUNT(*)                            AS nb_mesures
FROM fact_energy_consumption
WHERE meter_reading >= 0
GROUP BY primary_use, SUBSTR(reading_timestamp, 1, 7)
ORDER BY mois, conso_moyenne DESC;
