// ============================================================
// MODULE 2 — PIPELINE DE TRAITEMENT DISTRIBUÉ SPARK
// Projet : Analyse Prédictive Consommation Énergétique ASHRAE
// Plateforme : HDP 3.0 — Apache Spark 2.3.1
// Interpréteur Zeppelin : %spark
// ============================================================

// ============================================================
// CELLULE 1 : VÉRIFICATION DE L'ENVIRONNEMENT
// ============================================================

// Vérification de la version Spark
println("Spark version : " + sc.version)
println("Application ID : " + sc.applicationId)

// ============================================================
// CELLULE 2 : CHARGEMENT DES 3 DATASETS DEPUIS HDFS
// ============================================================

// Chargement des relevés de compteurs (20M+ lignes)
val dfMeter = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("hdfs:///user/hive/warehouse/energy_project/raw/meter_readings/train.csv")

// Chargement des métadonnées des bâtiments
val dfBuilding = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("hdfs:///user/hive/warehouse/energy_project/raw/building_metadata/building_metadata.csv")

// Chargement des données météorologiques
val dfWeather = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("hdfs:///user/hive/warehouse/energy_project/raw/weather/weather_train.csv")

// Vérification des schémas
println("=== SCHEMA METER READINGS ===")
dfMeter.printSchema()

println("=== SCHEMA BUILDING METADATA ===")
dfBuilding.printSchema()

println("=== SCHEMA WEATHER ===")
dfWeather.printSchema()

// ============================================================
// CELLULE 3 : STATISTIQUES DE BASE
// ============================================================

// Nombre d'enregistrements par dataset
println("=== NOMBRE D'ENREGISTREMENTS ===")
println(s"Meter readings : ${dfMeter.count()}")
println(s"Building metadata : ${dfBuilding.count()}")
println(s"Weather : ${dfWeather.count()}")

// Aperçu des données
println("\n=== APERÇU METER READINGS ===")
dfMeter.show(5, false)

println("\n=== APERÇU BUILDING METADATA ===")
dfBuilding.show(5, false)

println("\n=== APERÇU WEATHER ===")
dfWeather.show(5, false)

// ============================================================
// CELLULE 4 : NETTOYAGE DES DONNÉES
// ============================================================

// Filtrer uniquement l'électricité (meter=0)
// Exclure les valeurs négatives et nulles
val dfMeterClean = dfMeter
  .filter("meter = 0")
  .filter("meter_reading >= 0")
  .filter("meter_reading IS NOT NULL")

// Nettoyage building metadata
// Remplacer floor_count null par la médiane (1)
// Remplacer year_built null par 0
val dfBuildingClean = dfBuilding
  .na.fill(1.0, Seq("floor_count"))
  .na.fill(0, Seq("year_built"))

// Nettoyage météo
// Remplacer les valeurs nulles et -1 par 0
val dfWeatherClean = dfWeather
  .na.fill(0.0, Seq("precip_depth_1_hr", "cloud_coverage", "wind_direction"))
  .filter("air_temperature IS NOT NULL")

// Vérification après nettoyage
println("=== APRÈS NETTOYAGE ===")
println(s"Meter readings : ${dfMeterClean.count()}")
println(s"Building metadata : ${dfBuildingClean.count()}")
println(s"Weather : ${dfWeatherClean.count()}")

// Vérifier les nulls restants dans meter
println("\n=== NULLS RESTANTS METER ===")
dfMeterClean.select(dfMeterClean.columns.map(c =>
  org.apache.spark.sql.functions.sum(
    org.apache.spark.sql.functions.col(c).isNull.cast("int")
  ).alias(c)
): _*).show()

// ============================================================
// CELLULE 5 : JOINTURES DES 3 DATASETS
// ============================================================

// JOINTURES DES 3 DATASETS
// Jointure meter + building (inner join sur building_id)
val dfJoined = dfMeterClean.join(dfBuildingClean, Seq("building_id"), "inner")

// Jointure avec météo sur site_id + timestamp (left join)
// Left join pour conserver tous les relevés même sans données météo
val dfFull = dfJoined.join(dfWeatherClean, Seq("site_id", "timestamp"), "left")

// Vérification
println("=== DATASET FINAL APRÈS JOINTURES ===")
println(s"Nombre de lignes : ${dfFull.count()}")
println(s"Nombre de colonnes : ${dfFull.columns.length}")

println("\n=== SCHÉMA FINAL ===")
dfFull.printSchema()

println("\n=== APERÇU ===")
dfFull.show(5, false)

// ============================================================
// CELLULE 6 : STRATÉGIE DE PERSISTANCE — BENCHMARK
// ============================================================

import org.apache.spark.storage.StorageLevel

// TEST 1 : SANS cache
val t1Start = System.currentTimeMillis()
val countSansCache = dfFull.count()
val t1End = System.currentTimeMillis()
val tempsSansCache = t1End - t1Start

println(s"=== SANS CACHE ===")
println(s"Résultat : $countSansCache lignes")
println(s"Temps : ${tempsSansCache} ms")

// Mise en cache du DataFrame (MEMORY_AND_DISK pour éviter OOM)
dfFull.persist(StorageLevel.MEMORY_AND_DISK)

// Forcer le cache en faisant une action
dfFull.count()

// TEST 2 : AVEC cache
val t2Start = System.currentTimeMillis()
val countAvecCache = dfFull.count()
val t2End = System.currentTimeMillis()
val tempsAvecCache = t2End - t2Start

println(s"\n=== AVEC CACHE ===")
println(s"Résultat : $countAvecCache lignes")
println(s"Temps : ${tempsAvecCache} ms")

// Résumé du benchmark
println(s"\n=== BENCHMARK PERSISTANCE ===")
println(s"Sans cache : ${tempsSansCache} ms")
println(s"Avec cache : ${tempsAvecCache} ms")
println(s"Gain       : ${tempsSansCache - tempsAvecCache} ms")
println(s"Ratio      : ${tempsSansCache.toDouble / tempsAvecCache.toDouble} x plus rapide")

// ============================================================
// CELLULE 7 : VÉRIFICATION DE LA PERSISTANCE
// ============================================================

// Afficher les informations sur le cache
println("=== INFORMATIONS DU CACHE ===")
println(s"DataFrame en cache : ${dfFull.storageLevel}")
println(s"Storage Level : MEMORY_AND_DISK")

// ============================================================
// CELLULE 8 : STATISTIQUES DESCRIPTIVES
// ============================================================

// Statistiques descriptives du dataset final
println("\n=== STATISTIQUES DESCRIPTIVES ===")
dfFull.select(
  "meter_reading",
  "square_feet",
  "air_temperature",
  "wind_speed",
  "floor_count"
).describe().show(false)
