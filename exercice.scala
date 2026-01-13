// Indique que l'on souhaite utiliser scala version 3.7.4.
//> using scala "3.7.4"

// Indique que l'on souhaite utiliser la version 21 de JAVA.
//> using jvm "21"

// Indique que l'on souhaite utiliser spark SQL hors dans mon cas il est interdit d'utiliser le SQL.
//> using dep "org.apache.spark:spark-sql_2.13:4.1.1"

// Import des sparksession (pour lancer les sessions spark)
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * TP Scala & Spark - Analyse de transactions bancaires et détection de fraudes
 * 
 * Objectifs métier :
 * - Analyser les transactions bancaires pour comprendre les patterns de consommation
 * - Détecter les comportements suspects pouvant indiquer une fraude
 * - Identifier les anomalies dans l'utilisation des cartes bancaires
 */

/**
 *  Documentation technique :
 * .agg() : Aggregates
*/

@main def run(): Unit =
  
  // ================= INITIALISATION SPARK =================
  // Création d'une SparkSession : point d'entrée pour toutes les opérations Spark
  // master("local[*]") : exécution locale avec tous les cœurs CPU disponibles
  val spark = SparkSession.builder()
    .appName("TP Scala & Spark")
    .master("local[*]")
    .getOrCreate()
  
  // Réduire le niveau de log pour ne voir que les erreurs critiques
  // Améliore la lisibilité des résultats en production
  // n'affiche que les erreurs critiques car sinon ca spam dans la console
  spark.sparkContext.setLogLevel("ERROR")

  // Import des conversions implicites pour manipuler les DataFrames
  // Permet d'utiliser $"colonne" au lieu de col("colonne")
  // permet de faire des simplifications lors de l'appel/utilisation des colonnes des fichiers CSV
  import spark.implicits._

  try
    // ================= DÉFINITION DES CHEMINS DE DONNÉES =================
    val cardsPath        = "sources/cards_data.csv"        // Données des cartes bancaires
    val transactionsPath = "sources/transactions_data.csv" // Historique des transactions
    val usersPath        = "sources/users_data.csv"        // Profils des utilisateurs
    val mccPath          = "sources/mcc_codes.json"        // Merchant Category Codes (classification des commerçants)
    val fraudLabelsPath  = "sources/train_fraud_labels.json" // Labels de fraude pour l'apprentissage

    // ================= CHARGEMENT DES DONNÉES CSV =================
    // Métier : Les cartes contiennent les informations sur les moyens de paiement
    // (numéro, type, limite de crédit, historique d'utilisation)
    // Technique : inferSchema analyse automatiquement les types de données
    // Ouverture en lecture des fichiers CSV
    val cardsDF = spark.read
      .option("header", "true")      // Première ligne = noms de colonnes
      .option("inferSchema", "true")  // Détection automatique des types (Int, String, Double, etc.)
      .csv(cardsPath)

    // Métier : Les transactions représentent l'activité financière
    // Chaque ligne = un paiement avec montant, date, lieu, commerçant
    // Technique : Spark lit le CSV de manière lazy (évaluation paresseuse)
    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(transactionsPath)

    // Métier : Les utilisateurs contiennent le profil démographique et financier
    // (âge, revenus, score de crédit, localisation)
    // Utile pour contextualiser les patterns de dépense
    val usersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(usersPath)

    // ================= FONCTION UTILITAIRE D'AFFICHAGE =================
    // Technique : Helper pour afficher proprement les DataFrames
    // printSchema() : affiche la structure (noms et types des colonnes)
    // show() : affiche les premières lignes de données
    def showDF(title: String, df: org.apache.spark.sql.DataFrame): Unit =
      println("\n" + "=" * 60)
      println(title)
      println("=" * 60)
      df.printSchema()                    // Structure des données
      df.show(10, truncate = false)       // 10 premières lignes, texte non tronqué

    // Exploration initiale des données pour valider la structure
    showDF("CARDS DATA", cardsDF)
    showDF("TRANSACTIONS DATA", transactionsDF)
    showDF("USERS DATA", usersDF)

    // ================= CHARGEMENT DES DONNÉES JSON =================
    // Métier : MCC (Merchant Category Code) = classification standardisée des commerçants
    // Permet de catégoriser les transactions (restauration, carburant, hôtels, etc.)
    // Utilisé pour l'analyse des habitudes de consommation et la détection de fraude
    
    // Technique : Le JSON est un objet unique, on le lit en texte puis on le parse
    // 1. Lecture ligne par ligne puis concaténation en une seule chaîne JSON
    val mccJsonStringDF = spark.read.text(mccPath)
      .agg(concat_ws("", collect_list(col("value"))).as("json"))

    // 2. Définition du schéma : Map<String, String> (code MCC -> catégorie)
    val mapSchema = MapType(StringType, StringType)

    // 3. Parsing du JSON et explosion de la map en lignes individuelles
    // from_json : parse la chaîne JSON selon le schéma défini
    // explode : transforme une map en plusieurs lignes (1 par paire clé-valeur)
    val mccDF = mccJsonStringDF
      .select(from_json(col("json"), mapSchema).as("mcc_map"))
      .selectExpr("explode(mcc_map) as (mcc, merchant_category)")

    // Métier : Labels de fraude pour l'entraînement de modèles de ML (non utilisé ici)
    // Technique : multiLine permet de lire un JSON avec retours à la ligne
    val fraudLabelsDF = spark.read
      .option("multiLine", "true")
      .json(fraudLabelsPath)

    showDF("MCC CODES", mccDF)
    //showDF("FRAUD LABELS", fraudLabelsDF)  // Commenté : volumétrie importante

    // ================= VOLUMÉTRIE DES DONNÉES =================
    // Métier : Vue d'ensemble du volume d'activité pour dimensionner les analyses
    // Permet d'identifier la taille du dataset et la couverture des données
    
    // Technique : count() déclenche une action Spark (lecture effective des données)
    // distinct() élimine les doublons avant le comptage
    val totalTransactions = transactionsDF.count()
    val totalUsers        = usersDF.select("id").distinct().count()
    val totalCards        = cardsDF.select("id").distinct().count()
    val totalMerchants    = transactionsDF.select("merchant_id").distinct().count()

    println("\n--- VOLUMÉTRIE ---")
    println(s"Transactions : $totalTransactions")
    println(s"Clients uniques : $totalUsers")
    println(s"Cartes uniques : $totalCards")
    println(s"Commerçants uniques : $totalMerchants")

    // ================= QUALITÉ DES DONNÉES =================
    // Métier : Identifier les colonnes avec données manquantes
    // Important pour évaluer la fiabilité des analyses (ex: merchant_state souvent null pour transactions online)
    
    // Technique : Itération sur toutes les colonnes avec comptage des valeurs nulles
    // map() : transformation fonctionnelle (1 colonne -> 1 comptage)
    val nullCounts = transactionsDF.columns.map { c =>
      transactionsDF.filter(col(c).isNull).count()
    }

    println("\n--- COLONNES AVEC VALEURS NULL ---")
    transactionsDF.columns.zip(nullCounts).foreach {
      case (c, n) => println(s"$c : $n valeurs nulles")
    }

    // ================= NETTOYAGE DES MONTANTS =================
    // Métier : Préparation des données pour l'analyse quantitative
    // Les montants sont stockés avec "$" et doivent être convertis en nombres
    
    // Technique : regexp_replace supprime le symbole "$"
    // cast("double") convertit en nombre décimal (retourne null si échec)
    // withColumn crée une nouvelle colonne sans modifier l'originale (immutabilité)
    val txWithAmount = transactionsDF.withColumn(
      "amount_num",
      regexp_replace(col("amount"), "\\$", "").cast("double")
    )

    // Métier : Détection des montants négatifs (remboursements ou erreurs)
    // Les montants <= 0 peuvent indiquer des remboursements ou des anomalies
    val negativeAmounts = txWithAmount.filter(
      col("amount_num").isNotNull && col("amount_num") <= 0
    )
    println(s"\nTransactions avec montant <= 0 : ${negativeAmounts.count()}")

    // Métier : Transactions sans catégorie de commerçant
    // Peut indiquer des données incomplètes ou des problèmes de saisie
    val withoutMcc = txWithAmount.filter(col("mcc").isNull)
    println(s"Transactions sans MCC : ${withoutMcc.count()}")

    // ================= STATISTIQUES DESCRIPTIVES DES MONTANTS =================
    // Métier : Comprendre la distribution des montants de transactions
    // - Moyenne : tendance centrale (attention aux outliers)
    // - Médiane : valeur médiane (plus robuste aux valeurs extrêmes)
    // - Min/Max : identifier les bornes et détecter les anomalies
    
    // Technique : agg() permet d'appliquer plusieurs fonctions d'agrégation en une seule passe
    // percentile_approx : calcul approximatif de percentiles (plus rapide sur gros volumes)
    val amountStats = txWithAmount.agg(
      avg(col("amount_num")).as("moyenne"),
      expr("percentile_approx(amount_num, 0.5)").as("mediane"),
      min(col("amount_num")).as("minimum"),
      max(col("amount_num")).as("maximum")
    )

    println("\n--- STATISTIQUES SUR LES MONTANTS ---")
    amountStats.show(truncate = false)

    // ================= DISTRIBUTION PAR TRANCHES =================
    // Métier : Segmentation des transactions par montant pour identifier les comportements
    // - <10€ : micro-transactions (cafés, snacks, transports)
    // - 10-50€ : transactions courantes (repas, courses légères)
    // - 50-200€ : achats moyens (courses complètes, vêtements)
    // - >200€ : achats importants (électronique, gros achats)
    
    // Technique : when().otherwise() = équivalent SQL de CASE WHEN
    // Création de buckets pour l'analyse de distribution
    val amountBuckets = txWithAmount
      .filter(col("amount_num").isNotNull)
      .withColumn(
        "tranche_montant",
        when(col("amount_num") < 10, "<10€")
          .when(col("amount_num").between(10, 50), "10–50€")
          .when(col("amount_num").between(50, 200), "50–200€")
          .otherwise(">200€")
      )
      .groupBy("tranche_montant")
      .count()
      .orderBy("tranche_montant")

    println("\n--- DISTRIBUTION DES MONTANTS PAR TRANCHES ---")
    amountBuckets.show(truncate = false)

    // ================= ANALYSE TEMPORELLE =================
    // Métier : Identifier les patterns temporels dans l'activité transactionnelle
    // - Heures de pointe : pics d'activité (midi, fin de journée)
    // - Jours de semaine vs week-end : comportements différents
    // - Saisonnalité : variations mensuelles (utile pour détecter des anomalies)
    
    // Technique : Fonctions temporelles Spark (hour, dayofweek, month)
    // Extraction de features temporelles à partir d'un timestamp
    val txWithTimeFeatures = txWithAmount
      .withColumn("heure", hour(col("date")))
      .withColumn("jour_semaine", dayofweek(col("date")))  // 1=Dimanche, 2=Lundi, ..., 7=Samedi
      .withColumn("mois", month(col("date")))

    // Métier : Distribution horaire des transactions
    // Permet d'identifier les heures d'activité maximale et les creux
    // Utile pour la détection de fraude (transactions à des heures inhabituelles)
    val transactionsParHeure = txWithTimeFeatures
      .groupBy("heure")
      .count()
      .orderBy("heure")

    println("\n--- NOMBRE DE TRANSACTIONS PAR HEURE ---")
    transactionsParHeure.show(24, truncate = false)

    // Métier : Répartition par jour de la semaine
    // Week-end vs semaine : comportements de consommation différents
    // Les fraudeurs peuvent privilégier certains jours (moins de surveillance)
    
    // Technique : Mapping des numéros de jour vers des noms lisibles
    // Améliore la présentation des résultats (UX)
    val transactionsParJour = txWithTimeFeatures
      .groupBy("jour_semaine")
      .count()
      .orderBy("jour_semaine")
      .withColumn(
        "nom_jour",
        when(col("jour_semaine") === 1, "Dimanche")
          .when(col("jour_semaine") === 2, "Lundi")
          .when(col("jour_semaine") === 3, "Mardi")
          .when(col("jour_semaine") === 4, "Mercredi")
          .when(col("jour_semaine") === 5, "Jeudi")
          .when(col("jour_semaine") === 6, "Vendredi")
          .otherwise("Samedi")
      )
      .select("jour_semaine", "nom_jour", "count")

    println("\n--- NOMBRE DE TRANSACTIONS PAR JOUR DE LA SEMAINE ---")
    transactionsParJour.show(truncate = false)

    // ================= ENRICHISSEMENT AVEC LES CATÉGORIES MCC =================
    // Métier : Ajout de la catégorie de commerçant pour contextualiser les transactions
    // Les MCC (Merchant Category Codes) permettent de classifier les transactions
    // par type d'activité (restauration, carburant, voyages, etc.)
    // Essentiel pour l'analyse comportementale et la détection d'anomalies
    
    // Technique : LEFT JOIN pour conserver toutes les transactions
    // Même celles sans MCC correspondant (merchant_category sera null)
    val txWithCategory = txWithTimeFeatures
      .join(mccDF, txWithTimeFeatures("mcc") === mccDF("mcc"), "left")
      .select(
        txWithTimeFeatures("*"),
        mccDF("merchant_category")
      )

    // Métier : Identifier les catégories de commerçants les plus fréquentées
    // Révèle les habitudes de consommation globales
    // Exemple : si "restauration" est #1, indique une forte activité dans ce secteur
    
    // Technique : groupBy + count + orderBy(desc) + limit = top K pattern
    val top10CategoriesVolume = txWithCategory
      .groupBy("merchant_category")
      .count()
      .orderBy(desc("count"))
      .limit(10)

    println("\n--- TOP 10 DES CATÉGORIES PAR VOLUME DE TRANSACTIONS ---")
    top10CategoriesVolume.show(truncate = false)

    // Métier : Montant moyen par catégorie de commerçant
    // Identifie quelles catégories génèrent les plus gros montants
    // Ex: voyages/hôtels ont souvent des montants moyens élevés vs cafés/snacks
    // Utile pour détecter des transactions inhabituelles (montant anormalement élevé pour une catégorie)
    
    // Technique : agg() avec plusieurs fonctions d'agrégation en une passe
    val montantMoyenParCategorie = txWithCategory
      .filter(col("amount_num").isNotNull && col("merchant_category").isNotNull)
      .groupBy("merchant_category")
      .agg(
        avg(col("amount_num")).as("montant_moyen"),
        count("*").as("nombre_transactions")
      )
      .orderBy(desc("montant_moyen"))

    println("\n--- MONTANT MOYEN PAR CATÉGORIE (TOP 20) ---")
    montantMoyenParCategorie.show(20, truncate = false)

    // ================= CRÉATION D'INDICATEURS POUR LA DÉTECTION DE FRAUDE =================
    // Métier : Construction de features comportementales par carte
    // Ces indicateurs permettent d'identifier des comportements anormaux :
    // - Activité excessive (trop de transactions en 1 jour)
    // - Dispersion géographique (utilisation dans trop de villes)
    // - Montants inhabituels (dépenses anormalement élevées)
    // - Taux d'erreurs (refus/échecs de transactions)
    
    // Technique : Ajout d'une colonne date sans la composante heure
    // Permet les agrégations journalières propres
    val txWithDate = txWithCategory.withColumn("jour", to_date(col("date")))

    // ================= INDICATEUR 1 : ACTIVITÉ JOURNALIÈRE PAR CARTE =================
    // Métier : Nombre de transactions par carte et par jour
    // Une carte normale fait 1-3 transactions/jour en moyenne
    // >10 transactions/jour peut indiquer une fraude ou une utilisation commerciale
    
    // Technique : Double groupBy (card_id + jour) pour une granularité fine
    val txParCarteParJour = txWithDate
      .groupBy("card_id", "jour")
      .count()
      .withColumnRenamed("count", "nb_transactions")
      .orderBy("card_id", "jour")

    println("\n--- NOMBRE DE TRANSACTIONS PAR CARTE ET PAR JOUR (échantillon) ---")
    txParCarteParJour.show(20, truncate = false)

    // ================= INDICATEUR 2 : MONTANT JOURNALIER PAR CARTE =================
    // Métier : Somme des montants dépensés par carte et par jour
    // Permet de détecter des pics de dépenses inhabituels
    // Ex: une carte qui dépense habituellement 50€/jour puis subitement 5000€
    
    // Technique : sum() pour l'agrégation, filter pour exclure les valeurs nulles
    val montantTotalParCarteParJour = txWithDate
      .filter(col("amount_num").isNotNull)
      .groupBy("card_id", "jour")
      .agg(
        sum(col("amount_num")).as("montant_total"),
        count("*").as("nb_transactions")
      )
      .orderBy("card_id", "jour")

    println("\n--- MONTANT TOTAL PAR CARTE ET PAR JOUR (échantillon) ---")
    montantTotalParCarteParJour.show(20, truncate = false)

    // ================= INDICATEUR 3 : DISPERSION GÉOGRAPHIQUE =================
    // Métier : Nombre de villes distinctes où une carte a été utilisée
    // Une carte volée est souvent utilisée dans de multiples villes rapidement
    // Une carte légitime est généralement utilisée dans 1-2 villes (domicile + travail)
    // >5 villes différentes sur la période totale peut être suspect
    
    // Technique : countDistinct() pour compter les valeurs uniques
    val villesParCarte = txWithDate
      .filter(col("merchant_city").isNotNull)
      .groupBy("card_id")
      .agg(
        countDistinct("merchant_city").as("nb_villes_distinctes"),
        count("*").as("nb_transactions")
      )
      .orderBy(desc("nb_villes_distinctes"))

    println("\n--- NOMBRE DE VILLES DIFFÉRENTES PAR CARTE (TOP 20) ---")
    villesParCarte.show(20, truncate = false)

    // ================= INDICATEUR 4 : TAUX D'ERREURS =================
    // Métier : Ratio de transactions avec erreurs
    // Erreurs = refus, PIN incorrect, fonds insuffisants, etc.
    // Un taux d'erreurs élevé peut indiquer :
    // - Tentatives de fraude (test de cartes volées)
    // - Problèmes techniques
    // - Utilisation abusive
    
    // Technique : Transformation binaire (1 si erreur, 0 sinon) puis calcul de ratio
    val ratioErreurs = txWithDate
      .withColumn("has_error", when(col("errors").isNotNull, 1).otherwise(0))
      .agg(
        count("*").as("total_transactions"),
        sum(col("has_error")).as("transactions_avec_erreur"),
        (sum(col("has_error")) / count("*") * 100).as("pourcentage_erreurs")
      )

    println("\n--- RATIO DE TRANSACTIONS AVEC ERREUR ---")
    ratioErreurs.show(truncate = false)

    // Métier : Taux d'erreurs par carte (vue détaillée)
    // Identifie les cartes problématiques individuellement
    // Filtre à 10 transactions minimum pour éviter les faux positifs
    // (ex: 1 erreur sur 1 transaction = 100% mais non significatif)
    val ratioErreursParCarte = txWithDate
      .withColumn("has_error", when(col("errors").isNotNull, 1).otherwise(0))
      .groupBy("card_id")
      .agg(
        count("*").as("total_transactions"),
        sum(col("has_error")).as("transactions_avec_erreur"),
        (sum(col("has_error")) / count("*") * 100).as("pourcentage_erreurs")
      )
      .filter(col("total_transactions") >= 10)  // Seuil statistique minimum
      .orderBy(desc("pourcentage_erreurs"))

    println("\n--- RATIO D'ERREURS PAR CARTE (TOP 20, min 10 transactions) ---")
    ratioErreursParCarte.show(20, truncate = false)

    // ================= DÉTECTION DE COMPORTEMENTS SUSPECTS =================
    // Métier : Système de scoring de fraude basé sur des règles métier
    // Approche multi-critères pour identifier les cartes à risque élevé
    // Les seuils sont calibrés sur l'expérience bancaire et peuvent être ajustés
    
    // SEUILS DE DÉTECTION (configurables selon le contexte business)
    val seuilTransactionsParJour = 10      // Activité excessive : >10 tx/jour
    val seuilVillesDifferentes = 3          // Dispersion géographique : >3 villes/jour
    val seuilMontantJournalier = 1000.0    // Dépenses élevées : >1000€/jour

    // ================= AGRÉGATION COMPLÈTE PAR CARTE ET PAR JOUR =================
    // Métier : Création d'un DataFrame consolidé avec tous les indicateurs de fraude
    // Chaque ligne représente une carte pour un jour donné avec :
    // - Nombre de transactions
    // - Nombre de villes différentes visitées
    // - Montant total dépensé
    // - Indicateurs booléens pour chaque critère de suspicion
    
    // Technique : Agrégation unique pour optimiser les performances
    // On calcule tous les indicateurs en une seule passe
    val cardDailyStats = txWithDate
      .groupBy("card_id", "jour")
      .agg(
        count("*").as("nb_transactions"),
        countDistinct("merchant_city").as("nb_villes_distinctes"),
        sum(col("amount_num")).as("montant_total_journalier")
      )

    // ================= IDENTIFICATION DES CARTES SUSPECTES =================
    // Métier : Application des règles de détection de fraude
    // Une carte est considérée suspecte si elle remplit au moins UN des critères :
    // 1. Volume excessif : plus de X transactions par jour
    // 2. Dispersion géographique : transactions dans plus de 3 villes par jour
    // 3. Montant élevé : dépenses journalières > seuil
    
    // Technique : Création de colonnes booléennes pour chaque critère
    // puis filtrage sur au moins un critère vrai
    val suspicious_cards = cardDailyStats
      .withColumn(
        "critere_nb_transactions",
        col("nb_transactions") > seuilTransactionsParJour
      )
      .withColumn(
        "critere_nb_villes",
        col("nb_villes_distinctes") > seuilVillesDifferentes
      )
      .withColumn(
        "critere_montant_eleve",
        col("montant_total_journalier") > seuilMontantJournalier
      )
      .filter(
        col("critere_nb_transactions") || 
        col("critere_nb_villes") || 
        col("critere_montant_eleve")
      )
      .withColumn(
        "nb_criteres_actifs",
        (col("critere_nb_transactions").cast("int") +
         col("critere_nb_villes").cast("int") +
         col("critere_montant_eleve").cast("int"))
      )
      .withColumn(
        "niveau_risque",
        when(col("nb_criteres_actifs") >= 3, "CRITIQUE")
          .when(col("nb_criteres_actifs") === 2, "ÉLEVÉ")
          .otherwise("MODÉRÉ")
      )
      .orderBy(desc("nb_criteres_actifs"), desc("montant_total_journalier"))

    println("\n--- CARTES SUSPECTES (APERÇU) ---")
    println(s"Nombre total de jours suspects détectés : ${suspicious_cards.count()}")
    suspicious_cards.show(30, truncate = false)

    // ================= ANALYSE DES PATTERNS DE FRAUDE PAR CRITÈRE =================
    // Métier : Distribution des types d'alertes
    // Identifie quel critère de fraude est le plus fréquemment déclenché
    // Permet d'orienter les actions de prévention et les règles de détection
    
    // Technique : Comptage des cartes par critère activé
    val statsParCritere = suspicious_cards.agg(
      sum(col("critere_nb_transactions").cast("int")).as("critere_volume_tx"),
      sum(col("critere_nb_villes").cast("int")).as("critere_dispersion_geo"),
      sum(col("critere_montant_eleve").cast("int")).as("critere_montant_eleve")
    )

    println("\n--- STATISTIQUES PAR TYPE DE CRITÈRE SUSPECT ---")
    statsParCritere.show(truncate = false)

    // ================= DISTRIBUTION PAR NIVEAU DE RISQUE =================
    // Métier : Segmentation des alertes par gravité
    // Permet de prioriser les investigations (CRITIQUE > ÉLEVÉ > MODÉRÉ)
    
    // Technique : Groupement par niveau de risque
    val statsNiveauRisque = suspicious_cards
      .groupBy("niveau_risque")
      .agg(
        countDistinct("card_id", "jour").as("nb_jours_suspects"),
        countDistinct("card_id").as("nb_cartes_distinctes")
      )
      .orderBy(desc("nb_jours_suspects"))

    println("\n--- DISTRIBUTION PAR NIVEAU DE RISQUE ---")
    statsNiveauRisque.show(truncate = false)

    // ================= SCORING DE RISQUE PAR CARTE =================
    // Métier : Top des cartes les plus suspectes
    // Cartes avec multiples jours suspects = priorité maximale pour investigation
    // Agrégation par carte pour voir la récurrence des comportements suspects
    
    // Technique : Agrégation par carte avec comptage et statistiques
    val top10CartesSuspectes = suspicious_cards
      .groupBy("card_id")
      .agg(
        count("*").as("nb_jours_suspects"),
        sum(col("nb_transactions")).as("total_transactions_suspectes"),
        sum(col("montant_total_journalier")).as("montant_total_suspect"),
        max(col("niveau_risque")).as("niveau_risque_max"),
        avg(col("nb_criteres_actifs")).as("avg_criteres_actifs")
      )
      .orderBy(desc("nb_jours_suspects"), desc("montant_total_suspect"))
      .limit(10)

    println("\n--- TOP 10 DES CARTES AVEC LE PLUS DE COMPORTEMENTS SUSPECTS ---")
    top10CartesSuspectes.show(truncate = false)

    // ================= ANALYSE DÉTAILLÉE DES ERREURS =================
    // Métier : Comprendre les types d'erreurs pour identifier les patterns de fraude
    // Les erreurs peuvent indiquer :
    // - Tentatives de fraude (PIN incorrect, carte refusée)
    // - Problèmes techniques (connexion réseau)
    // - Utilisation légitime mais problématique (fonds insuffisants)
    
    println("\n" + "=" * 60)
    println("PARTIE 3 : ANALYSE DES ERREURS")
    println("=" * 60)
    
    // Filtrer les transactions avec erreurs
    val txWithErrors = txWithDate.filter(col("errors").isNotNull && col("errors") =!= "")
    
    println(s"\nNombre total de transactions avec erreurs : ${txWithErrors.count()}")
    println(s"Pourcentage d'erreurs : ${(txWithErrors.count().toDouble / txWithDate.count() * 100).formatted("%.2f")}%")
    
    // Types d'erreurs les plus fréquents
    val errorTypes = txWithErrors
      .groupBy("errors")
      .count()
      .orderBy(desc("count"))
      .withColumn("pourcentage", (col("count") / txWithErrors.count() * 100))
    
    println("\n--- TYPES D'ERREURS LES PLUS FRÉQUENTS ---")
    errorTypes.show(20, truncate = false)
    
    // Taux d'erreur par carte (détaillé)
    val errorRateByCard = txWithDate
      .groupBy("card_id")
      .agg(
        count("*").as("total_transactions"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).as("nb_erreurs"),
        (sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)) / count("*") * 100).as("taux_erreur")
      )
      .filter(col("total_transactions") >= 5)  // Minimum 5 transactions pour la pertinence
      .orderBy(desc("taux_erreur"))
    
    println("\n--- TAUX D'ERREUR PAR CARTE (TOP 20) ---")
    errorRateByCard.show(20, truncate = false)
    
    // Taux d'erreur par client (via join avec cartes)
    val errorRateByUser = txWithDate
      .join(cardsDF, txWithDate("card_id") === cardsDF("id"), "left")
      .groupBy(cardsDF("client_id"))
      .agg(
        count("*").as("total_transactions"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).as("nb_erreurs"),
        (sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)) / count("*") * 100).as("taux_erreur"),
        countDistinct(txWithDate("card_id")).as("nb_cartes")
      )
      .filter(col("total_transactions") >= 10)
      .orderBy(desc("taux_erreur"))
    
    println("\n--- TAUX D'ERREUR PAR CLIENT (TOP 20) ---")
    errorRateByUser.show(20, truncate = false)
    
    // Corrélation erreurs et catégories MCC
    val errorsByCategory = txWithCategory
      .filter(col("merchant_category").isNotNull)
      .groupBy("merchant_category")
      .agg(
        count("*").as("total_transactions"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).as("nb_erreurs"),
        (sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)) / count("*") * 100).as("taux_erreur")
      )
      .filter(col("total_transactions") >= 50)
      .orderBy(desc("taux_erreur"))
    
    println("\n--- TAUX D'ERREUR PAR CATÉGORIE DE COMMERÇANT ---")
    errorsByCategory.show(20, truncate = false)

    // ================= BONUS 1 : SCORE DE RISQUE COMPOSITE =================
    // Métier : Score unifié combinant plusieurs indicateurs de fraude
    // Score de 0 à 100 où 100 = risque maximal
    // Pondération des critères selon leur importance métier
    
    println("\n" + "=" * 60)
    println("BONUS : SCORE DE RISQUE COMPOSITE")
    println("=" * 60)
    
    // Calcul des statistiques par carte sur toute la période
    val cardRiskFeatures = txWithDate
      .groupBy("card_id")
      .agg(
        count("*").as("total_transactions"),
        countDistinct("merchant_city").as("nb_villes_distinctes"),
        sum(col("amount_num")).as("montant_total"),
        avg(col("amount_num")).as("montant_moyen"),
        max(col("amount_num")).as("montant_max"),
        sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)).as("nb_erreurs"),
        (sum(when(col("errors").isNotNull && col("errors") =!= "", 1).otherwise(0)) / count("*") * 100).as("taux_erreur"),
        countDistinct("jour").as("nb_jours_actifs"),
        (count("*") / countDistinct("jour")).as("tx_par_jour_moyen"),
        countDistinct("mcc").as("nb_categories_distinctes")
      )
    
    // Calcul du score de risque (0-100)
    val cardRiskScore = cardRiskFeatures
      .withColumn("score_volume", 
        when(col("tx_par_jour_moyen") > 15, 25.0)
          .when(col("tx_par_jour_moyen") > 10, 20.0)
          .when(col("tx_par_jour_moyen") > 5, 10.0)
          .otherwise(0.0)
      )
      .withColumn("score_geo",
        when(col("nb_villes_distinctes") > 10, 25.0)
          .when(col("nb_villes_distinctes") > 5, 20.0)
          .when(col("nb_villes_distinctes") > 3, 10.0)
          .otherwise(0.0)
      )
      .withColumn("score_montant",
        when(col("montant_max") > 5000, 20.0)
          .when(col("montant_max") > 2000, 15.0)
          .when(col("montant_max") > 1000, 10.0)
          .otherwise(0.0)
      )
      .withColumn("score_erreur",
        when(col("taux_erreur") > 30, 30.0)
          .when(col("taux_erreur") > 20, 25.0)
          .when(col("taux_erreur") > 10, 15.0)
          .when(col("taux_erreur") > 5, 10.0)
          .otherwise(0.0)
      )
      .withColumn("risk_score",
        (col("score_volume") + col("score_geo") + col("score_montant") + col("score_erreur"))
      )
      .withColumn("risk_level",
        when(col("risk_score") >= 70, "CRITIQUE")
          .when(col("risk_score") >= 50, "ÉLEVÉ")
          .when(col("risk_score") >= 30, "MODÉRÉ")
          .when(col("risk_score") >= 15, "FAIBLE")
          .otherwise("NORMAL")
      )
      .orderBy(desc("risk_score"))
    
    println("\n--- TOP 20 DES CARTES PAR SCORE DE RISQUE ---")
    cardRiskScore.show(20, truncate = false)
    
    // Distribution des scores de risque
    val riskDistribution = cardRiskScore
      .groupBy("risk_level")
      .count()
      .orderBy(desc("count"))
    
    println("\n--- DISTRIBUTION DES NIVEAUX DE RISQUE ---")
    riskDistribution.show(truncate = false)

    // ================= BONUS 2 : COMPARAISON CLIENTS NORMAUX VS SUSPECTS =================
    println("\n" + "=" * 60)
    println("BONUS : COMPARAISON CLIENTS NORMAUX VS SUSPECTS")
    println("=" * 60)
    
    // Identifier les cartes à risque élevé
    val suspiciousCardIds = cardRiskScore
      .filter(col("risk_score") >= 50)
      .select("card_id")
    
    // Statistiques pour cartes suspectes
    val suspiciousStats = txWithDate
      .join(suspiciousCardIds, Seq("card_id"), "inner")
      .agg(
        avg(col("amount_num")).as("montant_moyen"),
        stddev(col("amount_num")).as("ecart_type_montant"),
        countDistinct("merchant_city").as("nb_villes_moyen"),
        (sum(when(col("errors").isNotNull, 1).otherwise(0)) / count("*") * 100).as("taux_erreur"),
        (count("*") / countDistinct("jour")).as("tx_par_jour")
      )
      .withColumn("type", lit("SUSPECTS"))
    
    // Statistiques pour cartes normales
    val normalStats = txWithDate
      .join(suspiciousCardIds, Seq("card_id"), "left_anti")
      .agg(
        avg(col("amount_num")).as("montant_moyen"),
        stddev(col("amount_num")).as("ecart_type_montant"),
        countDistinct("merchant_city").as("nb_villes_moyen"),
        (sum(when(col("errors").isNotNull, 1).otherwise(0)) / count("*") * 100).as("taux_erreur"),
        (count("*") / countDistinct("jour")).as("tx_par_jour")
      )
      .withColumn("type", lit("NORMAUX"))
    
    // Comparaison
    val comparison = suspiciousStats.union(normalStats)
    
    println("\n--- COMPARAISON CLIENTS NORMAUX VS SUSPECTS ---")
    comparison.show(truncate = false)

    // ================= BONUS 3 : SAUVEGARDE EN CSV ET PARQUET =================
    println("\n" + "=" * 60)
    println("BONUS : SAUVEGARDE DES RÉSULTATS EN CSV ET PARQUET")
    println("=" * 60)
    
    // Créer les dossiers de sortie
    val outputPath = "output"
    val parquetPath = "output/parquet"
    
    // Créer les dossiers s'ils n'existent pas
    import java.nio.file.{Files, Paths}
    val outputDir = Paths.get(outputPath)
    val parquetDir = Paths.get(parquetPath)
    
    if (!Files.exists(outputDir)) {
      Files.createDirectories(outputDir)
      println(s"✓ Dossier '$outputPath' créé")
    }
    
    if (!Files.exists(parquetDir)) {
      Files.createDirectories(parquetDir)
      println(s"✓ Dossier '$parquetPath' créé")
    }
    
    // Sauvegarder les résultats clés en CSV (un seul fichier par dataset)
    try {
      cardRiskScore
        .coalesce(1)  // Regrouper en un seul fichier
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$outputPath/card_risk_scores_temp")
      
      // Renommer le fichier part-* en nom simple
      val sourceDir = new java.io.File(s"$outputPath/card_risk_scores_temp")
      val partFile = sourceDir.listFiles().filter(_.getName.startsWith("part-")).head
      val targetFile = new java.io.File(s"$outputPath/card_risk_scores.csv")
      partFile.renameTo(targetFile)
      
      // Supprimer les fichiers temporaires
      sourceDir.listFiles().foreach(_.delete())
      sourceDir.delete()
      
      println(s"✓ Scores de risque sauvegardés : $outputPath/card_risk_scores.csv")
    } catch {
      case e: Exception => println(s"✗ Erreur lors de la sauvegarde des scores de risque : ${e.getMessage}")
    }
    
    try {
      suspicious_cards
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$outputPath/suspicious_cards_temp")
      
      val sourceDir = new java.io.File(s"$outputPath/suspicious_cards_temp")
      val partFile = sourceDir.listFiles().filter(_.getName.startsWith("part-")).head
      val targetFile = new java.io.File(s"$outputPath/suspicious_cards.csv")
      partFile.renameTo(targetFile)
      sourceDir.listFiles().foreach(_.delete())
      sourceDir.delete()
      
      println(s"✓ Cartes suspectes sauvegardées : $outputPath/suspicious_cards.csv")
    } catch {
      case e: Exception => println(s"✗ Erreur lors de la sauvegarde des cartes suspectes : ${e.getMessage}")
    }
    
    try {
      // Pour les transactions enrichies, on limite à un échantillon (trop volumineux)
      txWithCategory
        .limit(100000)  // Limiter à 100k transactions pour éviter un fichier trop gros
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$outputPath/transactions_enrichies_temp")
      
      val sourceDir = new java.io.File(s"$outputPath/transactions_enrichies_temp")
      val partFile = sourceDir.listFiles().filter(_.getName.startsWith("part-")).head
      val targetFile = new java.io.File(s"$outputPath/transactions_enrichies_sample.csv")
      partFile.renameTo(targetFile)
      sourceDir.listFiles().foreach(_.delete())
      sourceDir.delete()
      
      println(s"✓ Transactions enrichies (échantillon) sauvegardées : $outputPath/transactions_enrichies_sample.csv")
    } catch {
      case e: Exception => println(s"✗ Erreur lors de la sauvegarde des transactions enrichies : ${e.getMessage}")
    }
    
    try {
      errorRateByCard
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$outputPath/error_rates_temp")
      
      val sourceDir = new java.io.File(s"$outputPath/error_rates_temp")
      val partFile = sourceDir.listFiles().filter(_.getName.startsWith("part-")).head
      val targetFile = new java.io.File(s"$outputPath/error_rates.csv")
      partFile.renameTo(targetFile)
      sourceDir.listFiles().foreach(_.delete())
      sourceDir.delete()
      
      println(s"✓ Taux d'erreur sauvegardés : $outputPath/error_rates.csv")
    } catch {
      case e: Exception => println(s"✗ Erreur lors de la sauvegarde des taux d'erreur : ${e.getMessage}")
    }
    
    // ================= SAUVEGARDE EN PARQUET =================
    println("\n--- Sauvegarde en format Parquet ---")
    
    try {
      cardRiskScore
        .write
        .mode("overwrite")
        .parquet(s"$parquetPath/card_risk_scores")
      println(s"✓ Scores de risque sauvegardés en Parquet : $parquetPath/card_risk_scores")
    } catch {
      case e: Exception => println(s"✗ Erreur lors de la sauvegarde Parquet des scores de risque : ${e.getMessage}")
    }
    
    try {
      suspicious_cards
        .write
        .mode("overwrite")
        .parquet(s"$parquetPath/suspicious_cards")
      println(s"✓ Cartes suspectes sauvegardées en Parquet : $parquetPath/suspicious_cards")
    } catch {
      case e: Exception => println(s"✗ Erreur lors de la sauvegarde Parquet des cartes suspectes : ${e.getMessage}")
    }
    
    try {
      txWithCategory
        .write
        .mode("overwrite")
        .parquet(s"$parquetPath/transactions_enrichies")
      println(s"✓ Transactions enrichies sauvegardées en Parquet : $parquetPath/transactions_enrichies")
    } catch {
      case e: Exception => println(s"✗ Erreur lors de la sauvegarde Parquet des transactions enrichies : ${e.getMessage}")
    }
    
    try {
      errorRateByCard
        .write
        .mode("overwrite")
        .parquet(s"$parquetPath/error_rates")
      println(s"✓ Taux d'erreur sauvegardés en Parquet : $parquetPath/error_rates")
    } catch {
      case e: Exception => println(s"✗ Erreur lors de la sauvegarde Parquet des taux d'erreur : ${e.getMessage}")
    }

    // ================= PARTIE 5 : SYNTHÈSE FINALE =================
    println("\n" + "=" * 80)
    println("PARTIE 5 : SYNTHÈSE FINALE & RECOMMANDATIONS")
    println("=" * 80)
    
    println("\n### 1. PATTERNS PRINCIPAUX OBSERVÉS ###")
    println("─" * 80)
    
    // Récupération des métriques clés
    val nbCartesSuspectes = cardRiskScore.filter(col("risk_score") >= 50).count()
    val nbCartesTotal = cardRiskScore.count()
    val pourcentageSuspects = (nbCartesSuspectes.toDouble / nbCartesTotal * 100).formatted("%.2f")
    
    println(s"""
    │ a) Volumétrie générale :
    │    - Total transactions : $totalTransactions
    │    - Clients uniques : $totalUsers
    │    - Cartes uniques : $totalCards
    │    - Commerçants uniques : $totalMerchants
    │    
    │ b) Comportements suspects détectés :
    │    - Cartes à risque élevé : $nbCartesSuspectes sur $nbCartesTotal ($pourcentageSuspects%)
    │    - Critères principaux : volume excessif, dispersion géographique, montants élevés
    """)
  
    
    println("\n" + "=" * 80)
    println("FIN DE L'ANALYSE - Tous les fichiers sauvegardés dans ./output/")
    println("=" * 80)
    println("\n📁 Fichiers CSV (lisibles directement) :")
    println("  - output/card_risk_scores.csv : Scores de risque par carte")
    println("  - output/suspicious_cards.csv : Détails des comportements suspects")
    println("  - output/error_rates.csv : Taux d'erreur par carte")
    println("  - output/transactions_enrichies_sample.csv : Échantillon de transactions enrichies")
    println("\n📦 Fichiers Parquet (pour réutilisation Spark) :")
    println("  - output/parquet/card_risk_scores/")
    println("  - output/parquet/suspicious_cards/")
    println("  - output/parquet/error_rates/")
    println("  - output/parquet/transactions_enrichies/ (complet)")
    println("=" * 80 + "\n")

  finally
    // ================= NETTOYAGE DES RESSOURCES =================
    // Technique : Fermeture propre de la SparkSession
    // Libère les ressources (mémoire, threads, connexions)
    // finally garantit l'exécution même en cas d'erreur dans le try
    // Bonne pratique : toujours arrêter Spark pour éviter les fuites de ressources
    spark.stop()