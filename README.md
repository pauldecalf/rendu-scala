# TP Scala & Spark - Analyse de transactions bancaires et détection de fraudes

## 📋 Description

Projet d'analyse de données bancaires utilisant **Apache Spark** et **Scala** pour détecter les comportements frauduleux dans les transactions par carte bancaire.

## 🎯 Objectifs

- Analyser les patterns de consommation des clients
- Détecter les comportements suspects multi-critères
- Calculer un score de risque composite
- Générer des rapports d'analyse en CSV et Parquet

## 🏗️ Architecture

### Indicateurs de fraude détectés :
1. **Volume excessif** : Trop de transactions par jour
2. **Dispersion géographique** : Utilisation dans trop de villes différentes
3. **Montants élevés** : Dépenses journalières anormalement élevées
4. **Taux d'erreurs** : Ratio de transactions refusées

### Score de risque composite :
- **CRITIQUE** : Score ≥ 70
- **ÉLEVÉ** : Score ≥ 50
- **MODÉRÉ** : Score ≥ 30
- **FAIBLE** : Score ≥ 15
- **NORMAL** : Score < 15

## 📁 Structure du projet

```
.
├── exercice.scala              # Code principal d'analyse
├── lire_resultats.scala        # Script de lecture des résultats Parquet
├── sources/                    # Données sources (non versionnées, voir DATA.md)
│   └── DATA.md                # Instructions pour obtenir les données
└── output/                     # Résultats générés (non versionnés)
    ├── *.csv                  # Résultats lisibles (Excel, éditeur)
    └── parquet/               # Résultats optimisés pour Spark
```

## 🚀 Installation et exécution

### Prérequis
- Java 21
- Scala 3.7.4
- Scala CLI

### ⚠️ Préparation des données sources

**IMPORTANT** : Avant d'exécuter le projet, vous devez créer un dossier `sources/` et y placer les fichiers de données nécessaires.

```bash
# Créer le dossier sources (si nécessaire)
mkdir -p sources

# Y placer les fichiers suivants :
# - cards_data.csv
# - transactions_data.csv
# - users_data.csv
# - mcc_codes.json
# - train_fraud_labels.json
```

Consultez le fichier `sources/DATA.md` pour plus d'informations sur l'obtention des données.

### Exécution

```bash
# Lancer l'analyse complète
scala-cli run exercice.scala

# Lire les résultats sauvegardés en Parquet
scala-cli run lire_resultats.scala
```

## 📊 Résultats générés

### Fichiers CSV (lisibles directement)
- `output/card_risk_scores.csv` : Scores de risque par carte
- `output/suspicious_cards.csv` : Détails des comportements suspects
- `output/error_rates.csv` : Taux d'erreur par carte
- `output/transactions_enrichies_sample.csv` : Échantillon de transactions enrichies

### Fichiers Parquet (pour réutilisation Spark)
- `output/parquet/card_risk_scores/` : Dataset complet des scores
- `output/parquet/suspicious_cards/` : Dataset complet des alertes
- `output/parquet/error_rates/` : Dataset complet des erreurs
- `output/parquet/transactions_enrichies/` : Dataset complet (toutes les transactions)

## 🔬 Analyses réalisées

1. **Volumétrie et qualité des données**
2. **Statistiques descriptives** (montants, distribution)
3. **Analyse temporelle** (heures de pointe, jours de la semaine)
4. **Enrichissement avec MCC** (catégories de commerçants)
5. **Détection de comportements suspects**
6. **Analyse des erreurs** par carte et par commerçant
7. **Score de risque composite**
8. **Comparaison clients normaux vs suspects**

## 🔍 Analyse des résultats

### Patterns principaux observés

#### Volumétrie générale
- **13 305 915 transactions** analysées sur 10 ans (2010-2020)
- **2 000 clients uniques** et **6 146 cartes** distinctes
- **74 831 commerçants** différents
- Montant moyen : **42,98 €** (médiane : 28,99 €)
- Taux d'erreur global : **1,59%** (211 393 transactions)

#### Comportements temporels
- **Pics d'activité** : 11h-13h (heures de déjeuner, ~940K transactions)
- **Creux nocturnes** : 0h-5h (~600K transactions)
- Distribution **uniforme sur la semaine** (~1,9M transactions/jour)

#### Catégories de dépenses
- **Top 3** : Supermarchés (1,6M), Alimentation diverse (1,5M), Stations-service (1,4M)
- **Montants moyens élevés** : Croisières (1 551 €), Métallurgie (700-800 €), Hôpitaux (726 €)

#### Détection de fraude
- **92 020 jours suspects** détectés sur 6 146 cartes
- **7 cartes à risque ÉLEVÉ** (0,17% du total)
- **Critères activés** :
  - Dispersion géographique : 72 394 alertes (critère le plus fréquent)
  - Montants élevés : 14 814 alertes
  - Volume excessif : 9 486 alertes

#### Patterns d'erreurs
- **"Insufficient Balance"** : 61,9% des erreurs
- **"Bad PIN"** : 15,2% des erreurs
- **"Technical Glitch"** : 12,4% des erreurs
- Certaines cartes atteignent **15% de taux d'erreur** (carte 2220, 2644, 5586)

### Indicateurs utiles pour un futur modèle de Machine Learning

#### Features comportementales (★★★ Haute importance)
1. **Volume transactionnel** : transactions/jour, écart-type du volume
2. **Dispersion géographique** : nombre de villes distinctes, distance entre transactions
3. **Patterns temporels** : heures inhabituelles, concentration de transactions
4. **Montants** : moyenne, écart-type, montant maximum, pics anormaux

#### Features d'erreur (★★ Importance moyenne)
5. **Taux d'erreur** : ratio de transactions refusées
6. **Types d'erreurs** : fréquence par type (PIN, CVV, solde)
7. **Séquences d'erreurs** : erreurs consécutives (signe de vol de carte)

#### Features contextuelles (★ Importance faible)
8. **Catégories MCC** : diversité des types de commerçants
9. **Utilisation de la puce** : ratio Chip vs Swipe vs Online
10. **Profil client** : revenus, âge, credit_score, dette

#### Features composites recommandées
- **Velocity checks** : transactions/heure sur une fenêtre glissante
- **Distance géographique** : km entre 2 transactions consécutives
- **Ratio Online/Physique** : changement brutal de comportement
- **Score de risque composite** : combinaison pondérée multi-critères

### Limites des données

#### Qualité des données
- ⚠️ **11,7% de merchant_state NULL** (1 563 700 transactions)
- ⚠️ **12,4% de zip NULL** (1 652 706 transactions) → limite la géolocalisation précise
- ⚠️ **98,4% de errors NULL** → la majorité des transactions sont "normales"
- ⚠️ **670 688 transactions avec montant ≤ 0** (5%) → remboursements non distingués

#### Couverture géographique
- Les transactions **en ligne** (ONLINE) n'ont pas de state/zip → impossible de tracer géographiquement
- Bias vers les zones urbaines (NYC, CA, etc.)

#### Labels de fraude
- ⚠️ **Pas de labels de fraude confirmée** dans les données
- Seuls des **indicateurs de suspicion** sont calculés (heuristiques)
- Impossible de valider les détections avec un ground truth
- Le fichier `train_fraud_labels.json` n'est pas utilisé dans l'analyse actuelle

#### Biais temporels
- Données de **2010-2020** → comportements d'achat pré-COVID
- Évolution des habitudes de consommation non capturée (explosion du e-commerce)
- Inflation non prise en compte dans les montants

#### Limites techniques
- **Pas de séquence temporelle fine** : difficile de détecter des transactions rapprochées
- **Pas d'informations sur le commerçant** : impossible de détecter des commerçants frauduleux
- **Pas de données sur les disputes/chargebacks** : pas de feedback sur les vraies fraudes

### Recommandations pour améliorer le modèle
1. ✅ **Nettoyer les données** : imputer les NULL, séparer remboursements/achats
2. ✅ **Ajouter des features temporelles** : délai entre transactions, fenêtres glissantes
3. ✅ **Utiliser les labels de fraude** disponibles dans `train_fraud_labels.json`
4. ✅ **Géolocalisation avancée** : calculer distances réelles avec lat/long
5. ✅ **Analyse de réseau** : détecter les commerçants suspects

## 📝 Technologies utilisées

- **Scala** 3.7.4
- **Apache Spark** 4.1.1
- **Java** 21
- Format de sauvegarde : CSV et Parquet

## 👤 Auteur

Paul Decalf

## 📄 Licence

Projet académique - TP Scala & Spark
# rendu-scala
# rendu-scala
