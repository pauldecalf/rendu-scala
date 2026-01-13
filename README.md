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
