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

### Questions et Réponses

#### 📊 Combien de colonnes par fichier ? Quels types de données semblent incorrects ou suspects ?

**Cards Data** : 13 colonnes
- ✅ Types corrects : `id`, `client_id`, `card_number`, `cvv`, `year_pin_last_changed`
- ⚠️ Types suspects :
  - `credit_limit` (String) → devrait être numérique (format "$24295")
  - `has_chip` (String "YES"/"NO") → devrait être Boolean
  - `card_on_dark_web` (String "Yes"/"No") → devrait être Boolean
  - `expires` (String "MM/YYYY") → devrait être Date

**Transactions Data** : 12 colonnes
- ✅ Types corrects : `id`, `client_id`, `card_id`, `merchant_id`, `mcc`
- ⚠️ Types suspects :
  - `amount` (String) → devrait être numérique (format "$14.57")
  - `use_chip` (String "Swipe Transaction") → devrait être enum ou code
  - `date` (Timestamp) → ✅ correct
  - **670 688 montants négatifs** (5%) → probablement des remboursements mal étiquetés

**Users Data** : 14 colonnes
- ✅ Types corrects : `id`, `current_age`, `credit_score`, `latitude`, `longitude`
- ⚠️ Types suspects :
  - `per_capita_income` (String) → devrait être numérique (format "$29278")
  - `yearly_income` (String) → devrait être numérique (format "$59696")
  - `total_debt` (String) → devrait être numérique (format "$127613")

**MCC Codes** : 2 colonnes
- ✅ Format correct (mcc: String, merchant_category: String)

**Conclusion** : Les montants financiers sont systématiquement stockés en String avec le symbole "$", nécessitant un nettoyage.

#### 📈 Qui génère le plus de lignes ?

**Fichier Transactions** : 13 305 915 lignes → **99,99% du volume total**
- Cards : 6 146 lignes (0,046%)
- Users : 2 000 lignes (0,015%)
- MCC Codes : ~1 000 lignes (négligeable)

**Par carte** : La carte **3239** génère **30 520 transactions** (record)
**Par client** : Le client **1888** génère **40 105 transactions** sur 3 cartes
**Par commerçant** : 74 831 commerçants uniques, moyenne de **178 transactions/commerçant**

#### 💰 Question métier : Les montants élevés sont-ils rares ou fréquents ?

**Distribution des montants** :
- **<10€** : 3 574 928 transactions (26,9%) → fréquents
- **10-50€** : 5 283 992 transactions (39,7%) → **très fréquents**
- **50-200€** : 4 127 894 transactions (31,0%) → fréquents
- **>200€** : 319 101 transactions (2,4%) → **RARES**

**Statistiques** :
- Montant moyen : **42,98 €**
- Médiane : **28,99 €** (50% des transactions < 29 €)
- Maximum : **6 820,20 €**

**Conclusion** : Les montants élevés (>200€) sont **rares** (2,4%). 97,6% des transactions sont < 200€.

#### ⏰ Existe-t-il des heures anormalement actives ?

**Heures normales actives** (comportement attendu) :
- **11h-12h** : 943 671 transactions (pic déjeuner)
- **12h-13h** : 953 498 transactions (pic maximal)
- **6h-17h** : ~80% du volume total

**Heures anormalement CALMES** (suspect si activité importante) :
- **23h-0h** : 158 877 transactions (1,2%)
- **3h-4h** : 103 784 transactions (0,8%) → **heure la plus calme**
- **0h-5h** : seulement 587 918 transactions (4,4%)

**Analyse de fraude** :
- Une transaction à **3h du matin** est statistiquement **16x moins probable** qu'à 12h
- Les cartes avec beaucoup de transactions nocturnes sont **potentiellement suspectes**
- La carte **1016** a des transactions à toutes heures → risque ÉLEVÉ détecté

#### 🏪 Certaines catégories sont-elles plus risquées ?

**Top 3 des catégories avec le plus d'erreurs** :
1. **Theatrical Producers** : 3,26% d'erreurs (1 288/39 544)
2. **Cable, Satellite TV** : 3,11% d'erreurs (1 602/51 526)
3. **Money Transfer** : 3,09% d'erreurs (18 206/589 140) → ⚠️ **très suspect**

**Catégories à montants élevés** (cible de fraude) :
- **Cruise Lines** : 1 551 € en moyenne
- **Hospitals** : 726 € en moyenne
- **Legal Services** : 536 € en moyenne

**Catégories sûres** (taux d'erreur < 1%) :
- Grocery Stores : 1,59M transactions, faible taux d'erreur
- Service Stations : 1,42M transactions, très standard

**Conclusion** : Oui, **Money Transfer** et **Theatrical Producers** sont significativement plus risquées que la moyenne (1,59%).

#### 🚨 Un client avec beaucoup d'erreurs est-il suspect ?

**Analyse statistique** :

**Client 954** : 14,64% d'erreurs (2 935/20 047 transactions) → **HAUTEMENT SUSPECT**
- 9x plus que la moyenne (1,59%)
- 4 cartes différentes
- Comportement anormal : peut-être des cartes volées

**Top 5 clients à taux d'erreur élevé** :
1. Client **954** : 14,64%
2. Client **1189** : 6,40%
3. Client **363** : 5,52%
4. Client **1424** : 5,33%
5. Client **1888** : 5,26%

**Corrélation erreurs / fraude** :
- Taux normal : 1,59%
- Taux suspect : > 3% (2x la moyenne)
- Taux critique : > 10% (6x la moyenne)

**Interprétation** :
- ✅ **Oui, un taux d'erreur > 5% est suspect** (peut indiquer vol de carte, PIN incorrect)
- ❌ **Mais attention aux faux positifs** : peut être un client âgé qui oublie son PIN
- 🔍 **À croiser avec** : dispersion géographique, montants élevés, heures anormales

**Conclusion** : Un client avec **> 5% d'erreurs** nécessite une investigation, surtout si combiné avec d'autres signaux.

#### 🔍 Quels patterns principaux sont observés ?

**1. Pattern de dispersion géographique**
- Carte normale : 10-50 villes distinctes
- Carte suspecte : > 100 villes distinctes
- Carte **3239** : **359 villes** → risque ÉLEVÉ

**2. Pattern de volume excessif**
- Carte normale : 1-5 transactions/jour
- Carte suspecte : > 10 transactions/jour
- Carte **1016** : **7,2 transactions/jour** sur 10 ans

**3. Pattern de montants élevés**
- 97,6% des transactions < 200€
- Jours avec montant total > 1 500€ → suspect
- Pic détecté : **3 550,79 €** en un jour (carte 1117)

**4. Pattern d'erreurs en cascade**
- Carte normale : 1-2% erreurs
- Carte suspecte : > 5% erreurs
- Carte **2220** : **15% d'erreurs** → carte probablement volée

**5. Pattern temporel nocturne**
- Transactions entre 0h-5h : 4,4% du volume
- Cartes actives la nuit avec montants élevés → suspect

#### 🤖 Quels indicateurs semblent utiles pour un futur modèle ?

**Features à haute importance** (★★★) :
1. **Taux d'erreur** (ratio transactions refusées) → forte corrélation avec fraude
2. **Dispersion géographique** (nombre de villes distinctes) → meilleur prédicteur observé
3. **Volume journalier** (nb transactions/jour) → détecte les pics anormaux
4. **Montant total journalier** → détecte les dépenses excessives
5. **Heure de transaction** → nuit = risque accru

**Features à importance moyenne** (★★) :
6. **Catégorie MCC** → certaines catégories plus risquées (Money Transfer)
7. **Écart-type des montants** → variabilité anormale
8. **Distance entre transactions** (lat/long) → voyages impossibles
9. **Type de transaction** (Chip vs Swipe vs Online) → Online plus risqué
10. **Nombre de transactions par heure** → pics suspects

**Features contextuelles** (★) :
11. **Credit score du client** → corrélation avec solvabilité
12. **Âge de la carte** (acct_open_date) → nouvelles cartes plus risquées
13. **Dark web flag** → si "Yes", risque maximal

**Score de risque composite** (utilisé dans l'analyse) :
- Combinaison pondérée : `score_volume + score_geo + score_montant + score_erreur`
- Seuils : CRITIQUE (≥70), ÉLEVÉ (≥50), MODÉRÉ (≥30)

#### ⚠️ Quelles limites présentent ces données ?

**1. Qualité des données**
- 11,7% de `merchant_state` NULL → géolocalisation incomplète
- 12,4% de `zip` NULL → impossible de calculer distances précises
- 5% de montants négatifs → remboursements non distingués des achats
- Types de données incorrects (String au lieu de numérique)

**2. Absence de labels**
- ⚠️ **Pas de labels de fraude confirmée** (ground truth manquant)
- Impossible de valider les détections
- Le fichier `train_fraud_labels.json` n'est pas exploité

**3. Biais temporels**
- Données de 2010-2020 (pré-COVID) → comportements obsolètes
- Explosion du e-commerce non capturée
- Inflation non prise en compte

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
