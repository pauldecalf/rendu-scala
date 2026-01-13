# Données sources

## 📂 Fichiers requis

Les fichiers de données suivants sont nécessaires pour exécuter l'analyse :

### Fichiers CSV
- `cards_data.csv` (~500 Ko) : Données des cartes bancaires
- `transactions_data.csv` (~1.2 Go) : Historique des transactions
- `users_data.csv` (~160 Ko) : Profils des utilisateurs

### Fichiers JSON
- `mcc_codes.json` : Codes MCC (Merchant Category Codes)
- `train_fraud_labels.json` : Labels de fraude pour l'apprentissage

## ⚠️ Note importante

Ces fichiers ne sont **pas versionnés dans Git** car ils sont trop volumineux (notamment `transactions_data.csv` qui fait 1.2 Go).

## 📥 Comment obtenir les données

1. Téléchargez les données depuis la source fournie par votre enseignant
2. Placez tous les fichiers dans ce dossier `sources/`
3. Vérifiez que la structure est correcte :

```
sources/
├── cards_data.csv
├── transactions_data.csv
├── users_data.csv
├── mcc_codes.json
└── train_fraud_labels.json
```

## ✅ Validation

Pour vérifier que tous les fichiers sont présents :

```bash
ls -lh sources/
```

Vous devriez voir les 5 fichiers listés ci-dessus.
