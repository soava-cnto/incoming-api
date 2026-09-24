# Traitement Centralisé des Fichiers CSV Problématiques

## 📋 Résumé

Implémentation d'un prétraitement centralisé pour tous les fichiers CSV de l'application, notamment pour gérer les problèmes provenant de VocalCom et d'autres sources :
- Espaces insécables (U+00A0)
- Guillemets mal formés
- Encodages variés (UTF-16, ISO-8859-1, etc.)

## 🎯 Routes Concernées

Toutes les routes d'ingestion utilisent maintenant le prétraitement centralisé :

| Route | Fichier | Méthode | Prétraitement |
|-------|---------|---------|--------------|
| `POST /ingest/file` | [routers/ingest.py](../routers/ingest.py) | `ingest_file()` | ✓ Via CSVReader |
| `POST /ingest/path` | [routers/ingest.py](../routers/ingest.py) | `ingest_path()` | ✓ Via CSVReader |
| `POST /ingest/sftp` | [routers/ingest.py](../routers/ingest.py) | `ingest_from_sftp()` | ✓ Via IngestionService.process_sftp_file() |
| `POST /ingest/sftp/auto` | [routers/ingest.py](../routers/ingest.py) | `ingest_yesterday()` | ✓ Via auto_ingest_yesterday() |

## 🏗️ Architecture

### Flux de Traitement

```
Fichier (brut)
     ↓
CSVPreprocessor.preprocess_raw_content()    ← Nettoyage AVANT parsing
     ↓                                          (caractères, séquences)
Contenu Nettoyé (str)
     ↓
pd.read_csv(engine="python")               ← Parsing robuste
     ↓
DataFrame Brut
     ↓
CSVPreprocessor.clean_dataframe()          ← Nettoyage APRÈS parsing
     ↓                                          (colonnes, valeurs)
DataFrame Nettoyé
     ↓
DataCleaner.clean()                        ← Nettoyage métier
     ↓
DBWriter.copy_dataframe()                  ← Insertion en base
```

### Composants Modifiés

#### 1. **Nouveau : CSVPreprocessor** (`app/utils/csv_preprocessor.py`)

Classe centralisée pour le prétraitement des CSV problématiques.

**Méthodes principales :**

- `preprocess_raw_content(raw_content, encoding)` → str
  - Nettoie le contenu brut avant parsing
  - Remplace U+00A0 par espace
  - Corrige les séquences ` ""` en `"`

- `read_csv_with_preprocessing(source, encoding, **pandas_kwargs)` → DataFrame
  - Intègre les deux phases de nettoyage (avant + après parsing)
  - Utilise `engine="python"` pour flexibilité

- `read_csv_in_chunks(source, encoding, chunksize, **pandas_kwargs)` → Generator
  - Lecture par chunks avec prétraitement
  - Utile pour gros fichiers

- `clean_dataframe(df)` → DataFrame
  - Nettoie les noms de colonnes (guillemets/espaces)
  - Nettoie les valeurs texte (guillemets/espaces)

- `detect_and_clean_encoding_issues(raw_content, attempted_encodings)` → (encoding, DataFrame)
  - Détecte l'encodage et nettoie automatiquement
  - Fallback sur liste d'encodages

#### 2. **Modifié : CSVReader** (`app/csv_reader.py`)

Utilise maintenant CSVPreprocessor au lieu de lire directement avec `pd.read_csv()`.

**Changements :**
- `_try_read()` → délègue à `CSVPreprocessor.read_csv_with_preprocessing()`
- `get_chunks()` → utilise `CSVPreprocessor.read_csv_in_chunks()`
- Gère l'exclusion de la colonne COMMENTAIRE via `usecols`

#### 3. **Modifié : IngestionService** (`app/services/ingestion_service.py`)

Refactorisé pour utiliser CSVPreprocessor de manière cohérente.

**Changements :**
- `process_csv()` → utilise CSVReader (qui utilise CSVPreprocessor)
- `process_sftp_file()` → utilise `CSVPreprocessor.read_csv_in_chunks()` directement
- Suppression de `clean_csv_remove_comment_column()` (logique consolidée dans CSVPreprocessor)
- Import corrigé : `from time import sleep` (au lieu de `from time import time`)
- Exception handling amélioré
- Docstrings enrichies

## 🧪 Tests Ajoutés

Fichier : [tests_csv_preprocessor.py](../tests_csv_preprocessor.py)

### Classes de Tests

#### **TestCSVPreprocessor**
- `test_preprocess_nonbreaking_space()` → Remplacement U+00A0
- `test_preprocess_malformed_quotes()` → Correction ` ""` 
- `test_clean_dataframe_columns()` → Nettoyage noms colonnes
- `test_clean_dataframe_values()` → Nettoyage valeurs texte
- `test_read_csv_with_preprocessing_utf16()` → Lecture UTF-16 avec prétraitement
- `test_read_csv_in_chunks_with_preprocessing()` → Lecture par chunks
- `test_read_csv_exclude_commentary_column()` → Exclusion COMMENTAIRE
- `test_read_csv_valid_csv_still_works()` → Rétrocompatibilité CSV valides
- `test_empty_file_raises_error()` → Gestion fichier vide
- `test_detect_and_clean_encoding_issues()` → Détection encodage + fallback

#### **TestCSVReader**
- `test_csv_reader_with_nonbreaking_spaces()` → CSVReader + espaces insécables
- `test_csv_reader_exclude_comment_column()` → CSVReader exclut COMMENTAIRE
- `test_csv_reader_include_comment_column()` → CSVReader inclut COMMENTAIRE si demandé

#### **TestIntegrationVocalComFile**
- `test_vocalcom_like_file()` → Simulation fichier VocalCom complet (UTF-16 + espaces + guillemets)

## ⚙️ Encodages Gérés

CSVPreprocessor supporte les encodages suivants (en ordre de priorité) :

1. Encodage explicite passé en paramètre
2. Fallback automatique : UTF-16, UTF-16-LE, UTF-16-BE, ISO-8859-1, CP1252, UTF-8

## 📝 Exemples d'Utilisation

### Lecture d'un fichier local avec prétraitement

```python
from app.csv_reader import CSVReader

reader = CSVReader(
    "2026-08-11_VocalCom_Incoming.csv",
    chunksize=5000,
    include_comment=False  # Exclut COMMENTAIRE
)

for chunk in reader.get_chunks():
    # Chunk est déjà nettoyé
    print(chunk.head())
```

### Lecture directe avec CSVPreprocessor

```python
from app.utils.csv_preprocessor import CSVPreprocessor

df = CSVPreprocessor.read_csv_with_preprocessing(
    "fichier.csv",
    encoding="utf-16"
)
```

### Détection automatique d'encodage

```python
with open("fichier.csv", "rb") as f:
    raw_data = f.read()

encoding, df = CSVPreprocessor.detect_and_clean_encoding_issues(raw_data)
print(f"Encodage détecté : {encoding}")
```

## 🔄 Flux d'Ingestion Détaillé

### Pour `POST /ingest/file`

```
UploadFile (bytes)
  ↓
IngestionService.process_csv()
  ↓
CSVReader.__init__()
  ↓
reader.get_chunks()  ← CSVPreprocessor.read_csv_in_chunks()
  ↓
[Chunk prétraité]  ← preprocess_raw_content() → pd.read_csv() → clean_dataframe()
  ↓
DataCleaner.clean()
  ↓
DBWriter.copy_dataframe()
```

### Pour `POST /ingest/sftp`

```
remote_path (str)
  ↓
IngestionService.process_sftp_file()
  ↓
SFTPClient.read_file()  → raw_data (bytes)
  ↓
SFTPClient.detect_encoding()  → encoding (str)
  ↓
CSVPreprocessor.read_csv_in_chunks(raw_data, encoding)
  ↓
[Chunk prétraité]  ← preprocess_raw_content() → pd.read_csv() → clean_dataframe()
  ↓
DataCleaner.clean()
  ↓
DBWriter.copy_dataframe()
```

## ✅ Garanties et Compatibilité

### Ce qui est préservé

- ✓ Format de réponse des API (toujours `{"status": ..., "file": ..., "rows": ...}`)
- ✓ Logique métier (DataCleaner.clean() inchangé)
- ✓ Gestion des erreurs (mêmes codes d'erreur, logging enrichi)
- ✓ Comportement d'exclusion COMMENTAIRE (via `include_comment` param)
- ✓ Chunks de la même taille (CHUNK_SIZE = 5000)

### Ce qui change

- ✓ **Nettoyage plus robuste** : espaces insécables et guillemets mal formés
- ✓ **Encoding plus flexible** : fallback automatique sur plusieurs encodages
- ✓ **Logging amélioré** : messages plus précis et détaillés
- ✓ **Code plus maintenable** : logique centralisée dans CSVPreprocessor

## 🛡️ Risques Identifiés et Mitigations

| Risque | Mitigation |
|--------|-----------|
| Perte de performance sur gros fichiers | Lecture par chunks maintenue, pas de chargement full-memory |
| Incompatibilité encodages exotiques | Fallback sur 6 encodages standards, détection automatique |
| Perte de données lors du nettoyage | Tests validant rétrocompatibilité CSV valides |
| Changement comportement COMMENTAIRE | Paramètre `include_comment` préservé et testé |
| Régression sur fichiers valides | Suite de tests complète + cas de rétrocompatibilité |

## 📦 Fichiers Modifiés / Créés

| Fichier | Type | Statut |
|---------|------|--------|
| `app/utils/csv_preprocessor.py` | ✨ Nouveau | Créé |
| `app/csv_reader.py` | 🔧 Modifié | Refactorisé pour utiliser CSVPreprocessor |
| `app/services/ingestion_service.py` | 🔧 Modifié | Refactorisé pour utiliser CSVPreprocessor, imports corrigés |
| `tests_csv_preprocessor.py` | ✨ Nouveau | Suite de tests complète |

## 🚀 Déploiement

1. **Vérifier la compilation** :
   ```bash
   python -m py_compile app/utils/csv_preprocessor.py app/csv_reader.py app/services/ingestion_service.py
   ```

2. **Exécuter les tests** :
   ```bash
   python -m unittest tests_csv_preprocessor.py -v
   ```

3. **Tester avec un fichier VocalCom réel** (si disponible)

4. **Monitorer les logs** en production pour vérifier :
   - Encodages détectés
   - Colonnes COMMENTAIRE exclues/incluses correctement
   - Nombre de lignes traitées

## 📞 Support

Pour les questions sur le traitement :
- Consulter les docstrings de `CSVPreprocessor`
- Vérifier les logs avec tags `[INGEST]`, `[SFTP]`, `[CSVReader]`
- Exécuter les tests unitaires pour valider un cas spécifique
