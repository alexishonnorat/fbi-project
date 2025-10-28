# FBI Fugitives Data Scraper

**Auteurs** : Lili Sheppard, Hugo Fissier, Alexis Honnorat  
**Date** : 21/10/2025

Script Python pour scraper, nettoyer et enrichir les données des fugitifs recherchés par le FBI.

---

## Prérequis

- Python 3.8 ou supérieur
- Connexion internet
- Accès à un proxy (fourni dans le code)

---

## Installation

### Étape 1 : Installer Crawl4AI

```bash
pip install crawl4ai
```

### Étape 2 : Installer les autres dépendances

```bash
pip install pandas beautifulsoup4 requests
```

### Étape 3 : Configurer le proxy

Ouvrir le fichier `main.py` et vérifier les paramètres du proxy (lignes 43-46) :

```python
PROXY_USERNAME = 'USERNAME'
PROXY_PASSWORD = 'PASSWORD'
PROXY_URL = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@gate.decodo.com:10001"
```

---

## Lancer le script

```bash
python3 main.py
```

**Temps d'exécution :** Environ 5 minutes pour 2 pages (~80 profils)

---

## Données collectées

Le script extrait pour chaque fugitif :

### Informations d'identification
- Nom complet
- Alias (noms d'emprunt)
- Catégorie du crime
- URL du profil FBI

### Caractéristiques démographiques
- Date de naissance
- Âge (calculé)
- Signe astrologique
- Sexe
- Nationalité
- Pays de naissance (avec code ISO)

### Description physique
- Taille (convertie en cm)
- Poids (converti en kg)
- Couleur des yeux
- Couleur des cheveux
- Présence de signes distinctifs (tatouages, cicatrices, etc.)

### Informations complémentaires
- Langues parlées (jusqu'à 3)
- Catégorie professionnelle
- Bureau FBI responsable
- Présence d'une récompense

---

## 📁 Fichiers générés

Le script crée **3 fichiers** dans le répertoire courant :

### 1. `fugitives_data_2pages.json`
Données brutes au format JSON. Conserve la structure complète extraite du site FBI.

### 2. `fugitives_dataframe_2pages.csv`
DataFrame brut avec les colonnes aplaties. Contient toutes les données non traitées.

### 3. `fugitives_cleaned_2pages.csv`
**Dataset final nettoyé et enrichi** - À utiliser pour l'analyse.

Contient **24 colonnes** organisées par thème :
- **Identification** : url, name, alias_text, alias_count, ncic_number, category
- **Démographie** : date_of_birth, age_years, zodiac_sign, sex, nationality, birth_country, birth_country_code
- **Physique** : height_cm, weight_kg, eye_color, hair_color, has_mark
- **Langues** : language_primary, language_secondary, language_tertiary
- **Professionnel** : occupation_category
- **FBI** : fbi_field_office, has_caution
