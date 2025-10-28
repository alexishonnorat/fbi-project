#!/usr/bin/env python3
"""
FBI Fugitives Data Scraper - Version Finale

Le processus se fait en 3 étapes principales :
    1. SCRAPING : Extraction des URLs et des profils depuis le site FBI.gov
    2. NETTOYAGE : Transformation et enrichissement des données brutes
    3. SAUVEGARDE : Export dans différents formats (JSON, CSV, Excel)

Auteurs : Lili Sheppard, Hugo Fisiser, Alexis Honnorat
Date : 21/10/2025

"""

import time
import json
import asyncio
import requests
import re
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
import pandas as pd
from datetime import datetime, date


# ============================================================================
# CONFIGURATION GLOBALE
# ============================================================================

# Configuration du proxy
PROXY_USERNAME = 'USERNAME'
PROXY_PASSWORD = 'PASSWORD'
PROXY_URL = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@gate.decodo.com:10001"
PROXY_VERIFICATION_URL = 'https://ip.decodo.com/json'

# Configuration du scraping
NOMBRE_PAGES = 1  # Nombre de pages à scraper
DELAY_BETWEEN_REQUESTS = 2  # Délai en secondes entre chaque requête

# Date de référence pour le calcul de l'âge
# Fixée pour assurer la cohérence des calculs
TODAY = date(2025, 10, 21)


# ============================================================================
# PARTIE 1 : SCRAPING DES DONNÉES
# ============================================================================


def setup_proxy():
    print("=" * 60)
    print("CONFIGURATION DU PROXY")
    print("=" * 60)

    try:
        # Test de connexion avec le proxy
        result = requests.get(PROXY_VERIFICATION_URL, proxies={
            'http': PROXY_URL,
            'https': PROXY_URL
        })
        print(f"Proxy configuré : {result.text}")
        return True
    except Exception as e:
        print(f"Erreur proxy : {e}")
        return False


def crawl_get(url):
    print(f"Crawling : {url}")

    async def _run():
        async with AsyncWebCrawler(proxy=PROXY_URL) as crawler:
            return await crawler.arun(url=url)

    return asyncio.run(_run())


def extract_category_from_url(url):
    """
    Extrait la catégorie du crime depuis l'URL du profil.

    Args:
        url (str): URL du profil du fugitif

    Returns:
        str or None: Catégorie extraite, ou None si non trouvée
    """
    try:
        parts = url.split('/')
        if 'wanted' in parts:
            wanted_index = parts.index('wanted')
            # La catégorie est l'élément suivant "wanted" dans l'URL
            if wanted_index + 1 < len(parts):
                return parts[wanted_index + 1]
        return None
    except Exception as e:
        print(f"Erreur extraction catégorie : {e}")
        return None


def generate_page_urls(num_pages):
    """
    Génère les URLs des pages de listing de fugitifs à scraper.

    Le site FBI utilise une structure d'URL différente pour la première page
    (URL simple) et les pages suivantes (URL avec paramètres de pagination).

    Args:
        num_pages (int): Nombre de pages à générer (environ 40 profils/page)

    Returns:
        list: Liste des URLs à scraper

    Note:
        - Page 1 : URL simple sans paramètres
        - Pages 2+ : URL avec paramètres de pagination complexes
    """
    print(f"\nGénération des URLs pour {num_pages} page(s)")

    all_pages = []
    for page_num in range(1, num_pages + 1):
        if page_num == 1:
            # URL de la première page (sans paramètres)
            url = "https://www.fbi.gov/wanted/fugitives"
        else:
            # URL des pages suivantes avec paramètres de pagination
            url = f"https://www.fbi.gov/wanted/fugitives/@@castle.cms.querylisting/f7f80a1681ac41a08266bd0920c9d9d8?display_fields=%28%27image%27%2C%29&sort_on=modified&available_tags=%28u%27Crimes+Against+Children%27%2C+u%22Cyber%27s+Most+Wanted%22%2C+u%27White-Collar+Crime%27%2C+u%27Counterintelligence%27%2C+u%27Human+Trafficking%27%2C+u%27Criminal+Enterprise+Investigations%27%2C+u%27Violent+Crime+-+Murders%27%2C+u%27Additional+Violent+Crimes%27%29&query.v%3Arecords=%5Bu%27published%27%5D&query.o%3Arecords=plone.app.querystring.operation.selection.any&limit=40&_layouteditor=true&display_type=wanted-feature-grid&query.i%3Arecords=review_state&page={page_num}"

        all_pages.append(url)
        print(f"Page {page_num} : URL générée")

    print(f"{len(all_pages)} URLs générées")
    return all_pages


def extract_profile_urls_from_page(html_content, page_num):
    """
    Extrait les URLs des profils individuels depuis une page de listing.

    Parse le HTML de la page pour trouver tous les liens vers les profils
    de fugitifs. Ces liens sont contenus dans des balises <p class="name">.

    Args:
        html_content (str): Contenu HTML de la page
        page_num (int): Numéro de la page

    Returns:
        list: Liste des URLs absolues des profils trouvés

    Note:
        Retourne une liste vide en cas d'erreur de parsing
    """
    print(f"Extraction des URLs de profils pour la page {page_num}")

    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Les noms des fugitifs sont dans des balises <p class="name">
        name_tags = soup.find_all('p', class_='name')

        profile_urls = []
        for name_tag in name_tags:
            # Chaque nom contient un lien <a> vers le profil complet
            link_tag = name_tag.find('a')
            if link_tag and link_tag.get('href'):
                profile_url = link_tag['href']
                profile_urls.append(profile_url)

        print(f"{len(profile_urls)} URLs de profils extraites")
        return profile_urls
    except Exception as e:
        print(f"Erreur d'extraction : {e}")
        return []


def extract_profile_data(html_content, profile_url):
    """
    Extrait toutes les données structurées d'un profil de fugitif.

    Cette fonction parse la page HTML d'un profil individuel et extrait :
    - Les informations d'identification (nom, alias)
    - L'URL de la photo
    - Le tableau de description (âge, taille, poids, etc.)
    - Les remarques et avertissements
    - Le bureau FBI responsable

    Args:
        html_content (str): Contenu HTML de la page du profil
        profile_url (str): URL du profil (pour référence)

    Returns:
        dict: Dictionnaire contenant toutes les données extraites avec les clés :
            - url : URL du profil
            - category : Catégorie du crime
            - name : Nom
            - alias : Alias
            - image_url : URL de la photo
            - description : Dictionnaire des caractéristiques (taille, poids, etc.)
            - remarks : Remarques
            - caution : Caution
            - field_office : Bureau FBI qui recherche le fugitif

    Note:
        En cas d'erreur, retourne un dictionnaire avec l'URL et l'erreur
    """
    print(f"Extraction des données du profil : {profile_url}")

    try:
        soup = BeautifulSoup(html_content, "html.parser")

        # Initialisation de la structure des données
        profile_data = {
            "url": profile_url,
            "category": extract_category_from_url(profile_url),
            "name": None,
            "alias": None,
            "image_url": None,
            "description": {},
            "remarks": None,
            "caution": None,
            "field_office": None
        }

        # Extraction du nom
        name_tag = soup.find("h1", class_="documentFirstHeading")
        if name_tag:
            profile_data["name"] = name_tag.get_text(strip=True)

        # Extraction des alias
        alias_div = soup.find("div", class_="wanted-person-aliases")
        if alias_div:
            alias_p = alias_div.find("p")
            if alias_p:
                profile_data["alias"] = alias_p.get_text(strip=True)

        # Extraction de l'URL de l'image
        img_tag = soup.find("div", class_="col-md-4 wanted-person-mug")
        if img_tag:
            img = img_tag.find("img")
            if img and img.get("src"):
                image_url = img["src"].replace("/preview", "/large")
                profile_data["image_url"] = image_url

        # Extraction du tableau de description (caractéristiques physiques, etc)
        description_table = soup.find("table", class_="wanted-person-description")
        if description_table:
            for row in description_table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) >= 2:
                    # Colonne 0 = label, Colonne 1 = valeur
                    key = cols[0].get_text(strip=True).rstrip(":")
                    val = cols[1].get_text(" ", strip=True)
                    profile_data["description"][key] = val

        # Extraction des remarks
        remarks_div = soup.find("div", class_="wanted-person-remarks")
        if remarks_div:
            remarks_p = remarks_div.find("p")
            if remarks_p:
                profile_data["remarks"] = remarks_p.get_text(strip=True)

        # Extraction du caution (avec récompense)
        caution_div = soup.find("div", class_="wanted-person-caution")
        if caution_div:
            caution_p = caution_div.find("p")
            if caution_p:
                profile_data["caution"] = caution_p.get_text(strip=True)

        # Extraction du field office (bureau FBI qui recherche le fugitif)
        field_office_p = soup.find("p", class_="field-office-list")
        if field_office_p:
            field_office_span = field_office_p.find("span", class_="field-office")
            if field_office_span:
                field_office_link = field_office_span.find("a")
                if field_office_link:
                    profile_data["field_office"] = field_office_link.get_text(strip=True)

        print(f"Données extraites : {profile_data['name']}")
        return profile_data

    except Exception as e:
        print(f"Erreur lors de l'extraction : {e}")
        return {"url": profile_url, "error": str(e)}


def scrape_profile_urls(page_urls):
    """
    Scrape les URLs de tous les profils.

    Cette fonction itère sur toutes les pages et extrait
    les liens vers les profils individuels de fugitifs.

    Args:
        page_urls (list): Liste des URLs des pages de listing à scraper

    Returns:
        list: Liste consolidée de toutes les URLs de profils trouvées
    """
    print("\n" + "=" * 60)
    print("ÉTAPE 1 : RÉCUPÉRATION DES URL DE PROFILS")
    print("=" * 60)

    all_profile_urls = []

    for page_num, page_url in enumerate(page_urls, start=1):
        print(f"\nTraitement de la page {page_num}/{len(page_urls)}")

        # Scraping de la page
        response = crawl_get(page_url)
        if response and response.html:
            profile_urls = extract_profile_urls_from_page(response.html, page_num)
            all_profile_urls.extend(profile_urls)
            print(f"{len(profile_urls)} URLs extraites de cette page")
        else:
            print(f"Échec de l'extraction pour la page {page_num}")

        print(f"Attente de {DELAY_BETWEEN_REQUESTS} secondes...")
        time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"\nTOTAL : {len(all_profile_urls)} URLs de profils collectées")
    return all_profile_urls


def scrape_profiles_data(profile_urls):
    """
    Scrape les données de chaque profil de fugitif.

    Pour chaque URL de profil, récupère la page HTML
    et extrait toutes les informations du fugitif.

    Args:
        profile_urls (list): Liste des URLs des profils à scraper

    Returns:
        list: Liste de dictionnaires contenant les données de chaque profil

    Note:
        - Continue en cas d'erreur sur un profil individuel
    """
    print("\n" + "=" * 60)
    print("ÉTAPE 2 : EXTRACTION DES INFOS DE CHAQUE PROFIL")
    print("=" * 60)

    collected_data = []

    for i, profile_url in enumerate(profile_urls, start=1):
        print(f"\n[{i}/{len(profile_urls)}] Traitement du profil")

        try:
            # Scraping de la page du profil
            response = crawl_get(profile_url)
            if response and response.html:
                profile_data = extract_profile_data(response.html, profile_url)
                collected_data.append(profile_data)
                print("Profil traité avec succès")
            else:
                print("Échec de l'extraction pour ce profil")

        except Exception as e:
            print(f"Erreur lors du traitement : {e}")
            continue  # Continue avec le prochain profil

        print(f"Attente de {DELAY_BETWEEN_REQUESTS} secondes...")
        time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"\nTERMINÉ : {len(collected_data)} profils extraits avec succès")
    return collected_data


def save_data_to_json(collected_data, num_pages):
    """
    Sauvegarde les données brutes collectées au format JSON.

    Le fichier JSON conserve la structure complète des données,
    y compris le dictionnaire 'description' imbriqué. Format utile
    pour archivage ou traitement ultérieur.

    Args:
        collected_data (list): Liste des dictionnaires de profils
        num_pages (int): Nombre de pages scrapées (pour le nom du fichier)

    Returns:
        str or None: Nom du fichier créé, ou None en cas d'erreur
    """
    print("\n" + "=" * 60)
    print("SAUVEGARDE DES DONNÉES EN JSON")
    print("=" * 60)

    filename_json = f"fugitives_data_{num_pages}pages.json"
    print(f"Sauvegarde dans le fichier : {filename_json}")

    try:
        with open(filename_json, "w", encoding="utf-8") as f:
            json.dump(collected_data, f, ensure_ascii=False, indent=2)

        print(f"JSON sauvegardé avec succès : {filename_json}")
        print(f"{len(collected_data)} profils sauvegardés")
        return filename_json

    except Exception as e:
        print(f"Erreur lors de la sauvegarde JSON : {e}")
        return None


def save_dataframe_to_files(df, num_pages):
    """
    Sauvegarde le DataFrame brut en formats CSV et Excel.

    Crée deux fichiers de sortie :
    - CSV
    - Excel

    Args:
        df (pd.DataFrame): DataFrame à sauvegarder
        num_pages (int): Nombre de pages scrapées (pour le nom du fichier)

    Returns:
        tuple: (csv_filename, excel_filename) ou (csv_filename, None)
               si pas de fichier excel
    """
    print("\n" + "=" * 60)
    print("SAUVEGARDE DU DATAFRAME")
    print("=" * 60)

    # Sauvegarde CSV
    csv_filename = f"fugitives_dataframe_{num_pages}pages.csv"
    print(f"Sauvegarde CSV : {csv_filename}")
    try:
        df.to_csv(csv_filename, index=False, encoding='utf-8')
        print(f"CSV sauvegardé : {csv_filename}")
    except Exception as e:
        print(f"Erreur sauvegarde CSV : {e}")
        csv_filename = None

    # Sauvegarde Excel
    excel_filename = f"fugitives_dataframe_{num_pages}pages.xlsx"
    print(f"Sauvegarde Excel : {excel_filename}")
    try:
        df.to_excel(excel_filename, index=False)
        print(f"Excel sauvegardé : {excel_filename}")
    except Exception as e:
        print(f"Excel non disponible : {e}")
        print("Pour activer Excel : pip install openpyxl")
        excel_filename = None

    return csv_filename, excel_filename


# ============================================================================
# PARTIE 2 : NETTOYAGE DES DONNÉES
# ============================================================================

# Dictionnaire pour catégoriser les occupations
OCCUPATION_KEYWORDS = {
    'Military/Intelligence': [
        'officer', 'gru', 'intelligence', 'military', 'fsb', 'security service',
        'syrian air force', 'army', 'navy', 'marine', 'directorate', 'general staff'
    ],
    'Medical': [
        'doctor', 'surgeon', 'physician', 'nurse', 'nursing', 'therapist',
        'cardiologist', 'medical', 'catheter', 'respiratory', 'chiropractor',
        'acupuncturist', 'chemist', 'pharmaceutical'
    ],
    'IT/Technology': [
        'it worker', 'software', 'programmer', 'developer', 'engineer', 'computer',
        'network', 'blockchain', 'backend', 'zksnark', 'technology', 'web'
    ],
    'Construction': [
        'construction', 'carpenter', 'welder', 'electrician', 'plumber',
        'mason', 'painter', 'handyman', 'builder', 'contractor', 'tractor driver',
        'floor sander'
    ],
    'Transportation': [
        'driver', 'truck', 'taxi', 'cab', 'mechanic', 'aviation', 'pilot',
        'shipping', 'boat', 'ship'
    ],
    'Food Service': [
        'cook', 'chef', 'waiter', 'restaurant', 'food service', 'kitchen'
    ],
    'Business/Management': [
        'ceo', 'president', 'director', 'manager', 'executive', 'businessman',
        'entrepreneur', 'owner', 'operator', 'consultant'
    ],
    'Finance/Trading': [
        'broker', 'trader', 'commodities', 'investment', 'banking', 'finance',
        'advisor', 'asset manager'
    ],
    'Government': [
        'government', 'official', 'diplomat', 'service officer', 'taxation',
        'commission', 'delegation'
    ],
    'Law Enforcement': [
        'police', 'constable', 'dispatcher', 'security', 'law enforcement'
    ],
    'Agriculture': [
        'farm', 'agriculture', 'laborer', 'migrant', 'seed', 'rice', 'landscaping'
    ],
    'Sales/Retail': [
        'sales', 'salesman', 'retail', 'store', 'clerk', 'dealer'
    ],
    'Trade/Manufacturing': [
        'warehouse', 'factory', 'manufacturing', 'production', 'textile',
        'procurement', 'arms broker'
    ],
    'Religious': [
        'priest', 'pastor', 'religious', 'clergy', 'minister'
    ],
    'Services': [
        'massage', 'nail technician', 'barber', 'salon', 'spa'
    ],
    'Education/Research': [
        'teacher', 'professor', 'researcher', 'education', 'academic'
    ],
    'Emergency Services': [
        'fireman', 'firefighter', 'paramedic', 'emt', 'rescue'
    ],
    'Entertainment': [
        'recording studio', 'amusement', 'arcade', 'game'
    ],
    'Unknown': [
        'unknown', 'unemployed', 'none', 'n/a'
    ]
}

# Dictionnaire des lieux de naissance vers (pays, code ISO)
BIRTH_PLACE_MAPPING = {
    'Los Angeles, California': ('United States', 'USA'),
    'Pasadena, California': ('United States', 'USA'),
    'Arcadia, California': ('United States', 'USA'),
    'Sacramento, California': ('United States', 'USA'),
    'San Francisco, California': ('United States', 'USA'),
    'Fresno, California': ('United States', 'USA'),
    'California': ('United States', 'USA'),
    'Brooklyn, New York': ('United States', 'USA'),
    'New York City, New York': ('United States', 'USA'),
    'New York': ('United States', 'USA'),
    'El Paso, Texas': ('United States', 'USA'),
    'Memphis, Tennessee': ('United States', 'USA'),
    'Mobile, Alabama': ('United States', 'USA'),
    'Selma, Alabama': ('United States', 'USA'),
    'Alabama': ('United States', 'USA'),
    'Springfield, Illinois': ('United States', 'USA'),
    'Olney, Illinois': ('United States', 'USA'),
    'Illinois': ('United States', 'USA'),
    'Detroit, Michigan': ('United States', 'USA'),
    'Michigan, USA': ('United States', 'USA'),
    'Miami, Florida': ('United States', 'USA'),
    'Florida': ('United States', 'USA'),
    'New Jersey': ('United States', 'USA'),
    'Virginia': ('United States', 'USA'),
    'Oregon': ('United States', 'USA'),
    'Pennsylvania': ('United States', 'USA'),
    'North Dakota': ('United States', 'USA'),
    'Louisiana': ('United States', 'USA'),
    'Massachusetts': ('United States', 'USA'),
    'Hawaii': ('United States', 'USA'),
    'Idaho': ('United States', 'USA'),
    'Ohio': ('United States', 'USA'),
    'Wadesboro, North Carolina': ('United States', 'USA'),
    'Washington, DC': ('United States', 'USA'),
    'Wayne, Pennsylvania': ('United States', 'USA'),
    'Hunan Province, China': ('China', 'CHN'),
    'Liaoning, China': ('China', 'CHN'),
    'Hangzhou, Zhejiang Province, China': ('China', 'CHN'),
    'Heilongjiang, China': ('China', 'CHN'),
    'Wusu, Xinjiang, China': ('China', 'CHN'),
    'Anhui, China': ('China', 'CHN'),
    'Tacheng, Xinjiang, China or Urumqi, Xinjiang, China': ('China', 'CHN'),
    'Shaanxi, China': ('China', 'CHN'),
    'Shandong, China': ('China', 'CHN'),
    'Shanghai, China': ('China', 'CHN'),
    'Weifang, Shandong, China': ('China', 'CHN'),
    'Zhejiang, China': ('China', 'CHN'),
    'China': ('China', 'CHN'),
    'People\'s Republic of China': ('China', 'CHN'),
    'Chelyabinskaya Oblast, Russia': ('Russia', 'RUS'),
    'Tver, Russia': ('Russia', 'RUS'),
    'Stavropol, Russia': ('Russia', 'RUS'),
    'Novocherkask, Russia': ('Russia', 'RUS'),
    'Volzhskiy, Volgogradskaya Oblast, Russia': ('Russia', 'RUS'),
    'Saint Petersburg, Russia': ('Russia', 'RUS'),
    'Murmanskaya Oblast, Russia': ('Russia', 'RUS'),
    'Sosnovka, Russia': ('Russia', 'RUS'),
    'Moscow, Russia': ('Russia', 'RUS'),
    'Village of Fenino, Serpukhovskoy District, Moscow Oblast, Russia': ('Russia', 'RUS'),
    'Totma, Vologda Oblast, Russia': ('Russia', 'RUS'),
    'Tymovskoye, Russia': ('Russia', 'RUS'),
    'Leningrad, Russia': ('Russia', 'RUS'),
    'Kaluga, Russia': ('Russia', 'RUS'),
    'City of Syktyvkar, Russia': ('Russia', 'RUS'),
    'Ramenskoye, Russia': ('Russia', 'RUS'),
    'Grozny, Chechnya, Russia': ('Russia', 'RUS'),
    'Rostov-On-Don, Russia': ('Russia', 'RUS'),
    'Khaborovsk, Russia': ('Russia', 'RUS'),
    'Obninsk, Kaluga Oblast, Russia': ('Russia', 'RUS'),
    'Vologda, Russia': ('Russia', 'RUS'),
    'Bratsk, Irkutsk Oblast, Russia': ('Russia', 'RUS'),
    'Kursk, Russia': ('Russia', 'RUS'),
    'Yoshkar-Ola, Russia': ('Russia', 'RUS'),
    'Stavropolskiy Kraya, Russia': ('Russia', 'RUS'),
    'Bologoe-4, Kalininskiy Oblast, Russia': ('Russia', 'RUS'),
    'Russia': ('Russia', 'RUS'),
    'Russian Federation': ('Russia', 'RUS'),
    'Zabol, Iran': ('Iran', 'IRN'),
    'Tehran, Iran': ('Iran', 'IRN'),
    'Tabriz, Iran': ('Iran', 'IRN'),
    'Zanjan, Iran': ('Iran', 'IRN'),
    'Urmia, Iran': ('Iran', 'IRN'),
    'Yazd Province, Iran': ('Iran', 'IRN'),
    'Sabzevar, Iran': ('Iran', 'IRN'),
    'Tehran Province, Iran': ('Iran', 'IRN'),
    'Karaj, Iran': ('Iran', 'IRN'),
    'Mianeh, Iran': ('Iran', 'IRN'),
    'Naghadeh, Iran': ('Iran', 'IRN'),
    'Ilam, Iran': ('Iran', 'IRN'),
    'Mashhad, Iran': ('Iran', 'IRN'),
    'Ardabil, Iran': ('Iran', 'IRN'),
    'Iran': ('Iran', 'IRN'),
    'Nayarit, Mexico': ('Mexico', 'MEX'),
    'Sinaloa, Mexico': ('Mexico', 'MEX'),
    'Baja California, Mexico': ('Mexico', 'MEX'),
    'Jalisco, Mexico': ('Mexico', 'MEX'),
    'Veracruz, Mexico': ('Mexico', 'MEX'),
    'Jerez, Zacatecas, Mexico': ('Mexico', 'MEX'),
    'Chuicopa, Sinaloa, Mexico': ('Mexico', 'MEX'),
    'Mezquital del Oro, Zacatecas, Mexico': ('Mexico', 'MEX'),
    'Zacatecas, Mexico': ('Mexico', 'MEX'),
    'Colima, Mexico': ('Mexico', 'MEX'),
    'Mexico City, Mexico': ('Mexico', 'MEX'),
    'Hidalgo, Mexico': ('Mexico', 'MEX'),
    'Durango, Mexico': ('Mexico', 'MEX'),
    'Mexico': ('Mexico', 'MEX'),
    'Jucuaran, Usulutan, El Salvador': ('El Salvador', 'SLV'),
    'San Francisco Menendez, Ahuachapan, El Salvador': ('El Salvador', 'SLV'),
    'San Salvador, San Salvador, El Salvador': ('El Salvador', 'SLV'),
    'Ozatlan, Usulutan, El Salvador': ('El Salvador', 'SLV'),
    'Ahuachapan, Ahuachapan, El Salvador': ('El Salvador', 'SLV'),
    'Tejutla, Chalatenango, El Salvador': ('El Salvador', 'SLV'),
    'Cuscatancingo, San Salvador, El Salvador': ('El Salvador', 'SLV'),
    'Usulutan, Usulutan, El Salvador': ('El Salvador', 'SLV'),
    'El Salvador': ('El Salvador', 'SLV'),
    'Pyongyang, North Korea': ('North Korea', 'PRK'),
    'Democratic People\'s Republic of Korea (North Korea)': ('North Korea', 'PRK'),
    'Kryvyi Rih, Dnipropetrovsk Oblast, Ukraine': ('Ukraine', 'UKR'),
    'Kyiv, Ukraine': ('Ukraine', 'UKR'),
    'Kiev, Ukraine': ('Ukraine', 'UKR'),
    'Boryspil, Kyiv Oblast, Ukraine': ('Ukraine', 'UKR'),
    'Ukraine': ('Ukraine', 'UKR'),
    'Homs, Syria': ('Syria', 'SYR'),
    'Damascus, Syria': ('Syria', 'SYR'),
    'Allepo, Syria': ('Syria', 'SYR'),
    'Syria': ('Syria', 'SYR'),
    'San Juan, Puerto Rico': ('Puerto Rico', 'PRI'),
    'Lajas, Puerto Rico': ('Puerto Rico', 'PRI'),
    'Aguada, Puerto Rico': ('Puerto Rico', 'PRI'),
    'Pakistan': ('Pakistan', 'PAK'),
    'Pranpura, Haryana, India': ('India', 'IND'),
    'Hyderabad, Pakistan': ('Pakistan', 'PAK'),
    'Karachi, Pakistan': ('Pakistan', 'PAK'),
    'India': ('India', 'IND'),
    'Honduras': ('Honduras', 'HND'),
    'Atlantida, Honduras': ('Honduras', 'HND'),
    'Copan, Honduras': ('Honduras', 'HND'),
    'Cambodia': ('Cambodia', 'KHM'),
    'Uzbekistan': ('Uzbekistan', 'UZB'),
    'Toy Teipa, Uzbekistan': ('Uzbekistan', 'UZB'),
    'Haiti': ('Haiti', 'HTI'),
    'Nigeria': ('Nigeria', 'NGA'),
    'Minsk, Belarus': ('Belarus', 'BLR'),
    'Jamaica': ('Jamaica', 'JAM'),
    'Santiago, Dominican Republic': ('Dominican Republic', 'DOM'),
    'Dominican Republic': ('Dominican Republic', 'DOM'),
    'La Calera, Chile': ('Chile', 'CHL'),
    'LaGuaira, Venezuela': ('Venezuela', 'VEN'),
    'Venezuela': ('Venezuela', 'VEN'),
    'Bangladesh': ('Bangladesh', 'BGD'),
    'Riga, Latvia': ('Latvia', 'LVA'),
    'Vietnam': ('Vietnam', 'VNM'),
    'Republic of Vietnam': ('Vietnam', 'VNM'),
    'Mong Cai, Quang Ninh Province, North Vietnam': ('Vietnam', 'VNM'),
    'Quang Binh Province, Vietnam': ('Vietnam', 'VNM'),
    'Sweden': ('Sweden', 'SWE'),
    'Canada': ('Canada', 'CAN'),
    'Bel Ombre, Mahe Island, Seychelles': ('Seychelles', 'SYC'),
    'Spain': ('Spain', 'ESP'),
    'Cuba': ('Cuba', 'CUB'),
    'Turkey': ('Turkey', 'TUR'),
    'Armenia': ('Armenia', 'ARM'),
    'Ghana': ('Ghana', 'GHA'),
    'Ecuador': ('Ecuador', 'ECU'),
    'Guayaquil, Ecuador': ('Ecuador', 'ECU'),
    'Brazil': ('Brazil', 'BRA'),
    'Laos': ('Laos', 'LAO'),
    'Philippines': ('Philippines', 'PHL'),
    'Ilocos Norte, Philippines': ('Philippines', 'PHL'),
    'Germany': ('Germany', 'DEU'),
    'Sydney, Australia': ('Australia', 'AUS'),
    'Sumqayit, Azerbaijan': ('Azerbaijan', 'AZE'),
    'Muscat, Oman': ('Oman', 'OMN'),
    'United Kingdom': ('United Kingdom', 'GBR'),
    'Batroun, Lebanon': ('Lebanon', 'LBN'),
    'Guatemala': ('Guatemala', 'GTM'),
    'Unknown': (None, None),
    '': (None, None),
}


def count_aliases(text):
    """
    Compte le nombre d'alias d'un fugitif.

    Parse le texte des alias en séparant selon différents délimiteurs
    (aka, |, virgule, point-virgule, retour à la ligne) et compte
    les alias valides trouvés.

    Args:
        text (str or None): Texte contenant les alias

    Returns:
        int: Nombre d'alias distincts (0 si aucun ou texte vide)
    """
    if pd.isna(text) or not str(text).strip():
        return 0

    # Normalisation : remplacer "a/k/a" par "aka"
    s = str(text).strip().replace("a/k/a", "aka")

    # Séparation selon différents délimiteurs
    parts = re.split(r'(?:\saka\s|\s*\|\s*|,\s*|;\s*|\n+)', s, flags=re.IGNORECASE)

    # Filtrage des valeurs invalides
    parts = [p.strip() for p in parts if p and p.strip() not in {"-", "None", "N/A", "n/a"}]
    return len(parts)


def _parse_date_with_format(text, fmt):
    """
    Tente de parser une date avec un format spécifique.

    Args:
        text (str): Chaîne de texte contenant une date
        fmt (str): Format de date à utiliser

    Returns:
        date or None: Date parsée, ou None si échec
    """
    try:
        return datetime.strptime(text, fmt).date()
    except ValueError:
        return None


def _parse_date_with_regex(text, pattern, year_group=2, month_group=0, day_group=1):
    """
    Tente de parser une date avec une expression régulière.

    Args:
        text (str): Chaîne de texte contenant une date
        pattern (str): Expression régulière à utiliser
        year_group (int): Index du groupe contenant l'année
        month_group (int): Index du groupe contenant le mois
        day_group (int): Index du groupe contenant le jour

    Returns:
        date or None: Date parsée, ou None si échec
    """
    match = re.search(pattern, text)
    if match:
        groups = match.groups()
        try:
            # Gérer l'ambiguïté des années à deux chiffres
            year = int(groups[year_group])
            if len(str(year)) == 2:
                year += 1900 if year > 25 else 2000

            return date(year, int(groups[month_group]), int(groups[day_group]))
        except (ValueError, IndexError):
            pass
    return None


def parse_date(text):
    """
    Extrait la première date valide d'une chaîne de texte.

    Args:
        text (str): Chaîne contenant une ou plusieurs dates

    Returns:
        date or None: Première date valide trouvée
    """
    if pd.isna(text) or not str(text).strip():
        return None

    text = str(text).strip()

    # Formats de date courants avec parsing direct
    date_formats = [
        "%B %d, %Y",   # "April 13, 1961"
        "%b %d, %Y",   # "Apr 13, 1961"
        "%m/%d/%Y",    # "04/13/1961"
        "%m/%d/%y",    # "04/13/61"
        "%Y-%m-%d",    # "1961-04-13"
        "%d %B %Y",    # "13 April 1961"
        "%d %b %Y"     # "13 Apr 1961"
    ]

    # Essayer de parser avec différents formats
    for fmt in date_formats:
        parsed_date = _parse_date_with_format(text, fmt)
        if parsed_date:
            return parsed_date

    # Formats spéciaux avec regex
    date_patterns = [
        # YYYY-MM-DD
        (r'\b(\d{4})-(\d{1,2})-(\d{1,2})\b', 0, 1, 2),
        # MM/DD/YYYY ou DD/MM/YYYY
        (r'\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b', 2, 0, 1)
    ]

    for pattern, year_group, month_group, day_group in date_patterns:
        parsed_date = _parse_date_with_regex(text, pattern, year_group, month_group, day_group)
        if parsed_date:
            return parsed_date

    return None


def compute_age(birth_date, ref_date=TODAY):
    """
    Calcule l'âge à partir de la date de naissance.

    Args:
        birth_date (date): Date de naissance
        ref_date (date, optional): Date de référence.

    Returns:
        int or None: Âge calculé, ou None si impossible
    """
    if birth_date is None:
        return None

    years = ref_date.year - birth_date.year
    if (ref_date.month, ref_date.day) < (birth_date.month, birth_date.day):
        years -= 1

    return years if years >= 0 else None


def zodiac_sign(birth_date):
    """
    Détermine le signe astrologique.

    Args:
        birth_date (date or None): Date de naissance

    Returns:
        str or None: Signe astrologique
    """
    if birth_date is None or pd.isna(birth_date):
        return None

    month, day = birth_date.month, birth_date.day

    # Définition des intervalles des signes astrologiques
    zodiac_ranges = [
        ((1, 20), (2, 18), "Aquarius"),
        ((2, 19), (3, 20), "Pisces"),
        ((3, 21), (4, 19), "Aries"),
        ((4, 20), (5, 20), "Taurus"),
        ((5, 21), (6, 20), "Gemini"),
        ((6, 21), (7, 22), "Cancer"),
        ((7, 23), (8, 22), "Leo"),
        ((8, 23), (9, 22), "Virgo"),
        ((9, 23), (10, 22), "Libra"),
        ((10, 23), (11, 21), "Scorpio"),
        ((11, 22), (12, 21), "Sagittarius"),
        (None, None, "Capricorn")  # Dernier signe par défaut
    ]

    for start, end, sign in zodiac_ranges:
        if start is None or (month == start[0] and day >= start[1]) or \
           (month == end[0] and day <= end[1]):
            return sign

    return None


def parse_height_to_cm(height):
    """
    Convertit une taille de différents formats en centimètres.

    Gère plusieurs formats de taille :
    - Centimètres : "180 cm", "180cm"
    - Mètres : "1.80 m", "1.8m"
    - Pouces seuls : "71 inches", "71""
    - Pieds et pouces : "5'11", "5 ft 11 in", "5 feet 11 inches"
    - Valeurs numériques brutes (avec détection du format probable)

    Args:
        height (str or None): Texte contenant la taille

    Returns:
        float or None: Taille en centimètres, ou None si impossible à parser

    Note:
        Les valeurs ambiguës sont interprétées selon des plages logiques
        (ex: 50-90 considéré comme pouces, 140-230 comme cm)
    """
    if pd.isna(height) or not str(height).strip():
        return None

    s = str(height).lower().strip()

    # Format centimètres (déjà dans l'unité cible)
    m_cm = re.search(r'(\d{2,3}(?:\.\d+)?)\s*cm\b', s)
    if m_cm:
        return float(m_cm.group(1))

    # Format mètres (conversion × 100)
    m_m = re.search(r'(\d(?:\.\d{1,2})?)\s*m\b', s)
    if m_m:
        val_m = float(m_m.group(1))
        # Validation : taille humaine raisonnable en mètres
        if 1.3 <= val_m <= 2.5:
            return round(val_m * 100.0, 1)

    # Format pouces seuls
    # Uniquement si pas de mention de pieds dans la chaîne
    m_in = re.search(r'(\d{2,3}(?:\.\d+)?)\s*(?:in|inch|inches|")\b', s)
    if m_in and not re.search(r'\bft|foot|feet|\'\b', s):
        return round(float(m_in.group(1)) * 2.54, 1)

    # Format pieds et pouces
    # Exemples : "5'11", "5 ft 11", "6 feet 2 inches"
    m_ft_in = re.search(r'(?:(\d+)\s*(?:ft|foot|feet|\' ))\s*(\d{1,2})?', s) or \
              re.search(r'(\d+)\s*\'\s*(\d{1,2})?', s)
    if m_ft_in:
        ft = int(m_ft_in.group(1))
        inch = int(m_ft_in.group(2)) if m_ft_in.group(2) else 0
        # Conversion : pieds → pouces → cm
        return round((ft * 12 + inch) * 2.54, 1)

    # Format numérique
    m_num = re.search(r'(\d{1,3}(?:\.\d+)?)', s)
    if m_num:
        val = float(m_num.group(1))
        # 50-90 : pouces
        if 50 <= val <= 90:
            return round(val * 2.54, 1)
        # 1.4-2.5 : mètres
        if 1.4 <= val <= 2.5:
            return round(val * 100, 1)
        # 140-230 : centimètres
        if 140 <= val <= 230:
            return round(val, 1)

    return None


def parse_weight_to_kg(weight):
    # Convertir le poids en kg
    if pd.isna(weight) or not str(weight).strip():
        return None

    s = str(weight).lower()

    # Format kg
    m_kg = re.findall(r'(\d+(?:\.\d+)?)\s*kg\b', s)
    if m_kg:
        vals = list(map(float, m_kg))
        return round(sum(vals) / len(vals), 1)

    # Format livres
    nums = re.findall(r'(\d+(?:\.\d+)?)', s)
    if nums:
        vals = list(map(float, nums))
        val_lb = (min(vals) + max(vals)) / 2.0 if len(vals) >= 2 else vals[0]
        return round(val_lb * 0.45359237, 1)

    return None


def categorize_occupation(text):
    # Catégoriser les métiers
    if pd.isna(text) or not str(text).strip():
        return 'Unknown'

    text_lower = str(text).lower()
    for category, keywords in OCCUPATION_KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in text_lower:
                return category
    return 'Other'


def extract_country_from_birth_place(place):
    # Extraire le pays et le code ISO à partir du lieu de naissance
    if pd.isna(place) or not str(place).strip():
        return (None, None)

    place = str(place).strip()
    if place in BIRTH_PLACE_MAPPING:
        return BIRTH_PLACE_MAPPING[place]
    return (place, None)


def split_languages(text):
    # Séparer les langues en 3 colonnes
    if pd.isna(text) or not str(text).strip():
        return (None, None, None)

    text = str(text).strip()
    languages = re.split(r'[,;/]|\s+and\s+', text, flags=re.IGNORECASE)
    languages = [lang.strip() for lang in languages if lang.strip()]

    lang1 = languages[0] if len(languages) > 0 else None
    lang2 = languages[1] if len(languages) > 1 else None
    lang3 = languages[2] if len(languages) > 2 else None

    return (lang1, lang2, lang3)


def extract_first_hair_color(hair):
    # Extrait la première couleur de cheveux
    if pd.isna(hair) or not str(hair).strip():
        return None

    hair = str(hair).strip()
    hair_clean = re.split(r'[/\(]|\s+and\s+', hair, flags=re.IGNORECASE)[0].strip()
    return hair_clean if hair_clean else None


def detect_scars_marks(text):
    # Présence de signes distinctifs ou pas
    if pd.isna(text) or not str(text).strip():
        return False

    s = str(text).lower()
    scar_keywords = [
        r'\btattoo|ink\b',
        r'\bscar(s)?\b',
        r'\bpierc(ed|ing|ings)\b',
        r'\bburn(ed|s| mark)\b',
        r'\bmissing\s+finger(s)?\b',
        r'\bfreckle(s)?\b', r'\bmole\b',
        r'\bbirthmark\b',
    ]

    for pattern in scar_keywords:
        if re.search(pattern, s):
            return True
    return False


def detect_dollar_amounts(text):
    # Détecte la présence de montants en dollars
    if pd.isna(text) or not str(text).strip():
        return False

    return bool(re.search(r'\$\s*[\d,]+(?:\.\d{1,2})?', str(text)))


def process_aliases(df):
    # Compte le nombre d'alias
    if "alias" in df.columns:
        df["alias_count"] = df["alias"].apply(count_aliases).astype("Int64")
        print("Alias comptés")
    return df


def process_dates_and_ages(df):
    """
    Traite les dates de naissance, calcule l'âge et le signe astrologique.

    Args:
        df (pd.DataFrame): DataFrame à traiter

    Returns:
        pd.DataFrame: DataFrame avec colonnes de date et d'âge mises à jour
    """
    dob_col = next((c for c in ["Date(s) of Birth Used", "Date of Birth", "DOB"] if c in df.columns), None)

    if dob_col:
        # Nettoyer et extraire la première date
        df[dob_col] = df[dob_col].apply(parse_date)
        print(f"Colonne '{dob_col}' nettoyée")

        # Calculer l'âge
        df['age_years'] = df[dob_col].apply(compute_age).astype("Int64")
        missing_after = df['age_years'].isna().sum()
        print(f"Âges calculés, {missing_after} valeurs manquantes")

        # Calculer le signe astrologique
        df['zodiac_sign'] = df[dob_col].apply(zodiac_sign)
        zodiac_count = df['zodiac_sign'].notna().sum()
        print(f"Signes astro : {zodiac_count} valeurs")

    return df


def process_occupations(df):
    """Catégorise les occupations."""
    if "Occupation" in df.columns:
        df['occupation_category'] = df['Occupation'].apply(categorize_occupation)
        print("Occupations catégorisées")

        cat_counts = df['occupation_category'].value_counts().sort_values(ascending=False)
        print("Top 5 catégories :")
        for cat, count in cat_counts.head(5).items():
            print(f"    {cat}: {count}")

    return df


def process_birth_places(df):
    """Extraire les pays de naissance."""
    if "Place of Birth" in df.columns:
        country_data = df['Place of Birth'].apply(extract_country_from_birth_place)
        df['birth_country'] = country_data.apply(lambda x: x[0])
        df['birth_country_code'] = country_data.apply(lambda x: x[1])

        country_count = df['birth_country'].notna().sum()
        code_count = df['birth_country_code'].notna().sum()

        print(f"Pays de naissance : {country_count} valeurs")
        print(f"Codes ISO : {code_count}")

    return df


def process_languages(df):
    """Split les langues en 3 colonnes."""
    if "Languages" in df.columns:
        lang_split = df["Languages"].apply(split_languages)
        df["Language_1"] = lang_split.apply(lambda x: x[0])
        df["Language_2"] = lang_split.apply(lambda x: x[1])
        df["Language_3"] = lang_split.apply(lambda x: x[2])

        has_lang1 = df["Language_1"].notna().sum()
        print(f"Langues splitées : {has_lang1} valeurs")

    return df


def process_hair_color(df):
    """Extrait la première couleur de cheveux."""
    if "Hair" in df.columns:
        df['hair_color'] = df['Hair'].apply(extract_first_hair_color)
        hair_count = df['hair_color'].notna().sum()
        print(f"Couleur de cheveux : {hair_count} valeurs")

    return df


def process_physical_measurements(df):
    """Traite les caractéristiques physiques - height et weight."""
    if "Height" in df.columns:
        df["height_cm"] = df["Height"].apply(parse_height_to_cm)
        height_count = df["height_cm"].notna().sum()
        print(f"Height converti : {height_count} valeurs")

    if "Weight" in df.columns:
        df["weight_kg"] = df["Weight"].apply(parse_weight_to_kg)
        weight_count = df["weight_kg"].notna().sum()
        print(f"Weight converti : {weight_count} valeurs")

    return df


def process_marks_and_caution(df):
    """Traite les marques et caution en booléens."""
    if "Scars and Marks" in df.columns:
        df["has_mark"] = df["Scars and Marks"].apply(detect_scars_marks).astype('bool')
        mark_count = df["has_mark"].sum()
        print(f"Scars/Marks (booléen) : {mark_count} avec marques")

    if "caution" in df.columns:
        df["has_caution"] = df["caution"].apply(detect_dollar_amounts).astype('bool')
        caution_count = df["has_caution"].sum()
        print(f"Caution (booléen) : {caution_count} avec montants")

    return df


def remove_unused_columns(df):
    """Supprime les colonnes inutiles/redondantes."""
    cols_to_drop = [
        "image_url", "remarks", "race", "Race", "caution",
        "Height", "Weight", "Scars and Marks", "Build",
        "Citizenship", "Complexion", "Age",
        "Languages", "Occupation", "Place of Birth", "Hair"
    ]

    dropped = []
    for col in cols_to_drop:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
            dropped.append(col)

    if dropped:
        print(f"{len(dropped)} colonnes supprimées")

    return df


def rename_and_reorganize_columns(df):
    """Renomme et réorganise les colonnes."""
    print("\n" + "=" * 60)
    print("FINALISATION DU DATASET")
    print("=" * 60)

    rename_map = {
        'Date(s) of Birth Used': 'date_of_birth',
        'Eyes': 'eye_color',
        'Sex': 'sex',
        'Nationality': 'nationality',
        'NCIC': 'ncic_number',
        'alias': 'alias_text',
        'field_office': 'fbi_field_office',
        'Language_1': 'language_primary',
        'Language_2': 'language_secondary',
        'Language_3': 'language_tertiary'
    }

    df = df.rename(columns=rename_map)
    print(f"{len(rename_map)} colonnes renommées")

    # Réorganiser les colonnes par thème
    column_order = [
        # Identification
        'url', 'name', 'alias_text', 'alias_count', 'ncic_number', 'category',

        # Démographie
        'date_of_birth', 'age_years', 'zodiac_sign', 'sex', 'nationality',
        'birth_country', 'birth_country_code',

        # Description physique
        'height_cm', 'weight_kg', 'eye_color', 'hair_color', 'has_mark',

        # Langues
        'language_primary', 'language_secondary', 'language_tertiary',

        # Profession
        'occupation_category',

        # FBI
        'fbi_field_office', 'has_caution'
    ]

    # Garder seulement les colonnes qui existent
    column_order = [col for col in column_order if col in df.columns]

    # Ajouter les colonnes manquantes à la fin
    for col in df.columns:
        if col not in column_order:
            column_order.append(col)

    df = df[column_order]
    print("Colonnes réorganisées par thème")

    # Vérifier les booléens
    bool_cols = ['has_mark', 'has_caution']
    for col in bool_cols:
        if col in df.columns:
            print(f"  {col}: {df[col].dtype}")

    return df


def clean_dataframe(df):
    """Nettoie le df."""
    print("\n" + "=" * 60)
    print("NETTOYAGE ET ENRICHISSEMENT DES DONNÉES")
    print("=" * 60)

    out = df.copy()

    # Traitement par étapes
    out = process_aliases(out)
    out = process_dates_and_ages(out)
    out = process_occupations(out)
    out = process_birth_places(out)
    out = process_languages(out)
    out = process_hair_color(out)
    out = process_physical_measurements(out)
    out = process_marks_and_caution(out)
    out = remove_unused_columns(out)
    out = rename_and_reorganize_columns(out)

    return out


def save_cleaned_dataframe(df, num_pages):
    """Sauvegarde le df nettoyé."""
    print("\n" + "=" * 60)
    print("SAUVEGARDE DU DATASET NETTOYÉ")
    print("=" * 60)

    # Sauvegarde CSV
    csv_filename_cleaned = f"fugitives_cleaned_{num_pages}pages.csv"
    df.to_csv(csv_filename_cleaned, index=False, encoding='utf-8')
    print(f"CSV nettoyé sauvegardé : {csv_filename_cleaned}")

    # Excel (optionnel)
    try:
        excel_filename = f"fugitives_cleaned_{num_pages}pages.xlsx"
        df.to_excel(excel_filename, index=False)
        print(f"Excel sauvegardé : {excel_filename}")
    except Exception as e:
        print(f"Excel non disponible : {e}")
        print("Pour activer Excel : pip install openpyxl")

    return csv_filename_cleaned


def verify_final_dataset(df):
    """Vérifie le dataset final."""
    print("\n" + "=" * 60)
    print("VÉRIFICATION DES TYPES")
    print("=" * 60)

    # Vérifier les types
    for col in df.columns:
        dtype = df[col].dtype
        emoji = "🟢" if dtype in ['bool', 'int64', 'float64', 'Int64'] else "🟡"
        print(f"{emoji} {col:30s} : {dtype}")

    print("\n" + "=" * 60)
    print("EXEMPLES DE VALEURS BOOLÉENNES")
    print("=" * 60)

    if 'has_mark' in df.columns:
        print("\nhas_mark (booléen) :")
        print(df['has_mark'].value_counts())
        print(f"Type: {df['has_mark'].dtype}")

    if 'has_caution' in df.columns:
        print("\nhas_caution (booléen) :")
        print(df['has_caution'].value_counts())
        print(f"Type: {df['has_caution'].dtype}")

    print("\n" + "=" * 60)
    print("RÉSUMÉ FINAL")
    print("=" * 60)
    print(f"Dataset nettoyé : {df.shape[0]} lignes, {df.shape[1]} colonnes")

# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================


def create_raw_dataframe(collected_data):
    """Crée le DataFrame brut avec aplatissement de la description."""
    print("\n" + "=" * 60)
    print("CRÉATION DU DATAFRAME BRUT")
    print("=" * 60)

    # Créer le df à partir de collected
    df = pd.DataFrame(collected_data)
    print(f"DataFrame créé : {df.shape[0]} lignes, {df.shape[1]} colonnes")

    # Aplatir la colonne 'description'
    if 'description' in df.columns:
        print("Aplatissement de la colonne 'description'...")
        df_description = pd.json_normalize(df['description'])
        df = df.drop('description', axis=1)
        df = pd.concat([df, df_description], axis=1)
        print(f"Colonne 'description' aplatie : {len(df_description.columns)} nouvelles colonnes")

    print(f"DataFrame final : {df.shape[0]} lignes, {df.shape[1]} colonnes")

    return df


def main():
    """Fonction principale qui orchestre tout le processus."""
    print("=" * 60)
    print("DÉMARRAGE DU SCRAPER FBI FUGITIVES")
    print("=" * 60)

    # Configuration
    print(f"Configuration : {NOMBRE_PAGES} page(s) à scraper")
    print(f"Estimation : environ {NOMBRE_PAGES * 40} profils")

    # Configuration du proxy
    if not setup_proxy():
        print("Impossible de configurer le proxy. Arrêt du programme.")
        return

    # Génération des URLs
    page_urls = generate_page_urls(NOMBRE_PAGES)

    # Scraping des URLs de profils
    profile_urls = scrape_profile_urls(page_urls)

    if not profile_urls:
        print("Aucun profil trouvé. Arrêt du programme.")
        return

    # Scraping des données des profils
    collected_data = scrape_profiles_data(profile_urls)

    if not collected_data:
        print("Aucune donnée collectée. Arrêt du programme.")
        return

    # Sauvegarde JSON
    json_file = save_data_to_json(collected_data, NOMBRE_PAGES)

    # Création du df brut
    df_raw = create_raw_dataframe(collected_data)

    # Sauvegarde CSV et excel du df brut
    csv_file, excel_file = save_dataframe_to_files(df_raw, NOMBRE_PAGES)

    # Nettoyage des données
    cleaned_df = clean_dataframe(df_raw)

    # Sauvegarde finale
    cleaned_csv = save_cleaned_dataframe(cleaned_df, NOMBRE_PAGES)

    # Vérification finale
    verify_final_dataset(cleaned_df)

    print("\nTERMINÉ ! Fichiers créés :")
    print(f"{json_file}")
    print(f"{csv_file}")
    if excel_file:
        print(f"{excel_file}")
    print(f"{cleaned_csv}")

    print("\nRésumé final :")
    print(f"{len(cleaned_df)} profils traités")
    print(f"{len(cleaned_df.columns)} colonnes")

    return cleaned_df

# ============================================================================
# EXÉCUTION
# ============================================================================


if __name__ == "__main__":
    try:
        result = main()
        print("\nProgramme terminé avec succès !")
    except KeyboardInterrupt:
        print("\nProgramme interrompu par l'utilisateur")
    except Exception as e:
        print(f"\nErreur inattendue : {e}")
        import traceback
        traceback.print_exc()
