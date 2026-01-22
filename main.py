"""
ZIM PDF Parser Microservice
FastAPI-basierter Service zum Parsen von ZIM-Foerderantraegen (XFA-PDFs)

VERSION: 3.1 - 22. Januar 2026
FEATURES:
- Unterstuetzt Standard-ZIM-Antraege (Einzelprojekt, Kooperation)
- Unterstuetzt Durchfuehrbarkeitsstudien (Antrag_DS)
- Extrahiert Mitarbeiter aus Anlage 6.1
- Extrahiert MA-Zuordnungen zu Arbeitspaketen
- UTF-8 Encoding korrekt

Deployment: Railway.app
"""

import os
import re
import json
import tempfile
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

try:
    from pypdf import PdfReader
except ImportError:
    raise ImportError("pypdf nicht installiert! pip install pypdf")

app = FastAPI(
    title="ZIM PDF Parser",
    description="Parst ZIM-Foerderantraege (XFA-PDFs) und extrahiert strukturierte Daten",
    version="3.1.0"
)

# CORS fuer Zugriff von Next.js App
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://pze.vercel.app",
        "https://*.vercel.app",
        "https://projektzeiterfassung20-git-v7-dev-martin-ds-projects-5cb70f89.vercel.app",
        os.getenv("ALLOWED_ORIGIN", "*")
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# HELPER FUNCTIONS
# ============================================

def extract_value(pattern: str, text: str) -> str:
    """Extrahiert einen Wert mit Regex"""
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else ''


def parse_float_value(value: str) -> float:
    """Parst einen String zu Float (mit deutschem Zahlenformat)"""
    if not value:
        return 0.0
    cleaned = value.strip()
    if ',' in cleaned and '.' in cleaned:
        if cleaned.rfind(',') > cleaned.rfind('.'):
            cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
            cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        cleaned = cleaned.replace(',', '.')
    try:
        return float(cleaned)
    except:
        return 0.0


def extract_float(pattern: str, text: str) -> float:
    """Extrahiert eine Zahl mit Regex"""
    value = extract_value(pattern, text)
    return parse_float_value(value)


def extract_all_values(tag_name: str, text: str) -> list:
    """Extrahiert alle Werte eines Tags"""
    pattern = f'<{tag_name}>([^<]*)</{tag_name}>'
    matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
    return [m.strip() for m in matches if m.strip()]


def parse_ap_nummer(lfd: str) -> tuple:
    """Parst AP-Nummern wie '1', '1.1', '2', '2.1' etc."""
    if not lfd:
        return (0, 0)
    lfd = str(lfd).strip().rstrip('.')
    if '.' in lfd:
        parts = lfd.split('.')
        try:
            return (int(parts[0]), int(parts[1]) if len(parts) > 1 and parts[1] else 0)
        except:
            return (0, 0)
    try:
        return (int(float(lfd)), 0)
    except:
        return (0, 0)


def detect_format(xfa_text: str) -> str:
    """Erkennt das PDF-Format"""
    if 'Antrag_DS' in xfa_text or '<thema>' in xfa_text:
        return 'durchfuehrbarkeitsstudie'
    elif 'cg_VMS_' in xfa_text or 'cg_case_' in xfa_text:
        return 'standard_zim'
    else:
        return 'unbekannt'


def parse_german_date(date_str: str) -> str:
    """Konvertiert deutsches Datum (DD.MM.YYYY) zu ISO (YYYY-MM-DD)"""
    if not date_str:
        return ''
    date_str = date_str.strip()
    # Bereits ISO-Format?
    if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
        return date_str[:10]
    # Deutsches Format?
    match = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
    if match:
        return f"{match.group(3)}-{match.group(2).zfill(2)}-{match.group(1).zfill(2)}"
    return date_str


# ============================================
# MITARBEITER EXTRAKTION (Anlage 6.1)
# ============================================

def extract_mitarbeiter_ds(xfa_text: str) -> list:
    """Extrahiert Mitarbeiter aus Durchfuehrbarkeitsstudie (Page11 Bloecke)"""
    mitarbeiter = []
    
    # Finde alle Page11 Bloecke (jeder Block = ein Mitarbeiter)
    page11_blocks = re.findall(r'<Page11>(.*?)</Page11>', xfa_text, re.DOTALL)
    
    print(f"  Gefundene Page11-Bloecke: {len(page11_blocks)}")
    
    for idx, block in enumerate(page11_blocks):
        # Nur Bloecke mit lfd (Mitarbeiternummer) verarbeiten
        if '<lfd>' not in block:
            continue
            
        lfd = extract_value(r'<lfd>([^<]+)</lfd>', block)
        name = extract_value(r'<name>([^<]+)</name>', block)
        vname = extract_value(r'<vname>([^<]+)</vname>', block)
        
        # Leere Eintraege ueberspringen
        if not name or not vname:
            continue
        
        # Geburtsdatum (verschiedene Feldnamen moeglich)
        geb = extract_value(r'<geb_gf>([^<]+)</geb_gf>', block)
        if not geb:
            geb = extract_value(r'<geb>([^<]+)</geb>', block)
        
        ma = {
            'ma_nr': int(float(lfd)) if lfd else idx + 1,
            'nachname': name,
            'vorname': vname,
            'name_komplett': f"{vname} {name}",
            'geburtsdatum': parse_german_date(geb),
            'qualifikation': extract_value(r'<quali>([^<]+)</quali>', block),
            'funktion': extract_value(r'<als>([^<]+)</als>', block) or extract_value(r'<statisch><als>([^<]+)</als>', block),
            'angestellt_seit': parse_german_date(extract_value(r'<ang_seit>([^<]+)</ang_seit>', block)),
            'wochenstunden': parse_float_value(extract_value(r'<wo_std>([^<]+)</wo_std>', block)),
            'jahresbrutto': parse_float_value(extract_value(r'<jahresbrutto>([^<]+)</jahresbrutto>', block)),
            'monatsbrutto': parse_float_value(extract_value(r'<monats_brutto>([^<]+)</monats_brutto>', block)),
            'stundensatz': parse_float_value(extract_value(r'<std_satz>([^<]+)</std_satz>', block)),
            'teilzeitfaktor': parse_float_value(extract_value(r'<tz_faktor>([^<]+)</tz_faktor>', block)) or 1.0,
        }
        
        # Berechne fehlende Werte
        if ma['jahresbrutto'] > 0 and ma['stundensatz'] == 0 and ma['wochenstunden'] > 0:
            jahresstunden = ma['wochenstunden'] * 52
            ma['stundensatz'] = round(ma['jahresbrutto'] / jahresstunden, 2)
        
        if ma['monatsbrutto'] == 0 and ma['jahresbrutto'] > 0:
            ma['monatsbrutto'] = round(ma['jahresbrutto'] / 12, 2)
        
        mitarbeiter.append(ma)
        print(f"    MA {ma['ma_nr']}: {ma['name_komplett']} - {ma['qualifikation']} - {ma['stundensatz']} EUR/h")
    
    return mitarbeiter


# ============================================
# MA-ZUORDNUNGEN EXTRAKTION (Anlage 5 / Page10)
# ============================================

def extract_ma_zuordnungen_ds(xfa_text: str) -> dict:
    """
    Extrahiert MA-Zuordnungen zu Arbeitspaketen aus Page10.
    Gibt ein Dict zurueck: {ap_code: [(ma_nr, pm), ...]}
    """
    zuordnungen = {}
    
    # Nicht-technische APs: <MA_10B>, <AP_10B>, <pm_10B>
    # Format: <MA_10B>1</MA_10B><pm_10B>1.5</pm_10B><AP_10B>1;3;</AP_10B>
    ma_blocks = re.findall(
        r'<MA_10B>([^<]*)</MA_10B>.*?<pm_10B>([^<]*)</pm_10B>.*?<AP_10B>([^<]*)</AP_10B>',
        xfa_text, re.DOTALL
    )
    
    print(f"  Nicht-technische MA-Zuordnungen: {len(ma_blocks)}")
    
    for ma_nr, pm_gesamt, aps in ma_blocks:
        ma_nr = int(float(ma_nr)) if ma_nr else 0
        if ma_nr == 0:
            continue
            
        # APs sind semicolon-separiert: "1;3;"
        ap_list = [ap.strip() for ap in aps.split(';') if ap.strip()]
        
        # PM gleichmaessig auf APs verteilen (vereinfacht)
        pm_total = parse_float_value(pm_gesamt)
        pm_per_ap = pm_total / len(ap_list) if ap_list else 0
        
        for ap in ap_list:
            ap_code = f"AP{ap}"
            if ap_code not in zuordnungen:
                zuordnungen[ap_code] = []
            zuordnungen[ap_code].append({
                'ma_nr': ma_nr,
                'pm': round(pm_per_ap, 2)
            })
    
    # Technische APs: <MA_10B_techn>, <AP_10B_techn>, <pm_10B_techn>
    ma_blocks_tech = re.findall(
        r'<MA_10B_techn>([^<]*)</MA_10B_techn>.*?<pm_10B_techn>([^<]*)</pm_10B_techn>.*?<AP_10B_techn>([^<]*)</AP_10B_techn>',
        xfa_text, re.DOTALL
    )
    
    print(f"  Technische MA-Zuordnungen: {len(ma_blocks_tech)}")
    
    for ma_nr, pm_gesamt, aps in ma_blocks_tech:
        ma_nr = int(float(ma_nr)) if ma_nr else 0
        if ma_nr == 0:
            continue
            
        ap_list = [ap.strip().rstrip('.') for ap in aps.split(';') if ap.strip()]
        pm_total = parse_float_value(pm_gesamt)
        pm_per_ap = pm_total / len(ap_list) if ap_list else 0
        
        for ap in ap_list:
            ap_code = f"AP{ap}"
            if ap_code not in zuordnungen:
                zuordnungen[ap_code] = []
            zuordnungen[ap_code].append({
                'ma_nr': ma_nr,
                'pm': round(pm_per_ap, 2)
            })
    
    # Alternative: Direkte Zuordnungen aus Zeile2 (Anlage 5)
    # <Arbeitspaket_Nr>1</Arbeitspaket_Nr>...<MA_Nr>1.00</MA_Nr><pm>0.500</pm>
    zeile_matches = re.findall(
        r'<Arbeitspaket_Nr>([^<]*)</Arbeitspaket_Nr>.*?<MA_Nr>([^<]*)</MA_Nr>.*?<pm>([^<]*)</pm>',
        xfa_text, re.DOTALL
    )
    
    if zeile_matches and not zuordnungen:
        print(f"  Direkte AP-Zuordnungen aus Anlage 5: {len(zeile_matches)}")
        for ap_nr, ma_nr, pm in zeile_matches:
            ap_code = f"AP{ap_nr.strip()}"
            ma = int(float(ma_nr)) if ma_nr else 0
            if ma == 0:
                continue
            if ap_code not in zuordnungen:
                zuordnungen[ap_code] = []
            zuordnungen[ap_code].append({
                'ma_nr': ma,
                'pm': parse_float_value(pm)
            })
    
    return zuordnungen


# ============================================
# PARSER: DURCHFUEHRBARKEITSSTUDIE
# ============================================

def parse_durchfuehrbarkeitsstudie(xfa_text: str, filename: str) -> dict:
    """Parser fuer Durchfuehrbarkeitsstudien (Antrag_DS)"""
    
    print("  Parsing Durchfuehrbarkeitsstudie...")
    
    # Zeilenumbrueche normalisieren
    text = xfa_text.replace('\n>', '>').replace('>\n', '>')
    
    # Projekt-Stammdaten
    projekt = {
        'name': extract_value(r'<thema>([^<]+)', text),
        'kurzname': '',
        'fkz': '',
        'start': extract_value(r'<Laufzeit[^>]*>.*?<von>([^<]+)</von>', text) or 
                 extract_value(r'Laufzeit.*?von.*?(\d{2}\.\d{2}\.\d{4})', text),
        'ende': extract_value(r'<Laufzeit[^>]*>.*?<bis>([^<]+)</bis>', text) or
                extract_value(r'Laufzeit.*?bis.*?(\d{2}\.\d{2}\.\d{4})', text),
        'foerderquote': extract_float(r'<foerdersatz[^>]*>([^<]+)', text) or 50.0,
        'gesamtkosten': extract_float(r'<sum_ges_pk>([^<]+)', text),
        'zuwendung': extract_float(r'<Zuwendung[^>]*>([^<]+)', text),
        'gesamt_pm': 0.0,
        'gesamt_pk': extract_float(r'<sum_ges_pk>([^<]+)', text),
        'laufzeit_monate': 0
    }
    
    # Kurzfassung als Kurzname
    kurzfass = extract_value(r'<kurzfass>([^<]+)', text)
    if kurzfass:
        projekt['kurzname'] = kurzfass[:100] + '...' if len(kurzfass) > 100 else kurzfass
    
    # Laufzeit berechnen
    if projekt['start'] and projekt['ende']:
        start_date = parse_german_date(projekt['start'])
        end_date = parse_german_date(projekt['ende'])
        projekt['start'] = start_date
        projekt['ende'] = end_date
        try:
            if start_date and end_date:
                s_parts = start_date.split('-')
                e_parts = end_date.split('-')
                s_year, s_month = int(s_parts[0]), int(s_parts[1])
                e_year, e_month = int(e_parts[0]), int(e_parts[1])
                projekt['laufzeit_monate'] = (e_year - s_year) * 12 + (e_month - s_month) + 1
        except:
            pass
    
    # Antragsteller
    antragsteller = {
        'firma': extract_value(r'<Antragsteller>([^<]+)', text) or '',
        'rechtsform': extract_value(r'<Rechtsform>([^<]+)', text),
        'strasse': extract_value(r'<str>([^<]+)', text),
        'plz': extract_value(r'<plz>([^<]+)', text),
        'ort': extract_value(r'<ort>([^<]+)', text),
        'bundesland': extract_value(r'<ddl_land>([^<]+)', text),
        'website': extract_value(r'<www>([^<]+)', text),
        'ansprechpartner_name': f"{extract_value(r'<vname_ap>([^<]+)', text)} {extract_value(r'<name_ap>([^<]+)', text)}".strip(),
        'ansprechpartner_funktion': '',
        'ansprechpartner_telefon': extract_value(r'<tel_ap>([^<]+)', text),
        'ansprechpartner_email': extract_value(r'<mail_ap>([^<]+)', text),
    }
    
    # Firma aus Website ableiten wenn leer
    if not antragsteller['firma'] and antragsteller['website']:
        domain = antragsteller['website'].replace('www.', '').split('.')[0]
        antragsteller['firma'] = domain.capitalize() + ' GmbH'
    
    # ========== MITARBEITER ==========
    print("  Extrahiere Mitarbeiter...")
    mitarbeiter = extract_mitarbeiter_ds(text)
    print(f"  {len(mitarbeiter)} Mitarbeiter gefunden")
    
    # ========== MA-ZUORDNUNGEN ==========
    print("  Extrahiere MA-Zuordnungen...")
    ma_zuordnungen = extract_ma_zuordnungen_ds(text)
    print(f"  Zuordnungen fuer {len(ma_zuordnungen)} APs gefunden")
    
    # ========== ARBEITSPAKETE ==========
    arbeitspakete = []
    
    # Nicht-technische APs
    ap_nrs = extract_all_values('Arbeitspaket_Nr', text)
    ap_names = extract_all_values('Arbeitspaket', text)
    ap_pms = extract_all_values('pm', text)
    ap_von = extract_all_values('RealisierungVON', text)
    ap_bis = extract_all_values('RealisierungBIS', text)
    
    print(f"  Nicht-techn. APs: {len(ap_nrs)} Nr, {len(ap_names)} Namen")
    
    for i in range(max(len(ap_nrs), len(ap_names))):
        ap_nr_str = ap_nrs[i] if i < len(ap_nrs) else str(i + 1)
        ap_name = ap_names[i] if i < len(ap_names) else ''
        pm_str = ap_pms[i] if i < len(ap_pms) else '0'
        von = ap_von[i] if i < len(ap_von) else ''
        bis = ap_bis[i] if i < len(ap_bis) else ''
        
        if ap_name and len(ap_name) > 2:
            haupt, unter = parse_ap_nummer(ap_nr_str)
            if haupt == 0:
                haupt = i + 1
            
            pm = parse_float_value(pm_str)
            ap_code = f"AP{ap_nr_str.strip()}"
            
            # MA-Zuordnungen fuer dieses AP
            ap_zuordnungen = ma_zuordnungen.get(ap_code, [])
            
            arbeitspakete.append({
                'ap_nummer': haupt,
                'ap_sub_nummer': unter,
                'ap_code': ap_code,
                'name': ap_name,
                'start_datum': parse_german_date(von),
                'ende_datum': parse_german_date(bis),
                'start_monat': None,
                'ende_monat': None,
                'gesamt_pm': pm,
                'is_technical': False,
                'mitarbeiter_zuordnungen': ap_zuordnungen
            })
            
            projekt['gesamt_pm'] += pm
    
    # Technische APs
    ap_nrs_tech = extract_all_values('Arbeitspaket_Nr_techn', text)
    ap_names_tech = extract_all_values('Arbeitspaket_techn', text)
    ap_pms_tech = extract_all_values('pm_techn', text)
    
    print(f"  Technische APs: {len(ap_nrs_tech)} Nr, {len(ap_names_tech)} Namen")
    
    for i in range(max(len(ap_nrs_tech), len(ap_names_tech))):
        ap_nr_str = ap_nrs_tech[i] if i < len(ap_nrs_tech) else ''
        ap_name = ap_names_tech[i] if i < len(ap_names_tech) else ''
        pm_str = ap_pms_tech[i] if i < len(ap_pms_tech) else '0'
        
        if ap_name and len(ap_name) > 2 and ap_nr_str:
            clean_nr = ap_nr_str.rstrip('.')
            haupt, unter = parse_ap_nummer(clean_nr)
            
            # Pruefe ob AP schon existiert
            ap_code = f"AP{clean_nr}"
            exists = any(ap['ap_code'] == ap_code for ap in arbeitspakete)
            
            if not exists and haupt > 0:
                pm = parse_float_value(pm_str)
                ap_zuordnungen = ma_zuordnungen.get(ap_code, [])
                
                arbeitspakete.append({
                    'ap_nummer': haupt,
                    'ap_sub_nummer': unter,
                    'ap_code': ap_code,
                    'name': ap_name,
                    'start_datum': None,
                    'ende_datum': None,
                    'start_monat': None,
                    'ende_monat': None,
                    'gesamt_pm': pm,
                    'is_technical': True,
                    'mitarbeiter_zuordnungen': ap_zuordnungen
                })
                
                projekt['gesamt_pm'] += pm
    
    # Sortieren nach AP-Nummer
    arbeitspakete.sort(key=lambda ap: (ap['ap_nummer'], ap['ap_sub_nummer'] or 0))
    
    return {
        'projekt': projekt,
        'antragsteller': antragsteller,
        'mitarbeiter': mitarbeiter,
        'arbeitspakete': arbeitspakete,
        'format': 'durchfuehrbarkeitsstudie'
    }


# ============================================
# PARSER: STANDARD ZIM
# ============================================

def parse_standard_zim(xfa_text: str, filename: str) -> dict:
    """Parser fuer Standard-ZIM-Antraege (Einzelprojekt, Kooperation)"""
    
    print("  Parsing Standard-ZIM...")
    
    # Projekt
    projekt = {
        'name': extract_value(r'<cg_VMS_VB_Projekt>([^<]+)', xfa_text),
        'kurzname': extract_value(r'<cg_VMS_VB_KurzName>([^<]+)', xfa_text),
        'fkz': extract_value(r'<cg_case_KENN_2>([^<]+)', xfa_text),
        'start': extract_value(r'<cg_VMS_VB_Beginn>([^<]+)', xfa_text),
        'ende': extract_value(r'<cg_VMS_VB_Ende>([^<]+)', xfa_text),
        'foerderquote': extract_float(r'<cg_VMS_AD_F[oö]rderquote>([^<]+)', xfa_text),
        'gesamtkosten': extract_float(r'<cg_VMS_HB_A_Kosten>([^<]+)', xfa_text),
        'zuwendung': extract_float(r'<cg_VMS_HB_A_ZuwendungFQ>([^<]+)', xfa_text),
        'gesamt_pm': extract_float(r'<sum_ges_pm>([^<]+)', xfa_text),
        'gesamt_pk': extract_float(r'<sum_ges_pk>([^<]+)', xfa_text),
        'laufzeit_monate': 0
    }
    
    # Antragsteller
    antragsteller = {
        'firma': extract_value(r'<cg_VMS_firma>([^<]+)', xfa_text),
        'rechtsform': extract_value(r'<cg_VMS_rechtsform>([^<]+)', xfa_text),
        'strasse': extract_value(r'<cg_VMS_str>([^<]+)', xfa_text),
        'plz': extract_value(r'<cg_VMS_plz>([^<]+)', xfa_text),
        'ort': extract_value(r'<cg_VMS_ort>([^<]+)', xfa_text),
        'bundesland': extract_value(r'<cg_VMS_bundesland>([^<]+)', xfa_text),
        'website': extract_value(r'<cg_VMS_www>([^<]+)', xfa_text),
        'ansprechpartner_name': extract_value(r'<cg_VMS_AP_name>([^<]+)', xfa_text),
        'ansprechpartner_funktion': extract_value(r'<cg_VMS_AP_funktion>([^<]+)', xfa_text),
        'ansprechpartner_telefon': extract_value(r'<cg_VMS_AP_tel>([^<]+)', xfa_text),
        'ansprechpartner_email': extract_value(r'<cg_VMS_AP_mail>([^<]+)', xfa_text),
    }
    
    # TODO: Mitarbeiter und APs fuer Standard-ZIM implementieren
    mitarbeiter = []
    arbeitspakete = []
    
    return {
        'projekt': projekt,
        'antragsteller': antragsteller,
        'mitarbeiter': mitarbeiter,
        'arbeitspakete': arbeitspakete,
        'format': 'standard_zim'
    }


# ============================================
# MAIN PARSER
# ============================================

def parse_zim_pdf(pdf_content: bytes, filename: str) -> dict:
    """Extrahiert alle Daten aus einem ZIM-PDF"""
    
    # PDF in temporaere Datei schreiben
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        tmp.write(pdf_content)
        tmp_path = tmp.name
    
    try:
        print(f"Lade PDF: {filename}")
        reader = PdfReader(tmp_path)
        
        # XFA-Daten extrahieren
        root = reader.trailer['/Root'].get_object()
        
        if '/AcroForm' not in root:
            raise ValueError("Keine Formulardaten gefunden (kein AcroForm)")
        
        acro = root['/AcroForm'].get_object()
        
        if '/XFA' not in acro:
            raise ValueError("Keine XFA-Daten gefunden")
        
        xfa = acro['/XFA']
        
        # XFA ist ein Array mit Namen und Streams
        xfa_text = ""
        for i, item in enumerate(xfa):
            if hasattr(item, 'get_object'):
                try:
                    obj = item.get_object()
                    if hasattr(obj, 'get_data'):
                        data = obj.get_data().decode('utf-8', errors='ignore')
                        xfa_text += data
                except:
                    pass
        
        if not xfa_text:
            raise ValueError("Konnte XFA-Daten nicht extrahieren")
        
        print(f"XFA-Daten extrahiert: {len(xfa_text)} Zeichen")
        
        # Format erkennen
        pdf_format = detect_format(xfa_text)
        print(f"Format erkannt: {pdf_format}")
        
        # Entsprechenden Parser aufrufen
        if pdf_format == 'durchfuehrbarkeitsstudie':
            result = parse_durchfuehrbarkeitsstudie(xfa_text, filename)
        elif pdf_format == 'standard_zim':
            result = parse_standard_zim(xfa_text, filename)
        else:
            print("Unbekanntes Format - versuche DS-Parser...")
            result = parse_durchfuehrbarkeitsstudie(xfa_text, filename)
            if not result['projekt']['name'] and not result['arbeitspakete']:
                print("DS-Parser fehlgeschlagen - versuche Standard-Parser...")
                result = parse_standard_zim(xfa_text, filename)
        
        # Budget berechnen
        budget = {
            'gesamtkosten': result['projekt']['gesamtkosten'],
            'personalkosten': result['projekt']['gesamt_pk'],
            'materialkosten': 0.0,
            'fremdleistungen': 0.0,
            'gemeinkosten': 0.0,
            'foerderquote': result['projekt']['foerderquote'],
            'foerdersumme': result['projekt']['zuwendung'],
            'eigenanteil': 0.0,
            'laufzeit_monate': result['projekt']['laufzeit_monate'],
            'gesamt_pm': result['projekt']['gesamt_pm']
        }
        
        # Statistik
        statistik = {
            'anzahl_mitarbeiter': len(result['mitarbeiter']),
            'anzahl_arbeitspakete': len(result['arbeitspakete']),
            'anzahl_ap_zuordnungen': sum(
                len(ap.get('mitarbeiter_zuordnungen', [])) 
                for ap in result['arbeitspakete']
            ),
            'gesamt_pm': result['projekt']['gesamt_pm'],
            'gesamt_pk': result['projekt']['gesamt_pk'],
            'laufzeit_monate': result['projekt']['laufzeit_monate'],
        }
        
        return {
            'projekt': result['projekt'],
            'antragsteller': result['antragsteller'],
            'budget': budget,
            'mitarbeiter': result['mitarbeiter'],
            'arbeitspakete': result['arbeitspakete'],
            'parse_datum': datetime.now().isoformat(),
            'quell_datei': filename,
            'format_erkannt': result['format'],
            'statistik': statistik
        }
        
    finally:
        import os
        try:
            os.unlink(tmp_path)
        except:
            pass


# ============================================
# API ENDPOINTS
# ============================================

@app.get("/")
async def root():
    return {
        "service": "ZIM PDF Parser",
        "version": "3.1.0",
        "status": "online",
        "endpoints": {
            "/parse": "POST - PDF hochladen und parsen",
            "/health": "GET - Health Check"
        },
        "features": [
            "Standard ZIM (Einzelprojekt, Kooperation)",
            "Durchfuehrbarkeitsstudie (Antrag_DS)",
            "Mitarbeiter-Extraktion (Anlage 6.1)",
            "MA-Zuordnungen zu Arbeitspaketen"
        ]
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "version": "3.1.0"}


@app.post("/parse")
async def parse_pdf(file: UploadFile = File(...)):
    """
    Parst ein ZIM-PDF und gibt strukturierte Daten zurueck.
    
    Unterstuetzte Formate:
    - Standard ZIM (Einzelprojekt, Kooperationsprojekt)
    - Durchfuehrbarkeitsstudie (Antrag_DS)
    
    Extrahierte Daten:
    - Projekt-Stammdaten
    - Antragsteller
    - Mitarbeiter (aus Anlage 6.1)
    - Arbeitspakete mit MA-Zuordnungen
    """
    
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Datei muss eine PDF sein")
    
    try:
        content = await file.read()
        print(f"\n{'='*50}")
        print(f"ZIM Parser v3.1")
        print(f"{'='*50}")
        print(f"Datei: {file.filename}")
        print(f"Groesse: {len(content)} bytes")
        
        result = parse_zim_pdf(content, file.filename)
        
        print(f"\n{'='*50}")
        print(f"ERGEBNIS:")
        print(f"  Projekt: {result['projekt']['name'][:50]}..." if result['projekt']['name'] else "  Projekt: (kein Name)")
        print(f"  Mitarbeiter: {result['statistik']['anzahl_mitarbeiter']}")
        print(f"  Arbeitspakete: {result['statistik']['anzahl_arbeitspakete']}")
        print(f"  MA-Zuordnungen: {result['statistik']['anzahl_ap_zuordnungen']}")
        print(f"  Gesamt-PM: {result['statistik']['gesamt_pm']}")
        print(f"{'='*50}\n")
        
        return JSONResponse(content={
            "success": True,
            "data": result
        })
        
    except ValueError as e:
        print(f"ValueError: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Parsing fehlgeschlagen: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
