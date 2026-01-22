"""
ZIM PDF Parser Microservice
FastAPI-basierter Service zum Parsen von ZIM-Foerderantraegen (XFA-PDFs)

VERSION: 3.0 - 22. Januar 2026
FEATURES:
- Unterstuetzt Standard-ZIM-Antraege (Einzelprojekt, Kooperation)
- Unterstuetzt Durchfuehrbarkeitsstudien (Antrag_DS)
- UTF-8 Encoding korrekt
- AP-Nummern wie "1.1", "1.2" werden unterstuetzt

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
    version="3.0.0"
)

# CORS fuer Zugriff von deiner Next.js App
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
    """Parst AP-Nummern wie "1", "1.1", "2", "2.1" etc."""
    if not lfd:
        return (0, 0)
    lfd = lfd.strip().rstrip('.')
    if '.' in lfd:
        parts = lfd.split('.')
        try:
            return (int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)
        except:
            return (0, 0)
    try:
        return (int(lfd), 0)
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


# ============================================
# PARSER: STANDARD ZIM
# ============================================

def parse_standard_zim(xfa_text: str, filename: str) -> dict:
    """Parser fuer Standard-ZIM-Antraege"""
    
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
    
    # Laufzeit berechnen
    if projekt['start'] and projekt['ende']:
        try:
            if '-' in projekt['start']:
                start_parts = projekt['start'].split('-')
                end_parts = projekt['ende'].split('-')
                start_year, start_month = int(start_parts[0]), int(start_parts[1])
                end_year, end_month = int(end_parts[0]), int(end_parts[1])
            else:
                start_parts = projekt['start'].split('.')
                end_parts = projekt['ende'].split('.')
                start_year, start_month = int(start_parts[2]), int(start_parts[1])
                end_year, end_month = int(end_parts[2]), int(end_parts[1])
            projekt['laufzeit_monate'] = (end_year - start_year) * 12 + (end_month - start_month) + 1
        except:
            pass
    
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
    
    # Mitarbeiter (TODO: vollstaendige Implementierung)
    mitarbeiter = []
    
    # Arbeitspakete (TODO: vollstaendige Implementierung)
    arbeitspakete = []
    
    return {
        'projekt': projekt,
        'antragsteller': antragsteller,
        'mitarbeiter': mitarbeiter,
        'arbeitspakete': arbeitspakete,
        'format': 'standard_zim'
    }


# ============================================
# PARSER: DURCHFUEHRBARKEITSSTUDIE
# ============================================

def parse_durchfuehrbarkeitsstudie(xfa_text: str, filename: str) -> dict:
    """Parser fuer Durchfuehrbarkeitsstudien (Antrag_DS)"""
    
    # Zeilenumbrueche normalisieren
    text = xfa_text.replace('\n>', '>').replace('>\n', '>')
    
    # Projekt
    projekt = {
        'name': extract_value(r'<thema>([^<]+)', text),
        'kurzname': '',
        'fkz': '',
        'start': '',
        'ende': '',
        'foerderquote': 50.0,  # DS hat feste 50%
        'gesamtkosten': 0.0,
        'zuwendung': 0.0,
        'gesamt_pm': 0.0,
        'gesamt_pk': extract_float(r'<sum_ges_pk>([^<]+)', text) or 
                     extract_float(r'<ges_pk>([^<]+)', text),
        'laufzeit_monate': 0
    }
    
    # Kurzfassung als Kurzname (erste 100 Zeichen)
    kurzfass = extract_value(r'<kurzfass>([^<]+)', text)
    if kurzfass:
        projekt['kurzname'] = kurzfass[:100] + '...' if len(kurzfass) > 100 else kurzfass
    
    # Antragsteller
    antragsteller = {
        'firma': '',
        'rechtsform': extract_value(r'<Rechtsform>([^<]+)', text),
        'strasse': extract_value(r'<str>([^<]+)', text),
        'plz': extract_value(r'<plz>([^<]+)', text),
        'ort': extract_value(r'<ort>([^<]+)', text) or extract_value(r'<pfach_ort>([^<]+)', text),
        'bundesland': extract_value(r'<ddl_land>([^<]+)', text),
        'website': extract_value(r'<www>([^<]+)', text),
        'ansprechpartner_name': '',
        'ansprechpartner_funktion': '',
        'ansprechpartner_telefon': extract_value(r'<tel_ap>([^<]+)', text) or 
                                   extract_value(r'<tel_gf>([^<]+)', text),
        'ansprechpartner_email': extract_value(r'<mail_ap>([^<]+)', text) or 
                                 extract_value(r'<mail_gf>([^<]+)', text),
    }
    
    # Firma aus Website oder Email ableiten
    if antragsteller['website']:
        domain = antragsteller['website'].replace('www.', '').split('.')[0]
        antragsteller['firma'] = domain.capitalize() + ' GmbH'
    elif antragsteller['ansprechpartner_email']:
        parts = antragsteller['ansprechpartner_email'].split('@')
        if len(parts) > 1:
            domain = parts[1].split('.')[0]
            antragsteller['firma'] = domain.capitalize() + ' GmbH'
    
    # Arbeitspakete - Nicht-technische APs
    arbeitspakete = []
    
    ap_nrs = extract_all_values('Arbeitspaket_Nr', text)
    ap_names = extract_all_values('Arbeitspaket', text)
    ap_pms = extract_all_values('pm', text)
    
    print(f"  Nicht-techn. APs: {len(ap_nrs)} Nr, {len(ap_names)} Namen, {len(ap_pms)} PM")
    
    for i in range(max(len(ap_nrs), len(ap_names))):
        ap_nr_str = ap_nrs[i] if i < len(ap_nrs) else str(i + 1)
        ap_name = ap_names[i] if i < len(ap_names) else ''
        pm_str = ap_pms[i] if i < len(ap_pms) else '0'
        
        if ap_name and len(ap_name) > 2:
            haupt, unter = parse_ap_nummer(ap_nr_str)
            if haupt == 0:
                haupt = i + 1
            
            pm = parse_float_value(pm_str)
            
            arbeitspakete.append({
                'ap_nummer': haupt,
                'ap_code': f'AP{ap_nr_str}',
                'name': ap_name,
                'start_monat': None,
                'ende_monat': None,
                'gesamt_pm': pm,
                'mitarbeiter_zuordnungen': []
            })
            
            projekt['gesamt_pm'] += pm
    
    # Arbeitspakete - Technische APs
    ap_nrs_tech = extract_all_values('Arbeitspaket_Nr_techn', text)
    ap_names_tech = extract_all_values('Arbeitspaket_techn', text)
    ap_pms_tech = extract_all_values('pm_techn', text)
    
    print(f"  Technische APs: {len(ap_nrs_tech)} Nr, {len(ap_names_tech)} Namen, {len(ap_pms_tech)} PM")
    
    for i in range(max(len(ap_nrs_tech), len(ap_names_tech))):
        ap_nr_str = ap_nrs_tech[i] if i < len(ap_nrs_tech) else ''
        ap_name = ap_names_tech[i] if i < len(ap_names_tech) else ''
        pm_str = ap_pms_tech[i] if i < len(ap_pms_tech) else '0'
        
        if ap_name and len(ap_name) > 2 and ap_nr_str:
            clean_nr = ap_nr_str.rstrip('.')
            haupt, unter = parse_ap_nummer(clean_nr)
            
            # Pruefe ob AP schon existiert
            exists = any(
                ap['ap_nummer'] == haupt and ap['ap_code'] == f'AP{clean_nr}'
                for ap in arbeitspakete
            )
            
            if not exists and haupt > 0:
                pm = parse_float_value(pm_str)
                
                arbeitspakete.append({
                    'ap_nummer': haupt,
                    'ap_code': f'AP{clean_nr}',
                    'name': ap_name,
                    'start_monat': None,
                    'ende_monat': None,
                    'gesamt_pm': pm,
                    'mitarbeiter_zuordnungen': []
                })
                
                projekt['gesamt_pm'] += pm
    
    # Sortieren
    arbeitspakete.sort(key=lambda ap: (ap['ap_nummer'], ap['ap_code']))
    
    # DS hat normalerweise keine detaillierten Mitarbeiter-Daten
    mitarbeiter = []
    
    return {
        'projekt': projekt,
        'antragsteller': antragsteller,
        'mitarbeiter': mitarbeiter,
        'arbeitspakete': arbeitspakete,
        'format': 'durchfuehrbarkeitsstudie'
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
            # Versuche beide Parser
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
        # Temporaere Datei loeschen
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
        "version": "3.0.0",
        "status": "online",
        "endpoints": {
            "/parse": "POST - PDF hochladen und parsen",
            "/health": "GET - Health Check"
        },
        "supported_formats": [
            "Standard ZIM (Einzelprojekt, Kooperation)",
            "Durchfuehrbarkeitsstudie (Antrag_DS)"
        ]
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "version": "3.0.0"}


@app.post("/parse")
async def parse_pdf(file: UploadFile = File(...)):
    """
    Parst ein ZIM-PDF und gibt strukturierte Daten zurueck.
    
    Unterstuetzte Formate:
    - Standard ZIM (Einzelprojekt, Kooperationsprojekt)
    - Durchfuehrbarkeitsstudie (Antrag_DS)
    """
    
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Datei muss eine PDF sein")
    
    try:
        content = await file.read()
        print(f"\n=== ZIM Parser v3.0 ===")
        print(f"Datei: {file.filename}, Groesse: {len(content)} bytes")
        
        result = parse_zim_pdf(content, file.filename)
        
        print(f"Erfolgreich! Projekt: {result['projekt']['name'][:50]}..." if result['projekt']['name'] else "Projekt: (kein Name)")
        print(f"APs: {result['statistik']['anzahl_arbeitspakete']}, PM: {result['statistik']['gesamt_pm']}")
        
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
