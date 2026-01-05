"""
ZIM PDF Parser Microservice
FastAPI-basierter Service zum Parsen von ZIM-Förderanträgen (XFA-PDFs)

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
    description="Parst ZIM-Förderanträge (XFA-PDFs) und extrahiert strukturierte Daten",
    version="1.0.0"
)

# CORS für Zugriff von deiner Next.js App
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://pze.vercel.app",
        "https://*.vercel.app",
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
    match = re.search(pattern, text)
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


def parse_ap_nummer(lfd: str) -> tuple:
    """Parst AP-Nummern wie "1", "1.1", "2", "2.1" etc."""
    if not lfd:
        return (0, 0)
    lfd = lfd.strip()
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


# ============================================
# MAIN PARSER
# ============================================

def parse_zim_pdf(pdf_content: bytes, filename: str) -> dict:
    """Extrahiert alle Daten aus einem ZIM-PDF"""
    
    # PDF in temporäre Datei schreiben (pypdf braucht Dateipfad)
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        tmp.write(pdf_content)
        tmp_path = tmp.name
    
    try:
        reader = PdfReader(tmp_path)
        
        # XFA-Daten extrahieren
        root = reader.trailer['/Root'].get_object()
        
        if '/AcroForm' not in root:
            raise ValueError("Keine Formulardaten gefunden (kein AcroForm) - ist dies ein XFA-PDF?")
        
        acro = root['/AcroForm'].get_object()
        
        if '/XFA' not in acro:
            raise ValueError("Keine XFA-Daten gefunden - ist dies ein ausgefüllter ZIM-Antrag?")
        
        xfa = acro['/XFA']
        
        # XFA ist ein Array mit Namen und Streams
        xfa_text = ""
        for item in xfa:
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
        
        # XFA normalisieren (Zeilenumbrüche entfernen)
        xfa_norm = xfa_text.replace('\n', '')
        
        # === PROJEKTDATEN ===
        projekt = {
            'name': extract_value(r'<cg_VMS_VB_Projekt>([^<]+)', xfa_norm),
            'kurzname': extract_value(r'<cg_VMS_VB_KurzName>([^<]+)', xfa_norm),
            'fkz': extract_value(r'<cg_case_KENN_2>([^<]+)', xfa_norm),
            'start': extract_value(r'<cg_VMS_VB_Beginn>([^<]+)', xfa_norm),
            'ende': extract_value(r'<cg_VMS_VB_Ende>([^<]+)', xfa_norm),
            'foerderquote': extract_float(r'<cg_VMS_AD_Förderquote>([^<]+)', xfa_norm) or 
                            extract_float(r'<cg_VMS_AD_F.rderquote>([^<]+)', xfa_norm),
            'gesamtkosten': extract_float(r'<cg_VMS_HB_A_Kosten>([^<]+)', xfa_norm),
            'zuwendung': extract_float(r'<cg_VMS_HB_A_ZuwendungFQ>([^<]+)', xfa_norm),
            'gesamt_pm': extract_float(r'<sum_ges_pm>([^<]+)', xfa_norm),
            'gesamt_pk': extract_float(r'<sum_ges_pk>([^<]+)', xfa_norm),
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
        
        # === ANTRAGSTELLER ===
        antragsteller = {
            'firma': extract_value(r'<Seite2_AST>([^<]+)', xfa_norm),
            'rechtsform': extract_value(r'<cg_VMS_AD_Rechtsform>([^<]+)', xfa_norm),
            'strasse': extract_value(r'<Strasse_Ast>([^<]+)', xfa_norm),
            'plz': extract_value(r'<PLZ_Ast>([^<]+)', xfa_norm),
            'ort': extract_value(r'<Ort_Ast>([^<]+)', xfa_norm),
            'bundesland': extract_value(r'<cg_VMS_AD_Bundesland>([^<]+)', xfa_norm) or
                          extract_value(r'<Bundeslan_Ast>([^<]+)', xfa_norm),
            'website': extract_value(r'<website_Ast>([^<]+)', xfa_norm),
            'ansprechpartner_name': f"{extract_value(r'<Seite2_VornameVB>([^<]+)', xfa_norm)} {extract_value(r'<Seite2_NameVB>([^<]+)', xfa_norm)}".strip() or
                                    extract_value(r'<Seite4_NameBefugter>([^<]+)', xfa_norm),
            'ansprechpartner_funktion': extract_value(r'<Seite2_FunktionVB>([^<]+)', xfa_norm),
            'ansprechpartner_telefon': extract_value(r'<Seite2_TelefonVB>([^<]+)', xfa_norm),
            'ansprechpartner_email': extract_value(r'<Seite2_MailVB>([^<]+)', xfa_norm),
        }
        
        # === BUDGET ===
        budget = {
            'gesamtkosten': projekt['gesamtkosten'],
            'personalkosten': projekt['gesamt_pk'] or extract_float(r'<cg_VMS_HB_A_Jahr1Kost01>([^<]+)', xfa_norm),
            'materialkosten': extract_float(r'<cg_VMS_HB_A_Material>([^<]+)', xfa_norm) or
                              extract_float(r'<cg_VMS_HB_A_Jahr1Kost02>([^<]+)', xfa_norm),
            'fremdleistungen': extract_float(r'<cg_VMS_HB_A_Fremdleist>([^<]+)', xfa_norm),
            'gemeinkosten': extract_float(r'<cg_VMS_HB_A_Gemein>([^<]+)', xfa_norm),
            'foerderquote': projekt['foerderquote'],
            'foerdersumme': projekt['zuwendung'],
            'eigenanteil': projekt['gesamtkosten'] - projekt['zuwendung'],
            'laufzeit_monate': projekt['laufzeit_monate'],
            'gesamt_pm': projekt['gesamt_pm'],
        }
        
        # === MITARBEITER ===
        mitarbeiter = []
        
        # Anlage 6.2 Lookup (PM-Summen)
        a62_lookup = {}
        a62_blocks = re.findall(r'<cg_file_262_Zeile1_Anlage62>(.*?)</cg_file_262_Zeile1_Anlage62>', xfa_norm, re.DOTALL)
        for block in a62_blocks:
            ma_id = extract_value(r'<cg_VMS_PK_DdsId_261>([^<]+)', block)
            if ma_id:
                pm_pro_jahr = {}
                for jahr_match in re.finditer(r'<cg_VMS_PK_iJahrZahl>(\d{4})</cg_VMS_PK_iJahrZahl>.*?<cg_VMS_PK_fPersMonat>([^<]+)</cg_VMS_PK_fPersMonat>', block, re.DOTALL):
                    jahr = int(jahr_match.group(1))
                    pm_str = jahr_match.group(2).strip()
                    pm = parse_float_value(pm_str)
                    pm_pro_jahr[jahr] = pm_pro_jahr.get(jahr, 0) + pm
                
                a62_lookup[ma_id] = {
                    'qual_gruppe': int(extract_value(r'<cg_VMS_PK_aQualGruppe>([^<]+)', block) or '4'),
                    'sum_pm': extract_float(r'<sum_pm>([^<]+)', block),
                    'sum_pk': extract_float(r'<sum_pk>([^<]+)', block),
                    'pm_pro_jahr': pm_pro_jahr
                }
        
        # Mitarbeiter aus Anlage 6.1
        ma_patterns = [
            r'<cg_file_261_a71>(.*?)</cg_file_261_a71>',
            r'<Teilform_page13>(.*?)</Teilform_page13>',
        ]
        
        found_ma_ids = set()
        for pattern in ma_patterns:
            for block in re.findall(pattern, xfa_norm, re.DOTALL):
                ma_id = extract_value(r'<cg_DdsId_261>([^<]+)', block)
                if not ma_id or ma_id in found_ma_ids:
                    continue
                
                nachname = extract_value(r'<cg_VMS_PM_aNachname>([^<]+)', block)
                vorname = extract_value(r'<cg_VMS_PM_aVorname>([^<]+)', block)
                
                if not nachname and not vorname:
                    continue
                
                found_ma_ids.add(ma_id)
                a62_data = a62_lookup.get(ma_id, {'qual_gruppe': 4, 'sum_pm': 0, 'sum_pk': 0, 'pm_pro_jahr': {}})
                
                mitarbeiter.append({
                    'ma_nr': int(ma_id) if ma_id.isdigit() else len(mitarbeiter) + 1,
                    'nachname': nachname,
                    'vorname': vorname,
                    'qualifikation': extract_value(r'<cg_VMS_PM_aQualFachAusb>([^<]+)', block),
                    'qualifikation_gruppe': a62_data['qual_gruppe'],
                    'geburtsdatum': extract_value(r'<cg_VMS_PM_dGeburtsdatum>([^<]+)', block),
                    'funktion': extract_value(r'<cg_VMS_PM_aFunktion>([^<]+)', block),
                    'angestellt_seit': extract_value(r'<cg_VMS_PM_dAngestSeit>([^<]+)', block),
                    'jahresbrutto': extract_float(r'<Jahresbrutto>([^<]+)', block) or
                                    extract_float(r'<cg_VMS_PM_iJahresbrutto>([^<]+)', block),
                    'stundensatz': extract_float(r'<std_satz>([^<]+)', block),
                    'wochenstunden': extract_float(r'<cg_VMS_PM_fWochArbeitsz>([^<]+)', block),
                    'teilzeitfaktor': extract_float(r'<cg_VMS_PM_fTeilzFaktor>([^<]+)', block) or 1.0,
                    'pm_gesamt': a62_data['sum_pm'],
                    'kosten_gesamt': a62_data['sum_pk'],
                    'pm_pro_jahr': a62_data['pm_pro_jahr'],
                })
        
        mitarbeiter.sort(key=lambda x: x['ma_nr'])
        
        # === ARBEITSPAKETE ===
        arbeitspakete = []
        ap_temp = {}
        
        zeile2_pattern = r'<Zeile2><lfd>([^<]*)</lfd>(?:<ap>([^<]*)</ap>|<ap/>)(?:<von>([^<]*)</von>|<von/>)(?:<bis>([^<]*)</bis>|<bis/>)(?:<ma_nr>([^<]*)</ma_nr>|<ma_nr/>)(?:<pm>([^<]*)</pm>|<pm/>)</Zeile2>'
        
        current_ap_code = None
        current_ap_name = None
        
        for match in re.finditer(zeile2_pattern, xfa_norm):
            lfd, ap, von, bis, ma_nr, pm = match.groups()
            
            lfd = lfd.strip() if lfd else ''
            ap = ap.strip() if ap else ''
            ma_nr = ma_nr.strip() if ma_nr else ''
            pm = pm.strip() if pm else ''
            
            if not ma_nr or not pm:
                if lfd and ap:
                    current_ap_code = lfd
                    current_ap_name = ap
                continue
            
            if lfd and ap:
                current_ap_code = lfd
                current_ap_name = ap
            
            if lfd and lfd not in ap_temp:
                haupt_nr, unter_nr = parse_ap_nummer(lfd)
                ap_temp[lfd] = {
                    'ap_nummer': haupt_nr,
                    'ap_unter_nummer': unter_nr,
                    'ap_code': f'AP{lfd}',
                    'name': ap or current_ap_name or f'Arbeitspaket {lfd}',
                    'start_monat': None,
                    'ende_monat': None,
                    'von_datum': von,
                    'bis_datum': bis,
                    'zuordnungen': []
                }
            
            if lfd and ma_nr:
                pm_val = parse_float_value(pm)
                ma_nr_int = int(ma_nr) if ma_nr.isdigit() else 0
                
                if pm_val > 0 and ma_nr_int > 0:
                    ap_temp[lfd]['zuordnungen'].append({
                        'ma_nr': ma_nr_int,
                        'pm': pm_val
                    })
        
        # AP-Liste erstellen
        def sort_key(code):
            haupt, unter = parse_ap_nummer(code)
            return (haupt, unter)
        
        for ap_code in sorted(ap_temp.keys(), key=sort_key):
            ap_data = ap_temp[ap_code]
            gesamt_pm = sum(z['pm'] for z in ap_data['zuordnungen'])
            
            arbeitspakete.append({
                'ap_nummer': ap_data['ap_nummer'],
                'ap_code': ap_data['ap_code'],
                'name': ap_data['name'],
                'start_monat': ap_data['start_monat'],
                'ende_monat': ap_data['ende_monat'],
                'gesamt_pm': round(gesamt_pm, 2),
                'mitarbeiter_zuordnungen': ap_data['zuordnungen']
            })
        
        # Statistik
        total_zuordnungen = sum(len(ap['mitarbeiter_zuordnungen']) for ap in arbeitspakete)
        
        return {
            'projekt': projekt,
            'antragsteller': antragsteller,
            'budget': budget,
            'mitarbeiter': mitarbeiter,
            'arbeitspakete': arbeitspakete,
            'parse_datum': datetime.now().isoformat(),
            'quell_datei': filename,
            'statistik': {
                'anzahl_mitarbeiter': len(mitarbeiter),
                'anzahl_arbeitspakete': len(arbeitspakete),
                'anzahl_ap_zuordnungen': total_zuordnungen,
                'gesamt_pm': projekt['gesamt_pm'] or sum(m['pm_gesamt'] for m in mitarbeiter),
                'gesamt_pk': projekt['gesamt_pk'],
                'laufzeit_monate': projekt['laufzeit_monate'],
            }
        }
        
    finally:
        # Temp-Datei löschen
        os.unlink(tmp_path)


# ============================================
# API ENDPOINTS
# ============================================

@app.get("/")
async def root():
    """Health Check"""
    return {
        "service": "ZIM PDF Parser",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "POST /parse": "PDF hochladen und parsen",
            "GET /health": "Health Check"
        }
    }


@app.get("/health")
async def health():
    """Health Check für Monitoring"""
    return {"status": "healthy"}


@app.post("/parse")
async def parse_pdf(file: UploadFile = File(...)):
    """
    Parst ein ZIM-PDF und gibt strukturierte Daten zurück.
    
    - **file**: ZIM-Förderantrag als PDF (XFA-Format)
    
    Returns: JSON mit Projekt, Antragsteller, Mitarbeiter, Arbeitspakete
    """
    
    # Validierung
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Nur PDF-Dateien erlaubt")
    
    # Größenlimit (20 MB)
    content = await file.read()
    if len(content) > 20 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Datei zu groß (max 20 MB)")
    
    try:
        result = parse_zim_pdf(content, file.filename)
        return JSONResponse(content={
            "success": True,
            "data": result
        })
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Parser-Fehler: {str(e)}")


# ============================================
# RUN SERVER
# ============================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
