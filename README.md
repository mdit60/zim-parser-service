# ZIM PDF Parser Microservice

FastAPI-basierter Microservice zum Parsen von ZIM-Förderanträgen (XFA-PDFs).

## Features

- Extrahiert Projektdaten, Antragsteller, Mitarbeiter und Arbeitspakete
- Unterstützt XFA-PDFs (Adobe-Format)
- REST API mit JSON-Output
- CORS-konfiguriert für Web-Zugriff

## API Endpoints

### `GET /`
Health Check und Service-Info

### `GET /health`
Einfacher Health Check

### `POST /parse`
PDF hochladen und parsen

**Request:**
```bash
curl -X POST "https://your-service.railway.app/parse" \
  -F "file=@ZIM-Antrag.pdf"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "projekt": { ... },
    "antragsteller": { ... },
    "mitarbeiter": [ ... ],
    "arbeitspakete": [ ... ],
    "statistik": { ... }
  }
}
```

## Deployment auf Railway

1. Repository auf GitHub erstellen
2. Railway.app → New Project → Deploy from GitHub
3. Repository auswählen
4. Automatisches Deployment startet

## Lokale Entwicklung

```bash
# Virtuelle Umgebung
python3 -m venv venv
source venv/bin/activate

# Dependencies installieren
pip install -r requirements.txt

# Server starten
uvicorn main:app --reload --port 8000
```

Test: http://localhost:8000/docs (Swagger UI)

## Environment Variables

- `PORT`: Server Port (default: 8000)
- `ALLOWED_ORIGIN`: Zusätzliche CORS-Origin (optional)
