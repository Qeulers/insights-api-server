# Insights API Server

This is a scaffolded FastAPI server.

## Setup

1. Create a virtual environment (optional but recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the server:
   ```bash
   uvicorn main:app --reload
   ```

The server will be available at http://127.0.0.1:8000

## Endpoints
- `GET /`: Health check, returns a welcome message.
