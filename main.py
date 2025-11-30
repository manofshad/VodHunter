from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from fastapi.responses import HTMLResponse
import shutil
import os
import ingest
import search

app = FastAPI()

# Create temp folder if it doesn't exist
os.makedirs("temp_search", exist_ok=True)


# Serve the Frontend
@app.get("/", response_class=HTMLResponse)
async def read_root():
    # We assume index.html is in the same folder
    if os.path.exists("index.html"):
        with open("index.html", "r") as f:
            return f.read()
    return "<h1>Error: index.html not found. Please create it.</h1>"


# --- INGEST ENDPOINT ---
@app.post("/ingest")
async def ingest_endpoint(url: str, creator: str, background_tasks: BackgroundTasks):
    """
    Triggers the download and indexing of a Twitch VOD.
    Hides the latency by running in the background.
    """
    print(f"📥 API received ingest request: {url}")
    background_tasks.add_task(ingest.index_video, url, creator)
    return {"status": "started", "message": f"Indexing {url} in background. Check terminal for progress."}


# --- SEARCH ENDPOINT ---
@app.post("/search")
async def search_endpoint(file: UploadFile = File(...)):
    """
    Accepts a video/audio file, extracts the fingerprint, and finds the source.
    """
    temp_path = f"temp_search/{file.filename}"

    # 1. Save the uploaded file to disk temporarily
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        print(f"🕵️‍♀️ API received search request for: {file.filename}")

        # 2. Generate Embeddings (Using the new AST Model)
        # Note: The first time this runs, it might take a second to process.
        embs, ts = search.process_query(temp_path)

        # 3. Search Vector Database
        D, I = search.search_index(embs)

        # 4. Align Results (Find the time offset)
        result = search.align_results(I, ts)

        if result:
            print(f"✅ Found match: {result['title']}")
            return {"found": True, "data": result}
        else:
            print("❌ No match found.")
            return {"found": False, "message": "No matching video found."}

    except Exception as e:
        print(f"🔥 Error: {str(e)}")
        return {"found": False, "error": str(e)}

    finally:
        # 5. Cleanup: Delete the temp file
        if os.path.exists(temp_path):
            os.remove(temp_path)