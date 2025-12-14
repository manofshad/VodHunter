import os
import sqlite3
import yt_dlp
import soundfile as sf
import numpy as np
import torch
from transformers import ASTFeatureExtractor, ASTModel
from tqdm import tqdm  # Progress bar

# --- CONFIGURATION ---
DB_PATH = 'metadata.db'
VECTOR_FILE = 'vectors.npy'
ID_FILE = 'ids.npy'
AUDIO_DIR = 'temp_ingest'
os.makedirs(AUDIO_DIR, exist_ok=True)

# --- 1. SETUP GPU (Metal/MPS) ---
# This is the critical speed fix for Mac
if torch.backends.mps.is_available():
    device = torch.device("mps")
    print("🚀 Using Apple Metal (GPU) acceleration!")
elif torch.cuda.is_available():
    device = torch.device("cuda")
    print("🚀 Using NVIDIA CUDA (GPU) acceleration!")
else:
    device = torch.device("cpu")
    print("🐢 Using CPU (Slow mode)...")

print("⏳ Loading AI Model (AST)...")
feature_extractor = ASTFeatureExtractor.from_pretrained("MIT/ast-finetuned-audioset-10-10-0.4593")
model = ASTModel.from_pretrained("MIT/ast-finetuned-audioset-10-10-0.4593").to(device)
model.eval()  # Tell model we are not training, just predicting


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS creators (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        url TEXT UNIQUE
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        creator_id INTEGER,
        url TEXT,
        title TEXT,
        processed BOOLEAN DEFAULT FALSE,
        FOREIGN KEY(creator_id) REFERENCES creators(id)
    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS fingerprints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        video_id INTEGER,
        timestamp_seconds REAL,
        FOREIGN KEY(video_id) REFERENCES videos(id)
    )''')
    conn.commit()
    conn.close()


def download_audio(url, filename_base):
    print(f"⬇️  Downloading {url}...")

    # Make sure we use an absolute path and no extension in filename_base
    audio_dir_abs = os.path.abspath(AUDIO_DIR)
    base = os.path.join(audio_dir_abs, filename_base)  # e.g. .../temp_ingest/vid_1_636f3060

    ydl_opts = {
        "format": "bestaudio/best",
        # yt-dlp will create something like base.webm/mp4, then FFmpegExtractAudio → base.wav
        "outtmpl": base + ".%(ext)s",
        "nopart": True,          # <— disables .part/.temp style partial files
        "overwrites": True,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "wav",
        }],
        "postprocessor_args": ["-ar", "16000", "-ac", "1"],
        "quiet": True,
        # If ffmpeg/ffprobe are not on PATH on Windows, uncomment this and set the dir:
        # "ffmpeg_location": r"C:\ffmpeg\bin",
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)

    wav_path = base + ".wav"
    return wav_path, info.get("title", "Unknown")


def get_embeddings(audio_path):
    print(f"🧠 Generating AI embeddings on {device}...")

    # Read the WHOLE file
    # (4 hours of 16kHz audio is ~450MB of RAM. Your Mac can handle this fine.)
    audio_data, sr = sf.read(audio_path)

    # --- NO LIMITS ANYMORE ---
    # We removed the cropping logic here.

    # Keep batch_size low (4 or 8) to protect your GPU VRAM
    one_second = 16000
    batch_size = 8  # Increased slightly from 4 to 8 for speed, should be safe.

    embeddings = []
    timestamps = []

    total_samples = len(audio_data)
    num_chunks = int(np.ceil(total_samples / one_second))

    print(f"📊 Processing full stream: {num_chunks} seconds (~{num_chunks / 3600:.1f} hours)")
    print(f"☕ This will take roughly {num_chunks / (batch_size * 2) / 60:.0f} minutes.")

    for i in tqdm(range(0, num_chunks, batch_size), unit="batch"):
        batch_audio = []
        batch_times = []

        for j in range(i, min(i + batch_size, num_chunks)):
            start = j * one_second
            end = start + one_second
            chunk = audio_data[start:end]

            if len(chunk) < one_second:
                chunk = np.pad(chunk, (0, one_second - len(chunk)))

            batch_audio.append(chunk)
            batch_times.append(start / 16000.0)

        if not batch_audio:
            continue

        inputs = feature_extractor(batch_audio, sampling_rate=16000, return_tensors="pt")
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)

        batch_embs = outputs.pooler_output.cpu().numpy()
        embeddings.extend(batch_embs)
        timestamps.extend(batch_times)

    return embeddings, timestamps


def save_vectors(new_vecs, new_ids):
    """Saves vectors to a simple numpy file"""
    if os.path.exists(VECTOR_FILE) and os.path.exists(ID_FILE):
        existing_vecs = np.load(VECTOR_FILE)
        existing_ids = np.load(ID_FILE)
        combined_vecs = np.concatenate([existing_vecs, np.array(new_vecs)], axis=0)
        combined_ids = np.concatenate([existing_ids, np.array(new_ids)], axis=0)
    else:
        combined_vecs = np.array(new_vecs)
        combined_ids = np.array(new_ids)

    np.save(VECTOR_FILE, combined_vecs)
    np.save(ID_FILE, combined_ids)
    print(f"💾 Saved {len(combined_vecs)} total vectors to disk.")


def index_video(url, creator_name):
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("INSERT OR IGNORE INTO creators (name, url) VALUES (?, ?)", (creator_name, url))
    cursor.execute("SELECT id FROM creators WHERE name = ?", (creator_name,))
    creator_id = cursor.fetchone()[0]

    file_base = f"vid_{creator_id}_{os.urandom(4).hex()}"
    try:
        audio_path, title = download_audio(url, file_base)
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return

    cursor.execute("INSERT INTO videos (creator_id, url, title, processed) VALUES (?, ?, ?, ?)",
                   (creator_id, url, title, True))
    video_id = cursor.lastrowid

    embeddings, timestamps = get_embeddings(audio_path)

    ids_list = []
    print("📝 Writing to database...")
    for i, _ in enumerate(embeddings):
        cursor.execute("INSERT INTO fingerprints (video_id, timestamp_seconds) VALUES (?, ?)",
                       (video_id, float(timestamps[i])))
        row_id = cursor.lastrowid
        ids_list.append(row_id)

    conn.commit()
    conn.close()

    save_vectors(embeddings, ids_list)

    if os.path.exists(audio_path):
        os.remove(audio_path)
    print("✅ Indexing Complete.")


def index_local_file(local_path, original_url, creator_name):
    """
    Ingests a file that is already on your hard drive.
    Skips the download step.
    """
    print(f"📂 reading local file: {local_path}")

    # 1. Initialize DB (in case you just deleted it)
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 2. Register Creator
    # We still need this so the database knows who 'jason' is
    cursor.execute("INSERT OR IGNORE INTO creators (name, url) VALUES (?, ?)", (creator_name, original_url))
    cursor.execute("SELECT id FROM creators WHERE name = ?", (creator_name,))
    creator_id = cursor.fetchone()[0]

    # 3. Register Video
    # We use a placeholder title since we aren't asking Twitch for info
    title = "Manual Import"
    cursor.execute("INSERT INTO videos (creator_id, url, title, processed) VALUES (?, ?, ?, ?)",
                   (creator_id, original_url, title, True))
    video_id = cursor.lastrowid

    # 4. Embed (This is where the GPU work happens)
    # We pass the local path directly!
    embeddings, timestamps = get_embeddings(local_path)

    # 5. Save to Database
    ids_list = []
    print("📝 Writing to database...")
    for i, _ in enumerate(embeddings):
        cursor.execute("INSERT INTO fingerprints (video_id, timestamp_seconds) VALUES (?, ?)",
                       (video_id, float(timestamps[i])))
        row_id = cursor.lastrowid
        ids_list.append(row_id)

    conn.commit()
    conn.close()

    # 6. Save Vectors to Disk
    save_vectors(embeddings, ids_list)
    print("✅ Local Indexing Complete.")


if __name__ == "__main__":
    existing_file = "temp_ingest/vid_1_393c9e61.wav" # Check this matches your actual file
    original_url = "https://www.twitch.tv/videos/2631086912"

    if os.path.exists(existing_file):
        index_local_file(existing_file, original_url, "jason")