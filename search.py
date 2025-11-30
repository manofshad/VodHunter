import os
import sqlite3
import soundfile as sf
import numpy as np
import torch
from transformers import ASTFeatureExtractor, ASTModel
from collections import Counter

DB_PATH = 'metadata.db'
VECTOR_FILE = 'vectors.npy'
ID_FILE = 'ids.npy'
TEMP_DIR = 'temp_search'
os.makedirs(TEMP_DIR, exist_ok=True)

# 1. SETUP GPU
if torch.backends.mps.is_available():
    device = torch.device("mps")
elif torch.cuda.is_available():
    device = torch.device("cuda")
else:
    device = torch.device("cpu")

print("⏳ Loading AI Model for Search...")
feature_extractor = ASTFeatureExtractor.from_pretrained("MIT/ast-finetuned-audioset-10-10-0.4593")
model = ASTModel.from_pretrained("MIT/ast-finetuned-audioset-10-10-0.4593").to(device)
model.eval()


def process_query(file_path):
    print(f"🎤 Processing query clip...")
    clean_path = os.path.join(TEMP_DIR, "clean_query.wav")
    cmd = f'ffmpeg -i "{file_path}" -ar 16000 -ac 1 -y "{clean_path}" -loglevel error'
    os.system(cmd)

    if not os.path.exists(clean_path):
        raise Exception("FFmpeg failed to process audio")

    audio_data, sr = sf.read(clean_path)

    chunk_size = 16000
    embeddings = []
    timestamps = []

    total_samples = len(audio_data)
    for start_idx in range(0, total_samples, chunk_size):
        end_idx = start_idx + chunk_size
        chunk = audio_data[start_idx:end_idx]

        if len(chunk) < chunk_size:
            chunk = np.pad(chunk, (0, chunk_size - len(chunk)))

        inputs = feature_extractor(chunk, sampling_rate=16000, return_tensors="pt")
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)

        emb = outputs.pooler_output.cpu().numpy().squeeze()
        embeddings.append(emb)
        timestamps.append(start_idx / 16000.0)

    return embeddings, timestamps


def search_index(query_embeddings):
    if not os.path.exists(VECTOR_FILE) or not os.path.exists(ID_FILE):
        return [], []

    db_vecs = np.load(VECTOR_FILE)
    db_ids = np.load(ID_FILE)

    query_np = np.array(query_embeddings)

    db_norm = np.linalg.norm(db_vecs, axis=1, keepdims=True)
    query_norm = np.linalg.norm(query_np, axis=1, keepdims=True)
    db_vecs = db_vecs / (db_norm + 1e-10)
    query_np = query_np / (query_norm + 1e-10)

    scores = np.dot(query_np, db_vecs.T)
    # INCREASED: Look at top 10 neighbors to catch faint matches
    top_k_indices = np.argsort(scores, axis=1)[:, -10:]
    top_k_indices = top_k_indices[:, ::-1]

    I = db_ids[top_k_indices]
    D = np.take_along_axis(scores, top_k_indices, axis=1)

    return D, I


def align_results(I, query_timestamps):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    candidates = []

    for t_idx, neighbors in enumerate(I):
        q_time = query_timestamps[t_idx]
        for db_row_id in neighbors:
            cursor.execute("SELECT video_id, timestamp_seconds FROM fingerprints WHERE id=?", (int(db_row_id),))
            row = cursor.fetchone()
            if row:
                vid_id, db_time = row
                offset = int(round(db_time - q_time))
                candidates.append((vid_id, offset))

    if not candidates:
        print("❌ DEBUG: No candidates found (Vector search returned nothing close).")
        conn.close()
        return None

    # --- DEBUG SECTION ---
    print(f"\n🧐 DEBUG: Top 5 candidate timestamps:")
    top_candidates = Counter(candidates).most_common(5)
    for (vid, off), count in top_candidates:
        # Convert offset to HH:MM:SS for easy reading
        h, r = divmod(off, 3600)
        m, s = divmod(r, 60)
        print(f"   -> Video {vid} @ {h}h {m}m {s}s (Score: {count})")
    # ---------------------

    (best_vid, best_offset), score = top_candidates[0]

    # LOWERED THRESHOLD TO 1 FOR TESTING
    if score < 1:
        print("❌ DEBUG: Score too low.")
        conn.close()
        return None

    cursor.execute("""
        SELECT videos.title, videos.url, creators.name 
        FROM videos 
        JOIN creators ON videos.creator_id = creators.id 
        WHERE videos.id=?
    """, (best_vid,))

    info = cursor.fetchone()
    conn.close()

    return {
        "streamer": info[2],
        "video": info[1],
        "title": info[0],
        "timestamp": best_offset,
        "confidence": score,
        "link": f"{info[1]}?t={best_offset}s"
    }