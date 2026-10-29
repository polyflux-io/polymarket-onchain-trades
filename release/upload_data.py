"""Upload data/ to the HF repo year by year (one commit per year, resumable: unchanged files are skipped)."""
import os, time, glob
from huggingface_hub import HfApi
api = HfApi(token=os.environ["HF_TOKEN"])
repo = os.environ.get("HF_REPO", "ieshuaganocry/polymarket-onchain-trades")
os.chdir(os.environ.get("RELEASE_DIR", "release_out"))
years = sorted(os.path.basename(p) for p in glob.glob("data/year=*"))
for y in years:
    n = len(glob.glob(f"data/{y}/*.parquet")); gb = sum(os.path.getsize(f) for f in glob.glob(f"data/{y}/*.parquet")) / 1e9
    for attempt in range(1, 6):
        t0 = time.time()
        try:
            api.upload_folder(repo_id=repo, repo_type="dataset", folder_path=f"data/{y}", path_in_repo=f"data/{y}",
                              commit_message=f"data: {y} ({n} days, {gb:.1f} GB)")
            print(f"uploaded {y}: {n} files, {gb:.1f} GB in {time.time()-t0:.0f}s", flush=True)
            break
        except Exception as e:
            print(f"{y} attempt {attempt} failed after {time.time()-t0:.0f}s: {e!r}"[:300], flush=True)
            time.sleep(30 * attempt)
    else:
        raise SystemExit(f"FAILED {y}")
print("UPLOAD DONE", flush=True)
