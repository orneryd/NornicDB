#!/usr/bin/env python3
"""
Directly download Qwen 0.5B Instruct model files using Python.
Bypasses SSL issues and network retry problems.
"""

import os
import sys
import ssl

# Disable SSL verification
ssl._create_default_https_context = ssl._create_unverified_context
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''

# Patch requests
try:
    import requests
    _original_request = requests.Session.request
    def patched_request(self, method, url, **kwargs):
        kwargs['verify'] = False
        return _original_request(self, method, url, **kwargs)
    requests.Session.request = patched_request
except Exception:
    pass

from huggingface_hub import snapshot_download
import logging

logging.basicConfig(level=logging.INFO)

def download_model():
    """Download Qwen 0.5B Instruct model."""
    model_id = "Qwen/qwen3-0.6b-Instruct"
    cache_dir = os.path.expanduser("~/.cache/huggingface/hub")
    
    print(f"Downloading {model_id}...")
    print(f"Cache directory: {cache_dir}")
    print("This may take a while (model is ~1GB)...")
    print()
    
    try:
        # Download all files
        local_dir = snapshot_download(
            repo_id=model_id,
            cache_dir=cache_dir,
            local_files_only=False,
            resume_download=True,
        )
        
        print(f"\n✓ Model downloaded successfully!")
        print(f"Location: {local_dir}")
        print(f"\nYou can now use it with:")
        print(f"  python3 train_nornicdb_cypher.py --train-only \\")
        print(f"      --dataset data/nornicdb_cypher.jsonl \\")
        print(f"      --local-model {local_dir}")
        
        return local_dir
    except Exception as e:
        print(f"\n✗ Download failed: {e}")
        print("\nAlternative: Try downloading manually from:")
        print(f"  https://huggingface.co/{model_id}")
        print("\nOr use the GGUF file you already have (but it won't work for training)")
        return None

if __name__ == "__main__":
    download_model()
