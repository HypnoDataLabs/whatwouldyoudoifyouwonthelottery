#!/usr/bin/env python3
import os, sys, json

"""
Thin client for vision OCR extraction.
- Expects env VISION_API_URL and VISION_API_KEY.
- CLI: python3 layer2/vision_extract.py <image_path>
- Output: JSON list of normalized records or [].
"""

def main():
    img = sys.argv[1] if len(sys.argv) > 1 else None
    if not img or not os.path.exists(img):
        print("[]")
        return

    api_url = os.getenv("VISION_API_URL")
    api_key = os.getenv("VISION_API_KEY")
    if not api_url or not api_key:
        # Not configured yet — return empty.
        print("[]")
        return

    # Example: POST multipart to your gateway that calls GPT-4o with a strict schema.
    # Implement for your backend; leave stubbed for now.
    try:
        import requests
        files = {'image': open(img, 'rb')}
        headers = {'Authorization': f'Bearer {api_key}'}
        r = requests.post(api_url, headers=headers, files=files, timeout=60)
        r.raise_for_status()
        data = r.json()
        print(json.dumps(data))
    except Exception:
        print("[]")

if __name__ == "__main__":
    main()
