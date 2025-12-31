#!/usr/bin/env python3
"""Fetch full article content for a list of URLs.

Usage:
  python fetch_urls.py --input urls.txt --output articles.json
  python fetch_urls.py --url https://example.com/article1 --url https://example.com/article2 -o out.json
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import List

import requests
from bs4 import BeautifulSoup

# Import the extractor from fetch_latest_news.py
try:
    from fetch_latest_news import fetch_article_content
except Exception:
    # If import fails (different working dir), fallback to copying minimal extractor
    def fetch_article_content(url: str, timeout: int = 10) -> str:
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
        except Exception:
            return ""
        soup = BeautifulSoup(r.content, "html.parser")
        paragraphs = [p.get_text(strip=True) for p in soup.find_all("p")]
        if not paragraphs:
            return ""
        out = []
        length = 0
        for p in paragraphs:
            if not p:
                continue
            out.append(p)
            length += len(p)
            if length > 1500:
                break
        return "\n\n".join(out)


def read_urls_from_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    return lines


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fetch article content for given URLs")
    parser.add_argument("--input", "-i", help="Path to text file with one URL per line")
    parser.add_argument("--url", "-u", action="append", help="URL to fetch (can be repeated)")
    parser.add_argument("--output", "-o", default="articles.json", help="Output JSON file")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout seconds")
    args = parser.parse_args(argv)

    urls: List[str] = []
    if args.input:
        urls.extend(read_urls_from_file(args.input))
    if args.url:
        urls.extend(args.url)

    if not urls:
        print("No URLs provided. Use --input or --url.", file=sys.stderr)
        sys.exit(2)

    results = []
    for u in urls:
        print(f"Fetching: {u}")
        try:
            content = fetch_article_content(u, timeout=args.timeout)
        except Exception as e:
            content = ""
        # Try to get title too
        title = ""
        try:
            r = requests.get(u, timeout=args.timeout, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            s = BeautifulSoup(r.content, "html.parser")
            t = s.find("title")
            if t:
                title = t.get_text(strip=True)
        except Exception:
            pass

        results.append({"url": u, "title": title, "content": content})

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(results)} items to {args.output}")


if __name__ == "__main__":
    main()
