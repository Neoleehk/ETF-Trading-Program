#!/usr/bin/env python3
"""Fetch URLs and save content into .txt files organized by market+topic and date.

Input format (one per line):
  Topic<TAB>URL
Examples:
  Today's news\thttps://www.yahoo.com/news/
  US\thttps://www.yahoo.com/news/us/

If a line contains only a URL, the script will use the URL path as the topic.

Files are written to an `output` directory next to the script. Filenames:
  <market>_<topic>_<YYYY-MM-DD>.txt or <topic>_<YYYY-MM-DD>.txt (if no market classification)
Multiple items for the same market+topic+date are appended to the same file.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:
    from fetch_latest_news import fetch_article_content
except Exception:
    # fallback simple extractor if import fails
    def fetch_article_content(url: str, timeout: int = 10) -> str:
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
        except Exception:
            return ""
        s = BeautifulSoup(r.text, "html.parser")
        ps = [p.get_text(strip=True) for p in s.find_all("p")]
        out = []
        length = 0
        for p in ps:
            if not p:
                continue
            out.append(p)
            length += len(p)
            if length > 2000:
                break
        return "\n\n".join(out)

# Import market+sector classification
try:
    from topic_classifier import classify_market_and_sector
    ENABLE_MARKET_CLASSIFICATION = True
except ImportError:
    ENABLE_MARKET_CLASSIFICATION = False
    print("Warning: topic_classifier not available, using legacy topic-only classification")


def read_topic_url_lines(path: str) -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "\t" in line:
                topic, url = line.split("\t", 1)
                items.append((topic.strip(), url.strip()))
            else:
                # single token: use last path segment as topic
                url = line
                topic = _topic_from_url(url)
                items.append((topic, url))
    return items


def _topic_from_url(url: str) -> str:
    # derive a short topic label from URL path
    try:
        p = requests.utils.urlparse(url).path
        if not p or p == "/":
            return "home"
        segs = [s for s in p.split("/") if s]
        return segs[0] if segs else "topic"
    except Exception:
        return "topic"


def extract_publish_date(soup: BeautifulSoup) -> Optional[date]:
    """Try several heuristics to find a publish date on the page and return a date object."""
    # 1) meta property article:published_time
    meta_props = [
        ("property", "article:published_time"),
        ("name", "pubdate"),
        ("name", "publishdate"),
        ("name", "date"),
        ("itemprop", "datePublished"),
    ]
    for attr, val in meta_props:
        m = soup.find("meta", attrs={attr: val})
        if m and m.get("content"):
            dt = _parse_datetime_string(m["content"])
            if dt:
                return dt.date()

    # 2) time tag with datetime
    t = soup.find("time")
    if t and t.get("datetime"):
        dt = _parse_datetime_string(t["datetime"])
        if dt:
            return dt.date()

    # 3) look for date-like strings in elements with class names like date, published
    candidates = soup.find_all(attrs={"class": re.compile(r"date|time|published|pubDate", re.I)})
    for c in candidates:
        text = c.get_text(separator=" ", strip=True)
        dt = _parse_datetime_string(text)
        if dt:
            return dt.date()

    return None


def _parse_datetime_string(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # Try ISO first
    try:
        # Some ISO strings include trailing Z
        if s.endswith("Z"):
            s2 = s.replace("Z", "+00:00")
            return datetime.fromisoformat(s2)
        return datetime.fromisoformat(s)
    except Exception:
        pass
    # Try common patterns with regex (YYYY-MM-DD)
    m = re.search(r"(20\d{2})[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12][0-9]|3[01])", s)
    if m:
        try:
            return datetime.fromisoformat(m.group(0))
        except Exception:
            pass
    # Try email.utils parser (RFC2822)
    try:
        import email.utils

        parsed = email.utils.parsedate_to_datetime(s)
        if parsed:
            return parsed
    except Exception:
        pass
    return None


def safe_filename(s: str) -> str:
    s = s.strip().replace(" ", "_")
    s = re.sub(r"[^0-9A-Za-z_\-\.]+", "", s)
    return s or "topic"


def save_items(items: List, out_dir: str, timeout: int = 10, enable_market_classification: bool = True) -> List[str]:
    """Save items into files grouped by market+topic and publish date.

    Items may be either (topic, url) tuples (legacy behavior) or dicts with keys:
      - topic (str), url (str)
      - optional: title (str), content (str), pub_date (YYYY-MM-DD string)

    Returns list of URLs that were saved (useful for updating seen-file).
    
    Args:
        items: List of items to save
        out_dir: Output directory
        timeout: Request timeout
        enable_market_classification: Whether to use market+sector classification
    """
    os.makedirs(out_dir, exist_ok=True)
    grouped: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    saved_urls: List[str] = []
    
    use_market_classification = enable_market_classification and ENABLE_MARKET_CLASSIFICATION

    for item in items:
        if isinstance(item, tuple) or isinstance(item, list):
            topic, url = item
            print(f"Fetching {url} (topic={topic})")
            try:
                # fetch raw page for date extraction and title
                r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
            except Exception as e:
                print(f"  Failed to fetch {url}: {e}")
                soup = BeautifulSoup("", "html.parser")

            pub_date = extract_publish_date(soup) or date.today()
            pub_date_str = pub_date.isoformat()

            # Title
            title_tag = soup.find("title")
            title = title_tag.get_text(strip=True) if title_tag else url

            # Content: use extractor (may re-download inside)
            try:
                content = fetch_article_content(url, timeout=timeout)
            except Exception:
                content = ""
            
            # Apply market+sector classification if enabled
            if use_market_classification:
                try:
                    market, sectors = classify_market_and_sector(url, title, content)
                    if sectors:
                        # Create entries for each sector
                        for sector in sectors:
                            market_topic = f"{market}_{sector}"
                            grouped[(market_topic, pub_date_str)].append({
                                "url": url, "title": title, "content": content
                            })
                    else:
                        # No sector classification, use market + general
                        market_topic = f"{market}_general"
                        grouped[(market_topic, pub_date_str)].append({
                            "url": url, "title": title, "content": content
                        })
                except Exception as e:
                    print(f"  Market classification failed for {url}: {e}, using legacy topic")
                    grouped[(topic, pub_date_str)].append({"url": url, "title": title, "content": content})
            else:
                # Legacy behavior: use original topic
                grouped[(topic, pub_date_str)].append({"url": url, "title": title, "content": content})
                
            saved_urls.append(url)
        elif isinstance(item, dict):
            # Prefer market+sector classification for dict items when enabled
            url = item.get("url")
            title = item.get("title") or url or ""
            content = item.get("content") or ""
            pub_date_str = item.get("pub_date")
            if not pub_date_str:
                html_text = item.get("html")
                if html_text:
                    soup = BeautifulSoup(html_text, "html.parser")
                    pub_date = extract_publish_date(soup) or date.today()
                    pub_date_str = pub_date.isoformat()
                else:
                    pub_date_str = date.today().isoformat()

            if use_market_classification:
                try:
                    market, sectors = classify_market_and_sector(url, title, content)
                    if sectors:
                        for sector in sectors:
                            market_topic = f"{market}_{sector}"
                            grouped[(market_topic, pub_date_str)].append({
                                "url": url, "title": title, "content": content
                            })
                    else:
                        market_topic = f"{market}_general"
                        grouped[(market_topic, pub_date_str)].append({
                            "url": url, "title": title, "content": content
                        })
                except Exception:
                    topic = item.get("topic") or item.get("category") or "topic"
                    grouped[(topic, pub_date_str)].append({"url": url, "title": title, "content": content})
            else:
                topic = item.get("topic") or item.get("category") or "topic"
                grouped[(topic, pub_date_str)].append({"url": url, "title": title, "content": content})
            saved_urls.append(url)
        else:
            # unsupported item type
            continue

    # Write files
    for (topic, pub_date_str), entries in grouped.items():
        fname = f"{safe_filename(topic)}_{pub_date_str}.txt"
        path = os.path.join(out_dir, fname)
        mode = "a" if os.path.exists(path) else "w"
        with open(path, mode, encoding="utf-8") as f:
            for e in entries:
                f.write(f"Title: {e['title']}\n")
                f.write(f"URL: {e['url']}\n")
                f.write(f"Date: {pub_date_str}\n\n")
                f.write(e.get('content') or "(no content extracted)")
                f.write("\n\n" + ("=" * 80) + "\n\n")
        print(f"Wrote {len(entries)} item(s) to {path}")

    return saved_urls


def main(argv=None):
    parser = argparse.ArgumentParser(description="Save fetched URLs into topic/date text files")
    parser.add_argument("--input", "-i", help="Input file with lines 'Topic<TAB>URL' or one URL per line")
    parser.add_argument("--outdir", "-d", default="output", help="Output directory")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout seconds")
    parser.add_argument("--url", "-u", action="append", help="Provide a single URL; can be repeated")
    parser.add_argument("--topic", "-t", help="Topic to use when passing single --url")
    args = parser.parse_args(argv)

    items: List[Tuple[str, str]] = []
    if args.input:
        items.extend(read_topic_url_lines(args.input))
    if args.url:
        for u in args.url:
            topic = args.topic or _topic_from_url(u)
            items.append((topic, u))

    if not items:
        print("No URLs provided. Use --input or --url.", file=sys.stderr)
        sys.exit(2)

    save_items(items, args.outdir, timeout=args.timeout)


if __name__ == "__main__":
    main()
