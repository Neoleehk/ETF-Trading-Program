#!/usr/bin/env python3
"""Fetch latest market news from Yahoo Finance.

Usage examples:
  python fetch_latest_news.py --count 5
  python fetch_latest_news.py --count 10 --json --output news.json
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
from typing import List

import requests
from bs4 import BeautifulSoup
import html
from typing import Optional
import time
import feedparser

BASE_URL = "https://finance.yahoo.com"
NEWS_URL = BASE_URL + "/news"


def fetch_article_content(url: str, timeout: int = 10) -> str:
    """Fetch an article page and try to extract the main textual content.

    Uses a few heuristics: prefer <article> tags, role="main", or the
    largest block of consecutive <p> tags. Returns a cleaned text snippet.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0 Safari/537.36"
        )
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
    except Exception:
        return ""

    # (readability-lxml removed) Use BeautifulSoup heuristics below for extraction

    # Use raw bytes so BeautifulSoup can detect the correct encoding (important for Chinese sites)
    soup = BeautifulSoup(r.content, "html.parser")

    # 1) Look for <article>
    article_tag = soup.find("article")
    if article_tag:
        ps = [p.get_text(strip=True) for p in article_tag.find_all("p")]
        text = "\n\n".join([t for t in ps if t])
        if len(text) > 80:
            return html.unescape(text).strip()

    # 2) role="main" or id/class contains article/story
    main = soup.find(attrs={"role": "main"})
    if main:
        ps = [p.get_text(strip=True) for p in main.find_all("p")]
        text = "\n\n".join([t for t in ps if t])
        if len(text) > 80:
            return html.unescape(text).strip()

    for kw in ("article", "story", "post", "main"):
        candidate = soup.find(lambda tag: tag.name == "div" and tag.get("class") and any(kw in c.lower() for c in tag.get("class")))
        if candidate:
            ps = [p.get_text(strip=True) for p in candidate.find_all("p")]
            text = "\n\n".join([t for t in ps if t])
            if len(text) > 80:
                return html.unescape(text).strip()

    # 3) Fallback: find longest run of consecutive <p> siblings
    paragraphs = [p.get_text(strip=True) for p in soup.find_all("p")]
    if paragraphs:
        # join top paragraphs until reaching ~1200 chars
        out = []
        length = 0
        for p in paragraphs:
            if not p:
                continue
            out.append(p)
            length += len(p)
            if length > 1200:
                break
        return html.unescape("\n\n".join(out)).strip()

    return ""


def fetch_latest_news(count: int = 10, timeout: int = 10, fetch_content: bool = False) -> List[dict]:
    """Fetch a list of latest news items from Yahoo Finance (headlines + links).

    If `fetch_content` is True, attempt to fetch each article page and include
    a `content` field with the extracted text (may be empty if extraction fails).
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0 Safari/537.36"
        )
    }

    resp = requests.get(NEWS_URL, headers=headers, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.content, "html.parser")

    articles: List[dict] = []
    seen = set()

    # Find headline anchors across heading tags first
    for heading_tag in ("h1", "h2", "h3", "h4"):
        for h in soup.find_all(heading_tag):
            a = h.find("a", href=True)
            if not a:
                continue
            href = a["href"]
            if "/news/" not in href:
                continue
            title = a.get_text(strip=True)
            if not title or title in seen:
                continue
            seen.add(title)
            link = urllib.parse.urljoin(BASE_URL, href)
            summary = ""
            parent = h.parent
            if parent:
                p = parent.find("p")
                if p:
                    summary = p.get_text(strip=True)
            item = {"title": title, "link": link, "summary": summary}
            if fetch_content:
                item["content"] = fetch_article_content(link, timeout=timeout)
                time.sleep(0.2)
            articles.append(item)
            if len(articles) >= count:
                return articles

    # Fallback: anchors with '/news/' anywhere
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/news/" not in href:
            continue
        title = a.get_text(strip=True)
        if not title or title in seen:
            continue
        seen.add(title)
        link = urllib.parse.urljoin(BASE_URL, href)
        summary = ""
        parent = a.find_parent()
        if parent:
            p = parent.find("p")
            if p:
                summary = p.get_text(strip=True)
        item = {"title": title, "link": link, "summary": summary}
        if fetch_content:
            item["content"] = fetch_article_content(link, timeout=timeout)
            time.sleep(0.2)
        articles.append(item)
        if len(articles) >= count:
            break

    return articles


def fetch_from_rss(rss_url: str, count: int = 10, timeout: int = 10, fetch_content: bool = False) -> List[dict]:
    """Fetch items from an RSS/Atom feed using `feedparser`.

    Returns list of dicts with `title`, `link`, `summary`, and optional `content`.
    """
    feed = feedparser.parse(rss_url)
    items: List[dict] = []
    if not feed or not getattr(feed, "entries", None):
        return items

    for entry in feed.entries[:count]:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        summary = entry.get("summary", "") or entry.get("description", "")
        item = {"title": title, "link": link, "summary": BeautifulSoup(summary, "html.parser").get_text(strip=True)}
        if fetch_content and link:
            item["content"] = fetch_article_content(link, timeout=timeout)
            time.sleep(0.2)
        items.append(item)

    return items


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fetch latest market news from Yahoo Finance")
    parser.add_argument("--count", "-n", type=int, default=10, help="Number of articles to fetch")
    parser.add_argument("--json", action="store_true", help="Output results as JSON to stdout")
    parser.add_argument("--output", "-o", type=str, help="Write output to file (use .json for JSON)")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout in seconds")
    parser.add_argument("--fetch-content", action="store_true", help="Also fetch and extract full article content")
    parser.add_argument("--rss-url", type=str, default="http://feeds.reuters.com/reuters/topNews", help="RSS/Atom feed URL to use for RSS source or fallback")
    parser.add_argument("--source", choices=("yahoo", "rss", "auto"), default="yahoo", help="Source to use: 'yahoo' (scrape Yahoo), 'rss' (use RSS), or 'auto' (try Yahoo then RSS)")
    args = parser.parse_args(argv)

    items = []
    try:
        if args.source == "yahoo":
            items = fetch_latest_news(count=args.count, timeout=args.timeout, fetch_content=args.fetch_content)
        elif args.source == "rss":
            items = fetch_from_rss(args.rss_url, count=args.count, timeout=args.timeout, fetch_content=args.fetch_content)
        else:  # auto
            try:
                items = fetch_latest_news(count=args.count, timeout=args.timeout, fetch_content=args.fetch_content)
            except Exception:
                items = []
            if not items:
                items = fetch_from_rss(args.rss_url, count=args.count, timeout=args.timeout, fetch_content=args.fetch_content)
    except Exception as e:
        print(f"Error fetching news: {e}", file=sys.stderr)
        sys.exit(2)

    if args.json or (args.output and args.output.lower().endswith(".json")):
        data = json.dumps(items, ensure_ascii=False, indent=2)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(data)
            print(f"Wrote {len(items)} items to {args.output}")
        else:
            print(data)
        return

    # Human-friendly output
    for i, it in enumerate(items, start=1):
        print(f"{i}. {it['title']}")
        print(f"   Link: {it['link']}")
        if it.get("summary"):
            print(f"   {it['summary']}")
        if args.fetch_content and it.get("content"):
            # print a short excerpt of the extracted content
            excerpt = it["content"].strip().replace("\n", " ")
            if excerpt:
                if len(excerpt) > 400:
                    excerpt = excerpt[:400].rstrip() + "..."
                print(f"   Content excerpt: {excerpt}")
        print()


if __name__ == "__main__":
    main()
