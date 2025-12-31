#!/usr/bin/env python3
"""Driver to fetch articles from specific sites (sina, 163) and save by topic/date.

Example:
  python fetch_sites.py --site sina --count 10 --outdir output
  python fetch_sites.py --site 163 --count 20 --outdir output
"""
from __future__ import annotations

import argparse
import sys
from typing import List, Tuple
import os
import time
from datetime import datetime, timedelta, date

from site_connectors import (
    get_sina_urls,
    get_163_urls,
    get_caixin_urls,
    get_wallstreetcn_urls,
    get_cicc_urls,
    get_eastmoney_urls,
    get_yicai_urls,
    get_thepaper_urls,
)
from save_by_topic_date import save_items
from topic_classifier import classify
from bs4 import BeautifulSoup
import requests
from requests.exceptions import RequestException


def _extract_content_by_site(site: str, soup: BeautifulSoup) -> tuple[str, str, str]:
    """Return (title, snippet, content) using site-specific selectors.

    Sites supported:
    - wallstreetcn: use element with class 'main'
    - sina: use title from class 'main-title' and article body from class 'article'
    - 163 (netease): article body from class 'post_main'
    Falls back to generic extraction when selectors not found.
    """
    # title fallback
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else ""
    snippet = ""
    paragraphs = []

    try:
        if site in ("wallstreetcn", "huajian"):
            main = soup.find(class_="main") or soup.find(id="main")
            if main:
                paragraphs = [p.get_text(strip=True) for p in main.find_all("p")]
                # also try header text inside main
                if not title:
                    h = main.find(["h1", "h2"])
                    if h:
                        title = h.get_text(strip=True)

        elif site in ("sina",):
            # title from .main-title if present
            mt = soup.find(class_="main-title")
            if mt:
                title = mt.get_text(strip=True)
            article = soup.find(class_="article") or soup.find(id="article")
            if article:
                paragraphs = [p.get_text(strip=True) for p in article.find_all("p")]
            else:
                # fall back to common article containers
                article = soup.find("article")
                if article:
                    paragraphs = [p.get_text(strip=True) for p in article.find_all("p")]

        elif site in ("163", "netease"):
            post = soup.find(class_="post_main") or soup.find(id="post_main")
            if post:
                paragraphs = [p.get_text(strip=True) for p in post.find_all("p")]

        else:
            # generic: try article role or largest <article>
            article = soup.find("article")
            if article:
                paragraphs = [p.get_text(strip=True) for p in article.find_all("p")]
    except Exception:
        paragraphs = []

    # fallback to first body paragraphs if nothing found
    if not paragraphs:
        paragraphs = [pp.get_text(strip=True) for pp in soup.find_all("p")]

    # snippet and content assembly
    snippet = paragraphs[0] if paragraphs else ""
    out_pars = []
    length = 0
    for pp in paragraphs:
        if not pp:
            continue
        out_pars.append(pp)
        length += len(pp)
        if length > 4000:
            break
    content = "\n\n".join(out_pars)
    return title, snippet, content


def gather(site: str, count: int) -> List[Tuple[str, str]]:
    if site == "sina":
        return get_sina_urls(count=count)
    if site in ("163", "netease"):
        return get_163_urls(count=count)
    if site == "caixin":
        return get_caixin_urls(count=count)
    if site in ("wallstreetcn", "huajian"):
        return get_wallstreetcn_urls(count=count)
    if site == "cicc":
        return get_cicc_urls(count=count)
    if site == "eastmoney":
        return get_eastmoney_urls(count=count)
    if site == "yicai":
        return get_yicai_urls(count=count)
    if site == "thepaper":
        return get_thepaper_urls(count=count)
    raise ValueError(f"Unknown site: {site}")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fetch from site connectors and save by topic/date")
    # Default excludes Sina to reduce noisy sources; include additional high-quality sources.
    parser.add_argument("--sites", type=str, default="wallstreetcn,eastmoney,yicai,thepaper,caixin,cicc", help="Comma-separated sites to fetch: wallstreetcn,eastmoney,yicai,thepaper,caixin,cicc[,163][,sina]")
    parser.add_argument("--count", type=int, default=500, help="Per-topic max number of articles to save (across sites) (default 500)")
    parser.add_argument("--outdir", default="output", help="Output directory for text files")
    parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default 15)")
    parser.add_argument("--retries", type=int, default=2, help="Number of fetch retries on failure (default 2)")
    parser.add_argument("--dry-run", action="store_true", help="Do not save files; just show which articles would be selected/rejected")
    parser.add_argument("--verbose", action="store_true", help="Verbose per-URL logging explaining why candidates were skipped or selected")
    parser.add_argument("--ignore-seen", action="store_true", help="Ignore entries in the seen-file and attempt to re-fetch previously seen URLs")
    parser.add_argument("--categories", type=str, default="communications,consumer discretionary,consumer staples,energy,financials,health care,industrials,materials,real estate,technology,utilities", help="Comma-separated target categories (sector list)")
    parser.add_argument("--must-contain", type=str, help="Comma-separated keywords; if provided, article must contain at least one in title/snippet")
    parser.add_argument("--keyword-threshold", type=int, default=2, help="Minimum keyword occurrences required for sector classification (default: 2)")
    parser.add_argument("--seen-file", type=str, help="Path to file maintaining seen URLs (one per line)")
    parser.add_argument("--target-date", type=str, help="Only include articles whose publish date equals this YYYY-MM-DD")
    parser.add_argument("--max-age-days", type=int, default=2, help="Exclude articles older than this many days (applies when --target-date is not provided)")
    parser.add_argument("--include-international", action="store_true", help="Also fetch international sources defined in international_sources.json")
    parser.add_argument("--intl-max", type=int, default=200, help="Max international articles per source (soft cap via config and filtering)")
    args = parser.parse_args(argv)

    sites = [s.strip().lower() for s in args.sites.split(",") if s.strip()]
    target_cats = set([c.strip().lower() for c in args.categories.split(",") if c.strip()])
    must_keywords = [k.strip().lower() for k in (args.must_contain or "").split(",") if k.strip()]

    selected = []
    headers = {"User-Agent": "Mozilla/5.0"}
    seen = set()
    if not args.ignore_seen and args.seen_file and os.path.exists(args.seen_file):
        try:
            with open(args.seen_file, "r", encoding="utf-8") as sf:
                for line in sf:
                    u = line.strip()
                    if u:
                        seen.add(u)
        except Exception:
            seen = set()
    # For each site, fetch candidates then select up to args.count per category (across sites)
    per_topic_limit = args.count
    # global counters per category (across all sites)
    sector_counts = {c: 0 for c in target_cats}
    all_full = False
    
    # Check if we already have enough content (useful when re-running)
    remaining_sectors = [cat for cat, count in sector_counts.items() if count < per_topic_limit and cat in target_cats]
    if not remaining_sectors:
        print(f"All target sectors already have sufficient content ({per_topic_limit} each). No need to fetch more.")
        return
    
    for site in sites:
        # Show current progress before processing each site
        remaining_sectors = [cat for cat, count in sector_counts.items() if count < per_topic_limit and cat in target_cats]
        if args.verbose or args.dry_run:
            filled_sectors = [f"{cat}:{sector_counts[cat]}/{per_topic_limit}" for cat in target_cats if sector_counts[cat] >= per_topic_limit]
            remaining_info = [f"{cat}:{sector_counts[cat]}/{per_topic_limit}" for cat in remaining_sectors]
            print(f"\n🌐 Processing site '{site}' | Remaining: [{', '.join(remaining_info)}] | Filled: [{', '.join(filled_sectors)}]")
        
        if not remaining_sectors:
            print(f"✅ All sectors full, skipping site '{site}' and remaining sites.")
            break
            
        try:
            # Fetch candidate links with adaptive sizing based on remaining slots across sectors
            # so we don't over-crawl when most sectors are already full.
            remaining_sectors = [cat for cat, count in sector_counts.items() if count < per_topic_limit and cat in target_cats]
            remaining_slots = sum(max(0, per_topic_limit - sector_counts.get(cat, 0)) for cat in remaining_sectors)
            if site in ("wallstreetcn", "eastmoney"):
                base_factor = 3  # prioritize but still adaptive
            elif site in ("yicai", "thepaper"):
                base_factor = 2
            elif site in ("cicc",):
                base_factor = 2
            elif site in ("caixin",):
                base_factor = 2
            else:
                base_factor = 1
            adaptive_count = max(10, remaining_slots * base_factor)
            # Upper bound to avoid runaway crawling when limits are large
            max_cap = per_topic_limit * 10
            requested_count = min(adaptive_count, max_cap)
            candidates = gather(site, requested_count)
        except Exception as e:
            print(f"Error gathering links from {site}: {e}", file=sys.stderr)
            continue

        # diagnostics
        total_candidates = len(candidates)
        skipped_seen = 0
        skipped_member = 0
        fetch_failures = 0
        classifier_rejects = 0

        processed_requests = 0
        for topic, url in candidates:
            if (not args.ignore_seen) and (url in seen):
                skipped_seen += 1
                if args.verbose or args.dry_run:
                    print(f"SKIP(seen): {url}")
                continue

            # Skip member-only paths (defense-in-depth)
            try:
                path = requests.utils.urlparse(url).path or ""
                if "/member/" in path or path.startswith("/member"):
                    skipped_member += 1
                    if args.verbose or args.dry_run:
                        print(f"SKIP(member-only): {url}")
                    continue
            except Exception:
                # if parsing fails, proceed to attempt fetch
                pass

            # fetch with retries/backoff
            r = None
            last_err = None
            for attempt in range(args.retries + 1):
                try:
                    r = requests.get(url, headers=headers, timeout=args.timeout)
                    r.raise_for_status()
                    processed_requests += 1
                    break
                except RequestException as e:
                    last_err = e
                    if attempt < args.retries:
                        backoff = 2 ** attempt
                        time.sleep(backoff)
                        continue
                    else:
                        fetch_failures += 1
                        if args.verbose or args.dry_run:
                            print(f"SKIP(fetch-failed): {url} -> {e}")
                        r = None

            if r is None:
                # couldn't fetch this URL
                continue

            try:
                s = BeautifulSoup(r.content, "html.parser")
                # use site-specific extractor to get title/snippet/content
                title, snippet, content = _extract_content_by_site(site, s)
            except Exception as e:
                if args.verbose or args.dry_run:
                    print(f"SKIP(parse-failed): {url} -> {e}")
                # attempt a minimal recovery: pull the <title> from raw text if possible
                try:
                    tmp = BeautifulSoup(r.text if r is not None else "", "html.parser")
                    tt = tmp.find("title")
                    title = tt.get_text(strip=True) if tt else ""
                except Exception:
                    title = ""
                snippet = ""
                content = ""

            # Use combined title + full content for classification to improve hit rate
            combined_snippet = (snippet or "") + "\n\n" + ((content or "")[:5000])
            matches = classify(url, title, combined_snippet, min_keyword_count=args.keyword_threshold)
            if not matches:
                classifier_rejects += 1
                if args.verbose or args.dry_run:
                    print(f"FALLBACK(classifier->none): {url} -> no sectors meet threshold ({args.keyword_threshold})")
                    preview = (content or "")[:200].replace("\n", " ")
                    print(f"TITLE: {title}")
                    print(f"PREVIEW: {preview}")
                    # Show keyword counts for debugging
                    from topic_classifier import SECTOR_MAP, _count_matches
                    debug_txt = " ".join(filter(None, [url, title or "", combined_snippet or ""]))
                    for sector_name, patterns in SECTOR_MAP:
                        count = _count_matches(debug_txt, patterns)
                        if count > 0:
                            print(f"  {sector_name}: {count} keyword matches (threshold: {args.keyword_threshold})")
                # If user explicitly requested 'others', treat as fallback; otherwise skip
                if "others" in target_cats:
                    matches = ["others"]
                else:
                    continue

            # filter matches to only the requested target categories (excluding already full sectors)
            available_cats = {cat for cat, count in sector_counts.items() if count < per_topic_limit}
            matches = [m for m in matches if m in available_cats]
            if not matches:
                # no intersection with available targets; possibly fallback to 'others'
                classifier_rejects += 1
                if args.verbose or args.dry_run:
                    print(f"FALLBACK(classifier->no-available-targets): {url} -> matched but all targets full")
                if "others" in available_cats:
                    matches = ["others"]
                else:
                    continue  # Skip processing since no sectors need more content

            if must_keywords:
                combined = (title + " " + snippet).lower()
                if not any(k in combined for k in must_keywords):
                    if args.verbose or args.dry_run:
                        print(f"SKIP(must-contain): {url} -> no keyword match")
                    continue

            # Date filtering: exact date if --target-date, else max-age-days
            try:
                from save_by_topic_date import extract_publish_date
                if r and r.content:
                    s_temp = BeautifulSoup(r.content, "html.parser")
                    pub_date = extract_publish_date(s_temp) if s_temp else None
                    if pub_date:
                        if args.target_date:
                            try:
                                from datetime import date
                                td = date.fromisoformat(args.target_date)
                                if pub_date != td:
                                    if args.verbose or args.dry_run:
                                        print(f"SKIP(date-mismatch): {url} -> publish {pub_date}, target {td}")
                                    continue
                            except Exception:
                                pass
                        else:
                            days_old = (datetime.now().date() - pub_date).days
                            if days_old > max(0, int(args.max_age_days or 0)):
                                if args.verbose or args.dry_run:
                                    print(f"SKIP(too-old): {url} -> {days_old} days old ({pub_date})")
                                continue
            except Exception as e:
                # If date extraction fails, continue processing
                if args.verbose:
                    print(f"Warning: date extraction failed for {url}: {e}")
                pass

            # For multi-label: add this URL to every matched sector that still needs items
            added_any = False
            for m in matches:
                # Double check in case sector became full during this loop iteration
                if sector_counts.get(m, 0) >= per_topic_limit:
                    if args.verbose or args.dry_run:
                        print(f"SKIP(limit): {url} -> sector '{m}' reached limit ({per_topic_limit}) during processing")
                    continue
                selected.append({"topic": m, "url": url, "title": title, "content": content})
                sector_counts[m] = sector_counts.get(m, 0) + 1
                added_any = True
                if args.verbose or args.dry_run:
                    # Show keyword match count for this sector
                    from topic_classifier import SECTOR_MAP, _count_matches
                    debug_txt = " ".join(filter(None, [url, title or "", combined_snippet or ""]))
                    sector_patterns = dict(SECTOR_MAP).get(m, [])
                    keyword_count = _count_matches(debug_txt, sector_patterns)
                    print(f"SELECT: {url} -> category '{m}' ({keyword_count} keywords) title='{title}' ({sector_counts[m]}/{per_topic_limit})")

            if added_any:
                seen.add(url)

            # check if all target sectors have reached the per-topic limit
            remaining_sectors = [cat for cat, count in sector_counts.items() if count < per_topic_limit and cat in target_cats]
            if not remaining_sectors:
                all_full = True
                if args.verbose or args.dry_run:
                    print(f"🎯 All target sectors have reached the per-topic limit ({per_topic_limit}). Stopping current site to save resources.")
                    print(f"📊 Final counts: {', '.join([f'{cat}:{sector_counts[cat]}' for cat in target_cats])}")
                break
            # adaptive stop: if we've already made enough successful requests relative to remaining slots, stop early
            remaining_slots = sum(max(0, per_topic_limit - sector_counts.get(cat, 0)) for cat in remaining_sectors)
            dynamic_cap = max(5, remaining_slots * 2)
            if processed_requests >= dynamic_cap:
                if args.verbose or args.dry_run:
                    print(f"⏹️ Adaptive stop for site '{site}': processed_requests={processed_requests} >= cap={dynamic_cap} (remaining_slots={remaining_slots}).")
                break

        # site-level diagnostic summary
        print(f"Site '{site}': candidates={total_candidates}, skipped_seen={skipped_seen}, skipped_member={skipped_member}, fetch_failures={fetch_failures}, classifier_rejects={classifier_rejects}")

        # Check after each site if we can stop early
        if all_full:
            remaining_sites = sites[sites.index(site)+1:]
            if remaining_sites:
                print(f"✅ All sectors satisfied! Skipping remaining sites: {', '.join(remaining_sites)}")
            break

    # If dry-run, print final summary and exit without saving
    if args.dry_run:
        print(f"DRY-RUN: total selected={len(selected)}")
        return

    # Optionally fetch international sources and merge
    if args.include_international:
        try:
            from fetch_international_news import InternationalNewsAggregator
            print("\n🌍 Fetching international sources (integrated)...")
            agg = InternationalNewsAggregator()
            all_items = agg.fetch_all_sources()
            intl_count = 0
            for src, items in (all_items or {}).items():
                for it in items[: max(0, args.intl_max)]:
                    selected.append({
                        "title": it.title,
                        "url": it.url,
                        "content": it.full_content or it.content or "",
                        "pub_date": (it.published.isoformat() if it.published else None)
                    })
                    intl_count += 1
            print(f"✅ Integrated {intl_count} international items")
        except Exception as e:
            print(f"Warning: failed to integrate international sources: {e}")

    if not selected:
        print("No articles matched the requested categories/filters or were retrieved from international sources.")
        return

    saved = save_items(selected, args.outdir, timeout=args.timeout)

    # update seen-file
    if args.seen_file and saved:
        try:
            with open(args.seen_file, "a", encoding="utf-8") as sf:
                for u in saved:
                    sf.write(u + "\n")
        except Exception as e:
            print(f"Warning: failed to update seen-file: {e}")


if __name__ == "__main__":
    main()
