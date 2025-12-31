#!/usr/bin/env python3
"""Orchestrator: run industry-level analysis outputs through HKSI for company-level recommendations.

Usage:
    python integrate_hksi.py [--sector SECTOR] [--top N] [--ticker-db PATH] [--install-deps]

This script picks a sector (from output/sector_summary.json by default), finds the latest
sector text file in `output/` (e.g. `technology_YYYY-MM-DD.txt`), extracts article URLs,
runs `HKSI.process_url` on each URL and aggregates entity sentiments into a ranked list
of company candidates.
"""
from __future__ import annotations
import argparse
import importlib
import json
import os
import re
import sys
import csv
import difflib
import datetime
from collections import defaultdict
from pathlib import Path
from statistics import mean
import time
import string
import traceback
from typing import Any
import traceback


def load_hksi_module(base_path: Path):
    # base_path should point to HKSI-main/HKSI-main
    if str(base_path) not in sys.path:
        sys.path.insert(0, str(base_path))
    try:
        mod = importlib.import_module('HKSI')
    except Exception as exc:
        raise ImportError(f"Failed to import HKSI from {base_path}: {exc}")
    return mod


def pick_sector(sector_arg: str | None, summary_path: Path) -> str:
    if sector_arg:
        return sector_arg
    if not summary_path.exists():
        raise FileNotFoundError(f"Sector summary not found: {summary_path}")
    with summary_path.open('r', encoding='utf-8') as f:
        data = json.load(f)
    if not data:
        raise ValueError('sector_summary.json is empty')
    return data[0]['sector']


def find_latest_sector_file(output_dir: Path, sector: str, market: str = None) -> Path | None:
    """Find the latest sector file, supporting both legacy and market+sector formats.
    
    Args:
        output_dir: Directory to search in
        sector: Sector name (e.g., 'technology', 'financials')
        market: Optional market filter ('CN', 'HK', 'US')
        
    Returns:
        Path to the latest matching file, or None if not found
    """
    candidates = []
    
    # Priority 1: Market-specific files if market is specified
    if market:
        market_pattern = f"{market}_{sector.replace(' ', '_')}*"
        market_candidates = sorted(output_dir.glob(market_pattern), 
                                 key=lambda p: p.stat().st_mtime, reverse=True)
        candidates.extend(market_candidates)
    
    # Priority 2: All market files for this sector (CN, HK, US)
    for mkt in ['CN', 'HK', 'US']:
        if market and mkt == market:
            continue  # Already added above
        market_pattern = f"{mkt}_{sector.replace(' ', '_')}*"
        market_candidates = sorted(output_dir.glob(market_pattern),
                                 key=lambda p: p.stat().st_mtime, reverse=True)
        candidates.extend(market_candidates)
    
    # Priority 3: Legacy format files (no market prefix)
    legacy_pattern = f"{sector.replace(' ', '_')}*"
    legacy_candidates = sorted(output_dir.glob(legacy_pattern), 
                             key=lambda p: p.stat().st_mtime, reverse=True)
    # Filter out market-prefixed files to avoid duplicates
    legacy_candidates = [c for c in legacy_candidates 
                        if not any(c.name.startswith(f"{mkt}_") for mkt in ['CN', 'HK', 'US'])]
    candidates.extend(legacy_candidates)
    
    return candidates[0] if candidates else None


def find_market_sector_files(output_dir: Path, sector: str) -> dict[str, Path]:
    """Find all market-specific sector files for a given sector.
    
    Returns:
        Dict mapping market codes to file paths
    """
    result = {}
    for market in ['CN', 'HK', 'US']:
        file_path = find_latest_sector_file(output_dir, sector, market)
        if file_path and file_path.name.startswith(f"{market}_"):
            result[market] = file_path
    return result


def parse_urls_from_sector_file(path: Path) -> list[str]:
    urls = []
    # find any http/https URL anywhere in the file
    url_re = re.compile(r"(https?://[^\s'\"<>]+)", flags=re.I)
    with path.open('r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            for m in url_re.finditer(line):
                urls.append(m.group(1).strip())
    # de-duplicate while preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def save_failed_urls(failed: list[dict], out_dir: Path):
    if not failed:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / 'failed_urls.json'
    try:
        if path.exists():
            with path.open('r', encoding='utf-8') as f:
                prev = json.load(f)
        else:
            prev = []
    except Exception:
        prev = []
    prev.extend(failed)
    with path.open('w', encoding='utf-8') as f:
        json.dump(prev, f, ensure_ascii=False, indent=2)


def load_failed_urls(out_dir: Path) -> list[str]:
    path = out_dir / 'failed_urls.json'
    if not path.exists():
        return []
    try:
        with path.open('r', encoding='utf-8') as f:
            data = json.load(f)
        return [d.get('url') for d in data if isinstance(d, dict) and d.get('url')]
    except Exception:
        return []


def _read_sector_allocations(path: Path) -> dict[str, float]:
    out: dict[str, float] = {}
    if not path.exists():
        return out
    try:
        with path.open('r', encoding='utf-8') as f:
            # skip header
            next(f)
            for line in f:
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 3:
                    sec = parts[0]
                    try:
                        pct = float(parts[2])
                    except Exception:
                        pct = 0.0
                    out[sec] = pct
    except Exception:
        return {}
    return out


def _load_company_rank(sector: str, output_dir: Path) -> list[tuple[str, dict]]:
    path = output_dir / f'company_rank_{sector.replace(" ", "_")}.json'
    if not path.exists():
        return []
    try:
        with path.open('r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('ranked', [])
    except Exception:
        return []


def generate_recommendation_report(output_dir: Path, ticker_db: dict | None = None, portfolio_size: float = 0.0, strategy: str = 'simple', top_per_sector: int = 3, alias_db: dict | None = None, ticker_sectors: dict | None = None, etf_only: bool = True, allowed_markets: set[str] | None = None) -> dict[str, Any]:
    """Generate a recommendation report using `sector_allocations.csv` and any
    `company_rank_<sector>.json` files present in `output_dir`.
    Returns a dict with `text` (string) and `details` (structured JSON).
    """
    sec_path = output_dir / 'sector_allocations.csv'
    # If a single market is requested, use its specific allocations file when available
    try:
        if allowed_markets and len(allowed_markets) == 1:
            _m = next(iter(allowed_markets))
            mkt_path = output_dir / f'sector_allocations_{_m}.csv'
            if mkt_path.exists():
                sec_path = mkt_path
    except Exception:
        pass
    sectors = _read_sector_allocations(sec_path)
    report_lines: list[str] = []
    details: dict[str, Any] = {'date': datetime.date.today().isoformat(), 'sectors': []}

    report_lines.append(f"Recommendation Report — {details['date']}")
    report_lines.append("")
    report_lines.append("Top-level sector allocations:")
    SEC_ALIAS = {
        'real': 'real estate',
        'real_estate': 'real estate',
        'health_care': 'health',
        'healthcare': 'health',
        'consumer_discretionary': 'consumer',
        'consumer_staples': 'consumer staples'
    }
    for sec_raw, pct in sectors.items():
        sec_disp = SEC_ALIAS.get(sec_raw.lower(), sec_raw)
        report_lines.append(f"- {sec_disp}: {pct}%")
    report_lines.append("")

    # For each sector, produce per-security suggestions if available
    # sector name aliases for readability
    # Load optional ETF map for sector fallbacks (per market)
    etf_map: dict[str, dict[str, str]] = {}
    try:
        root = Path(__file__).resolve().parent
        etf_path = root / 'etf_map.json'
        if etf_path.exists():
            with etf_path.open('r', encoding='utf-8') as ef:
                etf_map = json.load(ef) or {}
    except Exception:
        etf_map = {}

    # optional market weight config (per sector or default)
    def _load_market_weights(cfg_path: Path) -> dict[str, dict[str, float]]:
        default_cfg = {
            'default': {'US': 0.5, 'HK': 0.3, 'CN': 0.2}
        }
        try:
            if cfg_path.exists():
                with cfg_path.open('r', encoding='utf-8') as f:
                    data = json.load(f) or {}
                    # basic validation: ensure numbers and normalize keys
                    cleaned = {}
                    for k, v in data.items():
                        mk = str(k).strip()
                        if isinstance(v, dict):
                            cleaned[mk] = {m.upper(): float(vv) for m, vv in v.items() if m and isinstance(vv, (int, float))}
                    return cleaned or default_cfg
        except Exception:
            pass
        return default_cfg

    market_weights_cfg = _load_market_weights(Path(__file__).resolve().parent / 'market_weights.json')

    # determine sector bias (long/short) by sector_summary avg_score if available, else by allocation ranking
    bias_map: dict[str, str] = {}
    try:
        summ_path = output_dir / 'sector_summary.json'
        if summ_path.exists():
            with summ_path.open('r', encoding='utf-8') as sf:
                summ = json.load(sf)
            # normalize names via alias
            scores: dict[str, float] = {}
            for row in (summ or []):
                nm = SEC_ALIAS.get(str(row.get('sector','')).lower(), str(row.get('sector','')))
                scores[nm] = float(row.get('avg_score', 0.0) or 0.0)
            if scores:
                vals = sorted(scores.values())
                if vals:
                    mid = vals[len(vals)//2]
                else:
                    mid = 0.0
                for nm, sc in scores.items():
                    bias_map[nm] = 'long' if sc >= mid else 'short'
    except Exception:
        bias_map = {}
    # fallback by allocation ranking (top half long, bottom half short)
    if not bias_map:
        ranked_secs = sorted([(SEC_ALIAS.get(k.lower(), k), v) for k, v in sectors.items()], key=lambda x: x[1], reverse=True)
        half = max(1, len(ranked_secs)//2)
        for i, (nm, _) in enumerate(ranked_secs):
            bias_map[nm] = 'long' if i < half else 'short'

    for sec_raw, pct in sectors.items():
        sec = SEC_ALIAS.get(sec_raw.lower(), sec_raw)
        sec_entry: dict[str, Any] = {'sector': sec, 'sector_pct': pct, 'suggestions': []}
        report_lines.append(f"Sector: {sec} — {pct}% of portfolio")
        ranked = [] if etf_only else _load_company_rank(sec, output_dir)
        core_pct = round(pct * 0.6, 2)
        sat_pct = round(pct * 0.3, 2)
        buffer_pct = round(pct * 0.1, 2)
        if ranked and not etf_only:
            # pick top companies (prefer those with tickers or names not numeric)
            # Prefer entries that have observed tickers; then fill with non-numeric names
            with_tk = []
            without_tk = []
            for name, info in ranked:
                if info.get('tickers'):
                    with_tk.append((name, info))
                else:
                    # skip pure numeric names
                    if re.fullmatch(r"\d+", name):
                        continue
                    without_tk.append((name, info))
            # initial choice
            chosen = with_tk[:top_per_sector]
            if len(chosen) < top_per_sector:
                chosen.extend(without_tk[:(top_per_sector - len(chosen))])
            if not chosen:
                chosen = ranked[:top_per_sector]
            # dedupe by canonical ticker (prefer first ticker in info['tickers']) or lowercase name
            seen_ids = set()
            deduped = []
            for name, info in chosen:
                tks = info.get('tickers') or []
                canon = (tks[0].upper() if tks else name.lower())
                if canon in seen_ids:
                    continue
                seen_ids.add(canon)
                deduped.append((name, info))
            chosen = deduped
            # allocate core to top (cap at 8% of portfolio)
            # choose allocation method
            if strategy == 'conviction-weighted':
                # weight by avg_score + pos count (simple score)
                weights = []
                for name, info in chosen:
                    score = float(info.get('avg_score', 0.0) or 0.0)
                    pos = int(info.get('pos', 0) or 0)
                    w = max(0.0, score) + 0.1 * pos
                    weights.append(max(w, 0.01))
                total_w = sum(weights) if weights else 1.0
                # core portion assigned proportionally but cap first to 8% absolute
                for idx, (name, info) in enumerate(chosen):
                    frac = weights[idx] / total_w
                    alloc_core = round(core_pct * frac, 2)
                    if idx == 0:
                        alloc_core = min(8.0, alloc_core)
                    sec_entry['suggestions'].append({'name': name, 'allocation_pct': alloc_core, 'role': 'core' if idx == 0 else 'satellite', 'info': info})
                # satellite split evenly across chosen (after core allocation)
                per_sat = round(sat_pct / max(1, len(chosen)), 2)
                for s in sec_entry['suggestions']:
                    if s['role'] == 'satellite':
                        s['allocation_pct'] = round(s.get('allocation_pct', 0.0) + per_sat, 2)
            else:
                remaining_core = core_pct
                for i, (name, info) in enumerate(chosen):
                    if i == 0:
                        alloc = min(8.0, remaining_core)
                    else:
                        alloc = round(remaining_core / (len(chosen) - i), 2) if (len(chosen) - i) > 0 else 0.0
                    remaining_core = round(max(0.0, remaining_core - alloc), 2)
                    sec_entry['suggestions'].append({'name': name, 'allocation_pct': alloc, 'role': 'core' if i == 0 else 'satellite', 'info': info})
                # if satellite remainder, split across chosen
                if sat_pct > 0:
                    per_sat = round(sat_pct / max(1, len(chosen)), 2)
                    for s in sec_entry['suggestions']:
                        if s['role'] == 'satellite':
                            s['allocation_pct'] = round(s.get('allocation_pct', 0.0) + per_sat, 2)
            report_lines.append(f"  Core: {core_pct}%  Satellite: {sat_pct}%  Buffer: {buffer_pct}%")
            # prepare lookups
            alias_lookup = {k.lower(): v for k, v in (alias_db.items() if alias_db else {})}
            ticker_lookup = {}
            if ticker_db:
                for tk, cname in ticker_db.items():
                    if cname:
                        ticker_lookup[cname.lower()] = tk

            # map tickers for each suggestion
            for s in sec_entry['suggestions']:
                # attempt to map to ticker: 1) exact cname match 2) alias (Chinese) exact/substr 3) fuzzy name match
                ticker = None
                name_l = s['name'].lower()
                # 1) exact match in ticker_db
                if name_l in ticker_lookup:
                    ticker = ticker_lookup[name_l]
                # 2) alias match (prefer Chinese aliases)
                if not ticker and alias_lookup:
                    if name_l in alias_lookup:
                        ticker = alias_lookup[name_l]
                    else:
                        # substring match if name contains non-ascii (likely Chinese)
                        if any(ord(ch) > 127 for ch in s['name']):
                            for a, tk in alias_lookup.items():
                                if a in name_l:
                                    ticker = tk
                                    break
                # 3) fuzzy match on English names
                if not ticker and ticker_db:
                    names = list(ticker_db.values())
                    matches = difflib.get_close_matches(s['name'], names, n=1, cutoff=0.6)
                    if matches:
                        for tk, cname in ticker_db.items():
                            if cname == matches[0]:
                                ticker = tk
                                break
                s['ticker'] = ticker
                # sector consistency filter: if we have a sector mapping, keep only matching tickers
                if ticker and ticker_sectors:
                    try:
                        mapped = ticker_sectors.get(ticker) or ticker_sectors.get(ticker.upper()) or ticker_sectors.get(ticker.lower())
                        if mapped:
                            mapped_norm = SEC_ALIAS.get(str(mapped).lower(), str(mapped))
                            if mapped_norm.lower() != sec.lower():
                                s['drop'] = True
                    except Exception:
                        pass
                # compute dollar amount if portfolio_size provided
                if portfolio_size and s.get('allocation_pct'):
                    s['allocation_amount'] = round(portfolio_size * (s['allocation_pct'] / 100.0), 2)
                # defer printing until after dedupe
            # final dedupe within sector by ticker/name to avoid repeated synonyms
            final = []
            seen_tickers = set()
            seen_names = set()
            for s in sec_entry['suggestions']:
                if s.get('drop'):
                    continue
                tk = (s.get('ticker') or '').upper()
                nm = s.get('name', '').lower()
                key_tk = tk or None
                key_nm = nm or None
                if key_tk:
                    if key_tk in seen_tickers:
                        continue
                    seen_tickers.add(key_tk)
                else:
                    if key_nm in seen_names:
                        continue
                    seen_names.add(key_nm)
                final.append(s)
            sec_entry['suggestions'] = final
            # now print deduped suggestions
            for s in sec_entry['suggestions']:
                tk = s.get('ticker')
                amt = s.get('allocation_amount')
                report_lines.append(f"    - {s['name']}{(' ('+tk+')') if tk else ''}: {s['allocation_pct']}% ({s['role']}){(' -> '+str(amt) if amt else '')}")
        else:
            # ETF-only strategy: choose long or short per sector bias
            mapped_key = SEC_ALIAS.get(sec.lower(), sec)
            etfs = etf_map.get(mapped_key) or etf_map.get(mapped_key.lower()) or {}
            bias = bias_map.get(sec, 'long')
            # long ETF candidates per market
            long_list = []
            for mkt in ['US','HK','CN']:
                tk = (etfs.get(mkt) or '').strip()
                if not tk:
                    continue
                if allowed_markets and mkt not in allowed_markets:
                    continue
                long_list.append((tk, mkt))
            # inverse ETF candidates (US focus)
            inv_tk = (etfs.get('inverseUS') or '').strip()
            inv_list = []
            if inv_tk:
                if (not allowed_markets) or ('US' in allowed_markets):
                    inv_list = [(inv_tk, 'US')]
            total_pct = round(core_pct + sat_pct, 2)
            report_lines.append(f"  ETF-only strategy: bias={bias}.")
            if bias == 'long' and long_list:
                # compute per-market weights (sector-specific override or default), normalized to available markets
                weights = market_weights_cfg.get(mapped_key) or market_weights_cfg.get(mapped_key.lower()) or market_weights_cfg.get('default') or {'US': 0.5, 'HK': 0.3, 'CN': 0.2}
                active_markets = [m for _, m in long_list]
                weights_active = {m: float(weights.get(m, 0.0)) for m in active_markets}
                total_w = sum(weights_active.values())
                if total_w <= 0:
                    # fallback to equal split if no valid weights
                    weights_active = {m: 1.0 for m in active_markets}
                    total_w = float(len(active_markets))
                # assign core to the highest-weight market, others satellite
                max_mkt = max(weights_active.items(), key=lambda x: x[1])[0]
                for tk, mkt in long_list:
                    frac = weights_active.get(mkt, 0.0) / total_w if total_w > 0 else 0.0
                    per_alloc = round(total_pct * frac, 2)
                    role = 'core' if mkt == max_mkt else 'satellite'
                    s = {'name': f'{sec} ETF {mkt}', 'ticker': tk, 'allocation_pct': per_alloc, 'role': role, 'direction': 'long'}
                    if portfolio_size and per_alloc:
                        s['allocation_amount'] = round(portfolio_size * (per_alloc / 100.0), 2)
                    sec_entry['suggestions'].append(s)
                    amt = s.get('allocation_amount')
                    report_lines.append(f"    - {s['name']} ({s['ticker']}): {s['allocation_pct']}% ({s['role']}){(' -> $'+str(amt) if amt else '')}")
            elif bias == 'short' and inv_list:
                per = round(total_pct / max(1, len(inv_list)), 2)
                for i, (tk, mkt) in enumerate(inv_list):
                    role = 'core' if i == 0 else 'satellite'
                    s = {'name': f'{sec} Inverse ETF {mkt}', 'ticker': tk, 'allocation_pct': per, 'role': role, 'direction': 'short'}
                    if portfolio_size and per:
                        s['allocation_amount'] = round(portfolio_size * (per / 100.0), 2)
                    sec_entry['suggestions'].append(s)
                    # Add ETF suggestions to text report
                    amt = s.get('allocation_amount')
                    report_lines.append(f"    - {s['name']} ({s['ticker']}): {s['allocation_pct']}% ({s['role']}){(' -> $'+str(amt) if amt else '')}")
            else:
                # without inverse availability, allocate zero to long and let trading engine reduce long (SELL) if held
                report_lines.append(f"  No inverse ETF mapped; will avoid long allocation and reduce any existing longs.")
                # place a placeholder with zero allocation to signal no buys
                sec_entry['suggestions'].append({'name': f'{sec} ETF', 'allocation_pct': 0.0, 'role': 'core', 'direction': 'neutral'})
        sec_entry['buffer_pct'] = buffer_pct
        details['sectors'].append(sec_entry)
        report_lines.append("")

    report_text = "\n".join(report_lines)
    return {'text': report_text, 'details': details}


def _fetch_yahoo_history(ticker: str, days: int = 90) -> list[float] | None:
    """Attempt to download historical daily close prices from Yahoo Finance CSV API.
    Returns list of closes (most recent last) or None on failure.
    """
    try:
        import requests
    except Exception:
        return None
    end = int(time.time())
    start = end - days * 86400
    symbol = ticker.replace(' ', '').replace('/', '-')
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={start}&period2={end}&interval=1d&events=history&includeAdjustedClose=true"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None
        text = r.text
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if len(lines) <= 1:
            return None
        closes = []
        for ln in lines[1:]:
            parts = ln.split(',')
            if len(parts) < 5:
                continue
            close = parts[4]
            try:
                c = float(close)
                closes.append(c)
            except Exception:
                continue
        return closes if closes else None
    except Exception:
        # on failure, try EastMoney fallback
        try:
            em = _fetch_eastmoney_history(ticker, days=days)
            return em
        except Exception:
            return None


def _fetch_eastmoney_history(ticker: str, days: int = 90) -> list[float] | None:
    """Best-effort: attempt to fetch historical closes from EastMoney by scraping the quote page.
    This is a heuristic fallback and may fail for non-China tickers. Returns list of closes (oldest..newest) or None.
    """
    try:
        import requests
    except Exception:
        return None
    # construct a simple EastMoney quote page URL; this may or may not contain a useful history table
    symbol = ticker.replace('.', '').upper()
    url = f"https://quote.eastmoney.com/{symbol}.html"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            return None
        text = r.text
        # attempt to find recent closing prices as numbers in the page (simple regex for floats)
        # take the last N matches that look like prices
        price_re = re.compile(r">\s*([0-9]+\.[0-9]{2,4})\s*<")
        found = [float(m.group(1)) for m in price_re.finditer(text)]
        if not found:
            # try another pattern: plain numbers in javascript arrays
            js_re = re.compile(r"\[([0-9\.\,\s]+)\]")
            for jm in js_re.finditer(text):
                nums = re.findall(r"[0-9]+\.[0-9]{2,4}", jm.group(1))
                if nums:
                    found = [float(x) for x in nums]
                    break
        if not found:
            return None
        # prefer the most recent 'days' entries from the end
        if len(found) > days:
            found = found[-days:]
        return found
    except Exception:
        return None


def _compute_annualized_volatility(closes: list[float]) -> float | None:
    if not closes or len(closes) < 5:
        return None
    import math
    rets = []
    for i in range(1, len(closes)):
        if closes[i-1] == 0:
            continue
        rets.append((closes[i] / closes[i-1]) - 1.0)
    if not rets:
        return None
    mean_ret = sum(rets) / len(rets)
    var = sum((r - mean_ret) ** 2 for r in rets) / (len(rets) - 1)
    std_daily = math.sqrt(var)
    std_annual = std_daily * math.sqrt(252)
    return std_annual


def adjust_allocations_by_volatility(details: dict[str, Any], portfolio_size: float, vol_window: int = 90) -> dict[str, Any]:
    """Adjust allocations inversely by volatility within each sector.
    Modifies `details` in-place and returns it.
    """
    for sec in details.get('sectors', []):
        suggestions = sec.get('suggestions', [])
        vols = []
        for s in suggestions:
            tk = s.get('ticker')
            if not tk:
                vols.append(None)
                s['volatility'] = None
                continue
            closes = _fetch_yahoo_history(tk, days=vol_window)
            vol = _compute_annualized_volatility(closes) if closes else None
            vols.append(vol)
            s['volatility'] = vol
        inv = [ (1.0 / v) if (v and v>0) else None for v in vols ]
        total_inv = sum(x for x in inv if x)
        if total_inv <= 0:
            continue
        sector_pct = sec.get('sector_pct', 0.0)
        for i, s in enumerate(suggestions):
            if inv[i] is None:
                continue
            frac = inv[i] / total_inv
            new_alloc = round(sector_pct * frac, 2)
            s['allocation_pct'] = new_alloc
            if portfolio_size and s.get('allocation_pct'):
                s['allocation_amount'] = round(portfolio_size * (s['allocation_pct'] / 100.0), 2)
    return details


def render_details_to_text(details: dict[str, Any]) -> str:
    lines = [f"Recommendation Report — {details.get('date', '')}", ""]
    for sec in details.get('sectors', []):
        lines.append(f"Sector: {sec.get('sector')} — {sec.get('sector_pct')}%")
        for s in sec.get('suggestions', []):
            ticker = s.get('ticker') or ''
            amt = s.get('allocation_amount')
            amt_str = f" -> {amt}" if amt is not None else ''
            vol = s.get('volatility')
            vol_str = f" vol={round(vol,4)}" if vol else ''
            lines.append(f"  - {s.get('name')}{(' ('+ticker+')') if ticker else ''}: {s.get('allocation_pct')}% ({s.get('role')}){amt_str}{vol_str}")
        lines.append("")
    return "\n".join(lines)


def _fetch_current_price_yahoo(ticker: str) -> float | None:
    try:
        import requests
    except Exception:
        return None
    symbol = ticker.replace(' ', '').replace('/', '-')
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            closes = _fetch_yahoo_history(ticker, days=3)
            return closes[-1] if closes else None
        data = r.json()
        quote = (data or {}).get('quoteResponse', {}).get('result', [])
        if not quote:
            closes = _fetch_yahoo_history(ticker, days=3)
            return closes[-1] if closes else None
        price = quote[0].get('regularMarketPrice') or quote[0].get('postMarketPrice') or quote[0].get('preMarketPrice')
        if price is None:
            closes = _fetch_yahoo_history(ticker, days=3)
            return closes[-1] if closes else None
        try:
            return float(price)
        except Exception:
            return None
    except Exception:
        closes = _fetch_yahoo_history(ticker, days=3)
        return closes[-1] if closes else None


def _fetch_yahoo_quote(ticker: str) -> dict | None:
    try:
        import requests
    except Exception:
        return None
    symbol = ticker.replace(' ', '').replace('/', '-')
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        result = (data or {}).get('quoteResponse', {}).get('result', [])
        return result[0] if result else None
    except Exception:
        return None


def _fetch_open_price_yahoo(ticker: str) -> float | None:
    q = _fetch_yahoo_quote(ticker)
    if q:
        val = q.get('regularMarketOpen')
        if val is None:
            val = q.get('regularMarketPrice')
        try:
            return float(val) if val is not None else None
        except Exception:
            return None
    # fallback: last close
    closes = _fetch_yahoo_history(ticker, days=3)
    return closes[-1] if closes else None


def _eastmoney_secid_candidates(ticker: str) -> list[str]:
    """Return candidate EastMoney secids. CN: ['1.code'/'0.code'], HK: ['116.00xxx'], US: try several forms like '105.SYMBOL'."""
    tk = (ticker or '').upper().strip()
    try:
        code = tk.split('.')[0]
        suffix = tk.split('.')[1] if '.' in tk else ''
    except Exception:
        code, suffix = tk, ''
    cands: list[str] = []
    # CN A-shares
    if suffix in ('SS', 'SH'):
        cands.append(f"1.{code}")
        return cands
    if suffix == 'SZ':
        cands.append(f"0.{code}")
        return cands
    # HK: normalize leading zeros (e.g., 0700 -> 00700)
    if suffix == 'HK':
        c = code
        if len(c) == 4:
            c = '0' + c
        elif len(c) < 5:
            c = c.rjust(5, '0')
        cands.append(f"116.{c}")
        return cands
    # US: EastMoney often uses 105.<symbol> on push2
    # try multiple variations in case of special characters
    sym = code.upper()
    cands.extend([
        f"105.{sym}",
        f"105.{sym.replace('.', '-')}",
        f"105.{sym.replace('.', '')}"
    ])
    return cands


def _fetch_price_yfinance(ticker: str) -> float | None:
    """Fetch price using yfinance library (more reliable than direct Yahoo API)"""
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        # Get most recent price data
        hist = stock.history(period="2d")
        if not hist.empty:
            # Try today's open, fallback to most recent close
            if 'Open' in hist.columns and not hist['Open'].isna().all():
                latest_open = hist['Open'].dropna().iloc[-1]
                return float(latest_open)
            elif 'Close' in hist.columns and not hist['Close'].isna().all():
                latest_close = hist['Close'].dropna().iloc[-1]
                return float(latest_close)
    except Exception:
        pass
    return None


def _fetch_price_alpha_vantage(ticker: str, api_key: str = None, last_call_time: dict = {}) -> float | None:
    """Fetch price using Alpha Vantage API (free tier: 500 requests/day)"""
    if not api_key:
        return None
        
    try:
        import requests
        import time
        
        # Rate limiting: max 5 calls per minute for free tier
        current_time = time.time()
        if 'last_alpha_vantage_call' in last_call_time:
            time_since_last = current_time - last_call_time['last_alpha_vantage_call']
            if time_since_last < 12:  # Wait at least 12 seconds between calls
                time.sleep(12 - time_since_last)
        
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={api_key}"
        r = requests.get(url, timeout=15)
        
        last_call_time['last_alpha_vantage_call'] = time.time()
        
        if r.status_code != 200:
            return None
            
        data = r.json()
        time_series = data.get('Time Series (Daily)', {})
        if not time_series:
            return None
            
        # Get most recent day's data
        latest_date = max(time_series.keys())
        latest_data = time_series[latest_date]
        open_price = latest_data.get('1. open')
        
        if open_price:
            return float(open_price)
            
    except Exception:
        pass
    return None


def _fetch_price_akshare(ticker: str) -> float | None:
    """Fetch price using AKShare library for comprehensive data"""
    try:
        import akshare as ak
        
        # Handle different ticker formats
        symbol = ticker.upper()
        
        # Try US stock first
        try:
            df = ak.stock_us_daily(symbol=symbol.replace('.', '-'))
            if not df.empty:
                return float(df['open'].iloc[-1])
        except:
            pass
            
        # Try ETF data
        try:
            df = ak.fund_etf_hist_sina(symbol=symbol)
            if not df.empty:
                return float(df['open'].iloc[-1])
        except:
            pass
            
    except Exception:
        pass
    return None


def _fetch_price_eastmoney_us(ticker: str) -> float | None:
    """Fetch US stock/ETF price from EastMoney - 改进版 (专门针对美股ETF)"""
    try:
        import requests
        import json
        
        # 先尝试东方财富API接口
        api_urls = [
            f"http://push2.eastmoney.com/api/qt/stock/get?secid=107.{ticker.upper()}&fields=f43,f44,f45,f46,f47,f48,f49,f50,f51,f52,f53,f54,f55,f56,f57,f58",
            f"http://push2.eastmoney.com/api/qt/stock/get?ut=bd1d9ddb04089700cf9c27f6f7426281&secid=107.{ticker.upper()}&fields=f43,f57,f58,f170,f46,f44,f51,f168,f47,f164,f163,f116,f60,f45,f52,f50,f48,f167,f117,f71",
            f"http://push2ex.eastmoney.com/getstockinfo?id=107{ticker.upper()}&ut=bd1d9ddb04089700cf9c27f6f7426281"
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'http://quote.eastmoney.com/',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        }
        
        for api_url in api_urls:
            try:
                response = requests.get(api_url, headers=headers, timeout=8)
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if 'data' in data and data['data']:
                            # 尝试多个可能的价格字段
                            price_fields = ['f43', 'f44', 'f45', 'f46', 'f57', 'f58', 'f2']
                            for field in price_fields:
                                if field in data['data'] and data['data'][field]:
                                    try:
                                        price = float(data['data'][field])
                                        # 价格合理性验证（ETF一般在10-1000范围）
                                        if 5 <= price <= 2000:
                                            return price
                                    except (ValueError, TypeError):
                                        continue
                    except (json.JSONDecodeError, KeyError):
                        continue
            except:
                continue
                
    except Exception:
        pass
    return None

def _fetch_price_sina_us(ticker: str) -> float | None:
    """Fetch US stock/ETF price from Sina Finance (新浪API作为美股数据源)"""
    try:
        import requests
        import re
        
        # 新浪的美股API
        url = f"https://hq.sinajs.cn/list=gb_{ticker.lower()}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://finance.sina.com.cn/'
        }
        
        response = requests.get(url, headers=headers, timeout=8)
        if response.status_code == 200:
            content = response.text
            # 新浪API返回格式解析
            match = re.search(rf'var hq_str_gb_{ticker.lower()}="([^"]+)"', content)
            if match:
                data_str = match.group(1)
                parts = data_str.split(',')
                if len(parts) > 1 and parts[1]:
                    try:
                        price = float(parts[1])  # 通常第2个字段是现价
                        if 0.1 <= price <= 5000:
                            return price
                    except (ValueError, IndexError):
                        pass
                        
    except Exception:
        pass
    return None

def _fetch_price_macromicro(ticker: str) -> float | None:
    """Fetch ETF price from MacroMicro.me (专业ETF数据平台)"""
    try:
        import requests
        from bs4 import BeautifulSoup
        import time
        import random
        
        # MacroMicro ETF页面URL模式
        url = f"https://www.macromicro.me/etf/us/intro/{ticker.upper()}"
        
        # 尝试多种不同的用户代理来避免403错误
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
        ]
        
        for attempt, user_agent in enumerate(user_agents):
            headers = {
                'User-Agent': user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            }
            
            try:
                # 添加session来维持cookie
                session = requests.Session()
                
                # 先访问主页建立session (可选)
                if attempt == 0:
                    try:
                        session.get('https://www.macromicro.me', headers=headers, timeout=8)
                        time.sleep(random.uniform(1, 3))  # 随机等待
                    except:
                        pass
                
                r = session.get(url, headers=headers, timeout=15)
                
                if r.status_code == 200:
                    soup = BeautifulSoup(r.content, 'html.parser')
                    
                    # 尝试多种可能的价格选择器
                    price_selectors = [
                        'span.text-2xl.font-bold',  # 主要价格显示
                        '.text-2xl.font-bold',      # 价格文本
                        '.price-value',             # 价格值类
                        '[data-testid="current-price"]',  # 当前价格测试ID
                        '.current-price',           # 当前价格类
                        'div.price',               # 通用价格div
                        'span.price',              # 通用价格span
                        '.font-bold',              # 粗体字
                        '.text-3xl',               # 大字体价格
                        '.text-xl',                # 中等字体
                        'h2.text-2xl',             # 大标题中的价格
                        'div[class*="price"]',     # 包含price的类名
                        'span[class*="price"]'     # 包含price的span
                    ]
                    
                    for selector in price_selectors:
                        price_elements = soup.select(selector)
                        for elem in price_elements:
                            if elem:
                                price_text = elem.get_text().strip()
                                # 清理价格文本，移除货币符号和空格
                                price_text = price_text.replace('$', '').replace(',', '').replace(' ', '').replace('USD', '')
                                
                                # 更严格的价格验证
                                import re
                                price_match = re.search(r'^(\d{1,4}(?:\.\d{1,4})?)$', price_text)
                                if price_match:
                                    try:
                                        price = float(price_match.group(1))
                                        # 验证价格合理性 (ETF价格通常在1-1000之间)
                                        if 0.1 <= price <= 2000:
                                            return price
                                    except ValueError:
                                        continue
                    
                    # 如果选择器方法失败，尝试正则表达式查找价格模式
                    text = soup.get_text()
                    # 查找类似 "$123.45" 或 "123.45" 的价格模式
                    price_patterns = [
                        r'\$\s*([0-9]{1,4}(?:\.[0-9]{1,4})?)',  # $123.45 格式
                        r'Current Price[:\s]*\$?\s*([0-9]{1,4}(?:\.[0-9]{1,4})?)',  # Current Price: $123.45
                        r'Price[:\s]*\$?\s*([0-9]{1,4}(?:\.[0-9]{1,4})?)',  # Price: 123.45
                        r'([0-9]{1,4}\.[0-9]{2})\s*USD',  # 123.45 USD 格式
                        r'"current_price"\s*:\s*"?([0-9]{1,4}(?:\.[0-9]{1,4})?)"?',  # JSON格式
                        r'"price"\s*:\s*"?([0-9]{1,4}(?:\.[0-9]{1,4})?)"?'  # JSON price字段
                    ]
                    
                    for pattern in price_patterns:
                        matches = re.findall(pattern, text, re.IGNORECASE)
                        for match in matches:
                            try:
                                price = float(match)
                                if 0.1 <= price <= 2000:  # 合理价格范围
                                    return price
                            except ValueError:
                                continue
                                
                elif r.status_code == 403:
                    # 403错误，快速尝试下一个用户代理
                    if attempt < len(user_agents) - 1:
                        continue
                    
            except Exception:
                # 如果这个用户代理失败了，快速尝试下一个
                if attempt < len(user_agents) - 1:
                    continue
                
    except Exception:
        pass
    return None


def _fetch_enhanced_price(ticker: str, alpha_vantage_key: str = None) -> float | None:
    """Enhanced price fetching with multiple fallback data sources"""
    
    # First try EastMoney (best for CN/HK)
    price = _fetch_open_price_eastmoney(ticker)
    if price:
        return price
        
    # For US tickers, try multiple sources in order of reliability
    if not ticker.endswith(('.HK', '.SH', '.SZ')):
        # Try yfinance first (most stable for US market)
        price = _fetch_price_yfinance(ticker)
        if price:
            return price
            
        # Try Sina Finance for US stocks (often reliable)
        price = _fetch_price_sina_us(ticker)
        if price:
            return price
            
        # Try EastMoney US interface (may have limitations)
        price = _fetch_price_eastmoney_us(ticker)
        if price:
            return price
            
        # Try AKShare for US data
        price = _fetch_price_akshare(ticker)
        if price:
            return price
            
        # Try MacroMicro for US ETFs (may have anti-bot restrictions)
        price = _fetch_price_macromicro(ticker)
        if price:
            return price
            
        # Try Alpha Vantage if API key provided
        if alpha_vantage_key:
            price = _fetch_price_alpha_vantage(ticker, alpha_vantage_key)
            if price:
                return price
    
    # Fallback to original Yahoo Finance
    price = _fetch_open_price_yahoo(ticker)
    if price:
        return price
        
    return None


def _fetch_open_price_eastmoney(ticker: str) -> float | None:
    """Fetch open price via EastMoney push2 API if possible (CN/HK/US)."""
    try:
        import requests
    except Exception:
        return None
    secids = _eastmoney_secid_candidates(ticker)
    for secid in secids:
        url = f"https://push2.eastmoney.com/api/qt/stock/get?fltt=2&secid={secid}&fields=f46,f84,f85"
        try:
            r = requests.get(url, timeout=10, headers={'Referer': 'https://quote.eastmoney.com/'})
            if r.status_code != 200:
                continue
            data = r.json()
            d = (data or {}).get('data', {})
            val = d.get('f46')  # open price
            if val is None:
                val = d.get('f85')  # latest price as fallback
            if val is None:
                continue
            try:
                return float(val)
            except Exception:
                continue
        except Exception:
            continue
    return None


def _load_positions(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {'date': datetime.date.today().isoformat(), 'cash': 0.0, 'positions': []}
    try:
        with path.open('r', encoding='utf-8') as f:
            data = json.load(f)
        # normalize to {'positions': [{ticker, shares, avg_cost?}], 'cash': float}
        pos = []
        cash = float(data.get('cash', 0.0) or 0.0)
        if isinstance(data.get('positions'), list):
            for p in data.get('positions') or []:
                tk = (p.get('ticker') or '').strip()
                sh = int(p.get('shares') or 0)
                ac = p.get('avg_cost')
                pos.append({'ticker': tk, 'shares': sh, 'avg_cost': ac})
        elif isinstance(data.get('positions'), dict):
            for tk, sh in data.get('positions').items():
                pos.append({'ticker': str(tk).strip(), 'shares': int(sh or 0), 'avg_cost': None})
        else:
            # allow top-level dict of ticker: shares
            for tk, sh in {k: v for k, v in data.items() if isinstance(v, (int, float))}.items():
                pos.append({'ticker': str(tk).strip(), 'shares': int(sh or 0), 'avg_cost': None})
        return {'date': data.get('date') or datetime.date.today().isoformat(), 'cash': cash, 'positions': pos}
    except Exception:
        return {'date': datetime.date.today().isoformat(), 'cash': 0.0, 'positions': []}


def _save_positions(path: Path, payload: dict[str, Any]):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _get_market(ticker: str) -> str:
    tk = (ticker or '').upper()
    if tk.endswith('.HK'):
        return 'HK'
    if tk.endswith('.SS') or tk.endswith('.SZ') or tk.endswith('.SH'):
        return 'CN'
    return 'US'


def _build_targets_from_details(details: dict[str, Any], portfolio_size: float | None, market_budgets: dict[str, float] | None = None, allowed_markets: set[str] | None = None) -> dict[str, dict[str, Any]]:
    allowed_markets = allowed_markets or {'CN','HK','US'}
    candidates: list[dict[str, Any]] = []
    for sec in details.get('sectors', []):
        for s in sec.get('suggestions', []):
            tk = s.get('ticker')
            if not tk:
                continue
            mkt = _get_market(tk)
            if mkt not in allowed_markets:
                continue
            direction = s.get('direction') or 'long'
            candidates.append({'ticker': tk, 'name': s.get('name'), 'allocation_pct': float(s.get('allocation_pct') or 0.0), 'allocation_amount': s.get('allocation_amount'), 'market': mkt, 'direction': direction})

    targets: dict[str, dict[str, Any]] = {}
    if market_budgets:
        # Normalize within each market and assign amounts from that market's budget
        by_market: dict[str, list[dict[str, Any]]] = {}
        for c in candidates:
            by_market.setdefault(c['market'], []).append(c)
        for mkt, lst in by_market.items():
            total_pct = sum(max(0.0, c['allocation_pct']) for c in lst)
            budget = float(market_budgets.get(mkt, 0.0) or 0.0)
            if budget <= 0 or total_pct <= 0:
                continue
            for c in lst:
                frac = (c['allocation_pct'] / total_pct) if total_pct > 0 else 0.0
                amt = round(budget * frac, 2)
                t = targets.setdefault(c['ticker'], {'name': c['name'], 'target_amount': 0.0, 'allocation_pct': c['allocation_pct'], 'direction': c.get('direction','long')})
                t['target_amount'] += amt
    else:
        # fallback to allocation_amount or leave to pct-based later
        for c in candidates:
            t = targets.setdefault(c['ticker'], {'name': c['name'], 'target_amount': 0.0, 'allocation_pct': c['allocation_pct'], 'direction': c.get('direction','long')})
            if c['allocation_amount'] is not None:
                t['target_amount'] += float(c['allocation_amount'])
    return targets


def _compute_portfolio_value(positions: list[dict[str, Any]], prices: dict[str, float], cash: float = 0.0) -> float:
    total = float(cash or 0.0)
    for p in positions:
        tk = p.get('ticker')
        sh = int(p.get('shares') or 0)
        pr = prices.get(tk)
        if pr:
            total += sh * pr
    return round(total, 2)


def _generate_trades(targets: dict[str, dict[str, Any]], positions: dict[str, Any], min_trade_value: float = 0.0, market_budgets: dict[str, float] | None = None, min_turnover_ratio: float = 0.0, allowed_markets: set[str] | None = None, price_overrides: dict[str, float] | None = None) -> dict[str, Any]:
    # build price map for union of tickers
    allowed_markets = allowed_markets or {'CN','HK','US'}
    tickers = set(list(targets.keys()) + [p.get('ticker') for p in positions.get('positions', [])])
    tickers = {tk for tk in tickers if tk and _get_market(tk) in allowed_markets}
    prices: dict[str, float] = {}
    for tk in tickers:
        override = (price_overrides or {}).get(tk)
        if override is not None:
            prices[tk] = float(override)
            continue
        # Use enhanced price fetching with multiple data sources
        price = _fetch_enhanced_price(tk, args.alpha_vantage_key if 'args' in locals() else None)
        if price:
            prices[tk] = price
        else:
            print(f"Warning: No price found for {tk} from any data source")
            prices[tk] = 0.0
    # compute invested per market
    positions_list = [p for p in positions.get('positions', []) if p.get('ticker') in tickers]
    invested_by_market = {'CN': 0.0, 'HK': 0.0, 'US': 0.0}
    for p in positions_list:
        tk = p.get('ticker')
        sh = int(p.get('shares') or 0)
        pr = prices.get(tk) or 0.0
        invested_by_market[_get_market(tk)] += sh * pr
    # initialize cash per market
    cash_by_market = positions.get('cash_by_market', {}).copy() if isinstance(positions.get('cash_by_market'), dict) else {}
    if market_budgets:
        for m in ['CN','HK','US']:
            budget = float(market_budgets.get(m, 0.0) or 0.0)
            if budget > 0:
                cash_by_market[m] = max(0.0, round(budget - invested_by_market.get(m, 0.0), 2))
    # Cash-friendly target sizing: base target amounts on invested value only,
    # so we do not force deploying cash. Turnover enforcement will still operate.
    current_value = sum(invested_by_market.values())
    # if target_amounts missing, infer from pct and current portfolio value
    for tk, t in targets.items():
        if (t.get('target_amount') is None) or (t.get('target_amount') == 0.0):
            pct = float(t.get('allocation_pct') or 0.0)
            if pct and current_value:
                t['target_amount'] = round(current_value * (pct / 100.0), 2)
    # index current positions
    cur_map = {p.get('ticker'): int(p.get('shares') or 0) for p in positions.get('positions', []) if p.get('ticker')}
    trades = []
    new_positions_map = cur_map.copy()
    total_cash = float(positions.get('cash', 0.0) or 0.0)
    if not cash_by_market and total_cash:
        present_markets = sorted({_get_market(tk) for tk in tickers})
        if len(present_markets) == 1:
            cash_by_market[present_markets[0]] = total_cash
        elif len(present_markets) > 1:
            share = total_cash / len(present_markets)
            for m in present_markets:
                cash_by_market[m] = share
        total_cash = 0.0
    for tk in tickers:
        price = prices.get(tk) or 0.0
        if price <= 0:
            continue
        cur_sh = int(cur_map.get(tk, 0))
        cur_val = cur_sh * price
        tgt_amt = float(targets.get(tk, {}).get('target_amount') or 0.0)
        diff_amt = tgt_amt - cur_val
        if abs(diff_amt) < max(1.0, float(min_trade_value or 0.0)):
            continue
        qty = int(abs(diff_amt) // price)
        if qty <= 0:
            continue
        action = 'BUY' if diff_amt > 0 else 'SELL'
        mkt = _get_market(tk)
        if action == 'BUY':
            max_affordable = int((cash_by_market.get(mkt, 0.0) or 0.0) // price)
            qty = min(qty, max_affordable)
            if qty <= 0:
                continue
        trades.append({
            'datetime': datetime.datetime.now().isoformat(timespec='seconds'),
            'ticker': tk,
            'action': action,
            'shares': qty,
            'price': round(price, 4),
            'amount': round(qty * price * (1 if action == 'BUY' else -1), 2)
        })
        if action == 'BUY':
            new_positions_map[tk] = cur_sh + qty
            cash_by_market[mkt] = round((cash_by_market.get(mkt, 0.0) or 0.0) - qty * price, 2)
        else:
            qty = min(qty, cur_sh)
            new_positions_map[tk] = max(0, cur_sh - qty)
            cash_by_market[mkt] = round((cash_by_market.get(mkt, 0.0) or 0.0) + qty * price, 2)
            trades[-1]['shares'] = qty
            trades[-1]['amount'] = round(-qty * price, 2)

    # enforce min turnover per market
    if min_turnover_ratio and min_turnover_ratio > 0:
        def turnover_and_value(m: str):
            traded = sum(abs(t['amount']) for t in trades if _get_market(t['ticker']) == m)
            value = invested_by_market.get(m, 0.0) + (cash_by_market.get(m, 0.0) or 0.0)
            return traded, value
        while True:
            progress = False
            met_all = True
            for m in ['CN','HK','US']:
                traded, value = turnover_and_value(m)
                if value <= 0:
                    continue
                if traded + 1e-6 < min_turnover_ratio * value:
                    met_all = False
                    # choose best additional trade in this market
                    best = None
                    best_amt = 0.0
                    for tk in tickers:
                        if _get_market(tk) != m:
                            continue
                        price = prices.get(tk) or 0.0
                        if price <= 0:
                            continue
                        cur_sh = int(new_positions_map.get(tk, 0))
                        tgt_amt = float(targets.get(tk, {}).get('target_amount') or 0.0)
                        cur_val = cur_sh * price
                        diff_amt = tgt_amt - cur_val
                        if abs(diff_amt) < price:
                            continue
                        if diff_amt > 0:
                            max_aff = int((cash_by_market.get(m, 0.0) or 0.0) // price)
                            qty = max_aff
                            if qty <= 0:
                                continue
                            amt = qty * price
                            act = 'BUY'
                        else:
                            qty = min(int(abs(diff_amt) // price), cur_sh)
                            if qty <= 0:
                                continue
                            amt = qty * price
                            act = 'SELL'
                        if amt > best_amt:
                            best_amt = amt
                            best = (tk, act, qty, price)
                    if best:
                        tk, act, qty, price = best
                        trades.append({
                            'datetime': datetime.datetime.now().isoformat(timespec='seconds'),
                            'ticker': tk,
                            'action': act,
                            'shares': qty,
                            'price': round(price, 4),
                            'amount': round(qty * price * (1 if act == 'BUY' else -1), 2)
                        })
                        if act == 'BUY':
                            new_positions_map[tk] = new_positions_map.get(tk, 0) + qty
                            cash_by_market[m] = round((cash_by_market.get(m, 0.0) or 0.0) - qty * price, 2)
                        else:
                            new_positions_map[tk] = max(0, new_positions_map.get(tk, 0) - qty)
                            cash_by_market[m] = round((cash_by_market.get(m, 0.0) or 0.0) + qty * price, 2)
                        progress = True
            if met_all or not progress:
                break
    # build new positions list
    new_positions = [ {'ticker': tk, 'shares': sh} for tk, sh in new_positions_map.items() if tk ]
    combined_cash = sum(cash_by_market.values()) + total_cash
    new_value = _compute_portfolio_value(new_positions, prices, combined_cash)
    dist = []
    for p in new_positions:
        tk = p['ticker']
        sh = int(p['shares'] or 0)
        pr = prices.get(tk) or 0.0
        val = sh * pr
        pct = round((val / new_value) * 100.0, 2) if new_value > 0 else 0.0
        dist.append({'ticker': tk, 'shares': sh, 'price': round(pr,4), 'value': round(val,2), 'pct': pct})
    return {
        'prices': prices,
        'trades': trades,
        'new_positions': {
            'date': datetime.date.today().isoformat(),
            'cash_by_market': {k: round(v,2) for k,v in cash_by_market.items()},
            'positions': new_positions
        },
        'distribution': dist,
        'portfolio_value': new_value
    }


def _render_daily_trading_log(date_str: str, urls: list[str], targets: dict[str, dict[str, Any]], positions_before: dict[str, Any], trades_payload: dict[str, Any], rec_details: dict | None = None) -> str:
    lines = []
    lines.append(f"Daily Trading Log — {date_str}")
    lines.append("")
    # News summary (conclusions)
    lines.append("News Summary:")
    sectors = []
    suggestions = []
    if rec_details and isinstance(rec_details, dict):
        for sec in rec_details.get('sectors', []) or []:
            sectors.append({'sector': sec.get('sector'), 'pct': float(sec.get('sector_pct') or 0.0)})
            for s in sec.get('suggestions', []) or []:
                tk = s.get('ticker')
                suggestions.append({'sector': sec.get('sector'), 'name': s.get('name'), 'ticker': tk, 'pct': float(s.get('allocation_pct') or 0.0), 'role': s.get('role')})
    # Sector tilt
    if sectors:
        top_secs = sorted(sectors, key=lambda x: x['pct'], reverse=True)[:4]
        sec_str = ', '.join([f"{s['sector']}({s['pct']}%)" for s in top_secs if s.get('sector')])
        lines.append(f"- 板块权重倾向：重点关注 {sec_str}。")
    # Core/satellite focus and broader tickers
    if suggestions:
        cores = [s for s in suggestions if s.get('role') == 'core' and s.get('ticker')]
        sats = [s for s in suggestions if s.get('role') == 'satellite' and s.get('ticker')]
        top_tickers = sorted([s for s in suggestions if s.get('ticker')], key=lambda x: x['pct'], reverse=True)[:10]
        core_str = ', '.join([f"{c['ticker']}({c['pct']}%)" for c in cores[:6]]) or '（无已映射核心标的）'
        sat_str = ', '.join([f"{t['ticker']}({t['pct']}%)" for t in sats[:8]]) or '（无已映射卫星标的）'
        top_str = ', '.join([f"{t['ticker']}({t['pct']}%)" for t in top_tickers])
        lines.append(f"- 核心配置建议：{core_str}；卫星配置建议：{sat_str}。")
        lines.append(f"- 综合关注标的（不局限于少数股票）：{top_str}。")
    else:
        keys = sorted(list(targets.keys()))
        lines.append(f"- 综合关注标的：{', '.join(keys[:10])}。")
    lines.append("")
    # Position analysis
    lines.append("Position Analysis:")
    prices = trades_payload.get('prices', {})
    pos_list = positions_before.get('positions', []) or []
    # current value and target per ticker
    current_vals = {}
    total_current = 0.0
    for p in pos_list:
        tk = p.get('ticker')
        sh = int(p.get('shares') or 0)
        pr = prices.get(tk) or 0.0
        val = sh * pr
        current_vals[tk] = val
        total_current += val
    # analyze
    for tk in sorted(set(list(targets.keys()) + list(current_vals.keys()))):
        cur = current_vals.get(tk, 0.0)
        tgt = float(targets.get(tk, {}).get('target_amount') or 0.0)
        pr = prices.get(tk) or 0.0
        diff = tgt - cur
        status = 'neutral'
        if diff > 0:
            status = 'underweight'
        elif diff < 0:
            status = 'overweight'
        action = 'BUY' if diff > 0 else ('SELL' if diff < 0 else 'HOLD')
        # Skip clutter: do not list HOLD entries
        if action == 'HOLD':
            continue
        cur_pct = round((cur / total_current) * 100.0, 2) if total_current > 0 else 0.0
        tgt_pct = None
        # if total target known, approximate pct
        total_tgt = sum(float(t.get('target_amount') or 0.0) for t in targets.values())
        tgt_pct = round((tgt / total_tgt) * 100.0, 2) if total_tgt > 0 else None
        lines.append(f"- {tk}: cur={round(cur,2)} ({cur_pct}%), tgt={round(tgt,2)}{(' ('+str(tgt_pct)+'%)') if tgt_pct is not None else ''}, status={status}, suggested={action}")
    lines.append("")
    # Trading plan
    lines.append("Today's Trading Plan:")
    trades = trades_payload.get('trades', [])
    if not trades:
        lines.append("- No trades generated (targets match current holdings or price unavailable).")
    else:
        for t in trades:
            lines.append(f"- {t.get('action')} {t.get('ticker')} {t.get('shares')} @ {t.get('price')}")
    lines.append("")
    # References
    lines.append("References:")
    if urls:
        for u in urls:
            lines.append(f"- {u}")
    else:
        lines.append("- (none)")
    return "\n".join(lines)


def _save_trades(output_dir: Path, trades_payload: dict[str, Any]):
    out_dir = output_dir / 'trades'
    out_dir.mkdir(parents=True, exist_ok=True)
    when = datetime.date.today().isoformat()
    csv_path = out_dir / f"trades_{when}.csv"
    json_path = out_dir / f"trades_{when}.json"
    try:
        # write CSV
        import csv as _csv
        with csv_path.open('w', encoding='utf-8-sig', newline='') as cf:
            writer = _csv.writer(cf)
            writer.writerow(['datetime','ticker','action','shares','price','amount'])
            for t in trades_payload.get('trades', []):
                writer.writerow([t.get('datetime'), t.get('ticker'), t.get('action'), t.get('shares'), t.get('price'), t.get('amount')])
        # write JSON full payload
        with json_path.open('w', encoding='utf-8') as jf:
            json.dump(trades_payload, jf, ensure_ascii=False, indent=2)
    except Exception:
        pass


def aggregate_entities(results: list[dict]) -> dict:
    agg = defaultdict(lambda: {'scores': [], 'pos': 0, 'neg': 0, 'neutral': 0, 'count': 0, 'tickers': set(), 'names': set()})
    for res in results:
        ents = res.get('entities', {})
        for ent, data in ents.items():
            cls = data.get('class', 'neutral')
            score = float(data.get('score', 0.0) or 0.0)
            ticker = data.get('ticker')
            name = data.get('company') or ent
            # normalize ticker/name into a canonical key
            def _normalize_key(name_in: str, ticker_in: str | None):
                tk = (ticker_in or '').strip()
                # normalize ticker: uppercase, remove dots
                if tk:
                    tk = tk.upper().replace('.', '')
                # if name looks like a ticker (short alnum), prefer that
                nm = name_in.strip()
                nm_clean = nm.strip(string.punctuation + "\n\r\t ")
                # decide key
                if tk:
                    return tk
                # if name is short alnum token (<=5) and mostly ascii, treat as ticker
                if re.fullmatch(r"[A-Za-z0-9]{1,6}", nm_clean):
                    return nm_clean.upper()
                # otherwise use cleaned company name
                return nm_clean

            key = _normalize_key(name, ticker)
            entry = agg[key]
            entry['scores'].append(score)
            entry['count'] += 1
            # collect tickers and observed names
            if ticker:
                entry['tickers'].update([t.strip().upper().replace('.', '') for t in str(ticker).split() if t.strip()])
            entry['names'].add(name)
            if cls == 'positive':
                entry['pos'] += 1
            elif cls == 'negative':
                entry['neg'] += 1
            else:
                entry['neutral'] += 1
    # finalize
    out = {}
    # stopwords / tokens to ignore (english and chinese common junk)
    STOPWORDS = set(x.lower() for x in [
        # english/common short tokens
        'ai', 'app', 'qq', 'report', 'company', 'companies', 'market', 'markets', 'product', 'products', 'quarter', 'q1', 'q2', 'q3', 'q4',
        'analyst', 'analysts', 'press', 'press release', 'press-release', 'announcement', 'statement', 'investment', 'investors', 'earnings', 'revenue', 'sales', 'price', 'share', 'shares', 'stock', 'stocks',
        # chinese/common tokens
        '的', '公司', '报告', '市场', '发展', '增长', '产品', '服务', '投资', '美元', '中国', '美国', '用户', '新闻', '公告', '研报', '分析师', '营收', '利润', '股价', '市值', '涨幅', '跌幅'
    ])
    # common media/site names to exclude from entity lists (lowercase)
    MEDIA_NAMES = set(x.lower() for x in [
        'sina', 'sina.com', 'sina.com.cn', 'wallstreetcn', 'wallstreetcn.com', 'caixin', 'wsj', 'reuters', 'bloomberg', 'ft', 'financial times', 'cnstock', 'netease', 'sohu', 'ifeng', 'yahoo', 'yahoo.com', 'xinhua', 'cnbc', 'weibo', 'twitter', 'wechat', 'wechat.com'
    ])

    def contains_chinese(s: str) -> bool:
        return any(ord(ch) > 127 for ch in s)

    for k, v in agg.items():
        avg = mean(v['scores']) if v['scores'] else 0.0
        key_l = k.lower().strip()
        # filter out obvious year tokens
        if re.fullmatch(r"\d{4}", k) and 1900 <= int(k) <= 2100:
            continue
        # filter pure numeric tokens
        if re.fullmatch(r"\d+", k):
            continue
        # filter stopwords and media/site names
        if key_l in STOPWORDS:
            continue
        if key_l in MEDIA_NAMES:
            continue
        # also filter if any observed name is a known media name
        names_lower = [n.lower() for n in v.get('names', []) if isinstance(n, str)]
        if any((nm in MEDIA_NAMES) or any(m in nm for m in MEDIA_NAMES) for nm in names_lower):
            continue
        # if Chinese token, require length >=2 (single-char chinese is noisy)
        if contains_chinese(k) and len(k.strip()) < 2:
            continue
        # require observed tickers; drop entities without observed ticker
        has_tickers = bool(v.get('tickers'))
        if not has_tickers:
            continue
        # drop very short ascii tokens that are unlikely to be tickers
        if not contains_chinese(k):
            clean_k = re.sub(r"[^A-Za-z0-9]", '', k)
            if len(clean_k) <= 1 and not has_tickers:
                continue

        out[k] = {
            'avg_score': round(avg, 4),
            'pos': v['pos'],
            'neg': v['neg'],
            'neutral': v['neutral'],
            'count': v['count'],
            'tickers': sorted(list(v['tickers'])),
            'names': sorted(list(v['names']))
        }
    return out


def main():
    parser = argparse.ArgumentParser(description='Integrate industry analysis with HKSI company analyzer')
    parser.add_argument('--sector', '-s', help='Sector name (e.g. technology). If omitted, picks top sector from output/sector_summary.json')
    parser.add_argument('--top', '-n', type=int, default=10, help='Top N company suggestions to print')
    parser.add_argument('--ticker-db', help='Optional ticker DB (CSV or JSON) path to pass to HKSI')
    parser.add_argument('--install-deps', action='store_true', help='Automatically install missing HKSI dependencies if needed')
    parser.add_argument('--max-articles', type=int, default=20, help='Max number of articles to process from sector file')
    parser.add_argument('--report-csv', action='store_true', help='Write a CSV summary beside the JSON ranking')
    parser.add_argument('--rerun-failed', action='store_true', help='Process previously failed URLs from output/failed_urls.json')
    parser.add_argument('--portfolio-size', type=float, default=0.0, help='Total portfolio size in your currency to compute dollar allocations (optional)')
    parser.add_argument('--strategy', choices=['simple','conviction-weighted'], default='simple', help='Allocation strategy')
    parser.add_argument('--top-per-sector', type=int, default=3, help='Number of top companies per sector to consider for allocations')
    parser.add_argument('--volatility-adjust', action='store_true', help='Adjust allocations inversely by historical volatility (per sector)')
    parser.add_argument('--vol-window', type=int, default=90, help='Days of history to use when computing volatility')
    parser.add_argument('--trade', action='store_true', help='Generate buy/sell actions to rebalance to today\'s suggested portfolio')
    parser.add_argument('--positions-file', type=str, default='output/positions.json', help='Path to current positions JSON')
    parser.add_argument('--min-trade-value', type=float, default=0.0, help='Minimum trade notional to include (filters tiny trades)')
    parser.add_argument('--budget-cn', type=float, default=1000000.0, help='Budget for A-shares (CNY)')
    parser.add_argument('--budget-hk', type=float, default=1000000.0, help='Budget for Hong Kong stocks (HKD)')
    parser.add_argument('--budget-us', type=float, default=1000000.0, help='Budget for US stocks (USD)')
    parser.add_argument('--min-turnover', type=float, default=0.10, help='Minimum turnover ratio per market (e.g., 0.10 for 10%)')
    parser.add_argument('--price-file', type=str, default='', help='Optional JSON file mapping ticker->price for offline pricing')
    parser.add_argument('--alpha-vantage-key', type=str, default='', help='Alpha Vantage API key for enhanced US stock price data (free tier: 500 requests/day)')
    parser.add_argument('--refresh-all', action='store_true', default=True, help='Refresh company rankings for all sectors using latest sector files (filters out Sina; prefers EastMoney/WSCN)')
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    output_dir = root / 'output'
    summary_path = output_dir / 'sector_summary.json'

    sector = pick_sector(args.sector, summary_path)
    print(f"Selected sector: {sector}")

    # optional: refresh rankings for all sectors using latest files and source filtering
    # This helps avoid stale rankings dominated by noisy sources.
    if getattr(args, 'refresh_all', False):
        sec_path = output_dir / 'sector_allocations.csv'
        all_secs = list(_read_sector_allocations(sec_path).keys())
        print(f"Refreshing company rankings for all sectors: {', '.join(all_secs)}")
        SEC_FILE_ALIAS = {
            'real': 'real_estate',
            'real estate': 'real_estate',
        }
        for sec_name in all_secs:
            sf = find_latest_sector_file(output_dir, sec_name) or find_latest_sector_file(output_dir, sec_name.replace(' ', '_'))
            if not sf:
                print(f"  Skip: no sector file for {sec_name}")
                continue
            urls0 = parse_urls_from_sector_file(sf)
            # Filter sources: prefer EastMoney and WallstreetCN; exclude Sina
            allow_domains = ("eastmoney.com", "wallstreetcn.com")
            block_domains = ("sina.com.cn", "sina.cn")
            filtered = []
            for u in urls0:
                uu = u.lower()
                if any(b in uu for b in block_domains):
                    continue
                if any(a in uu for a in allow_domains):
                    filtered.append(u)
            if not filtered:
                filtered = [u for u in urls0 if not re.search(r"sina\.(com\.cn|cn)", u, flags=re.I)]
            urls_sec = filtered[:args.max_articles]
            print(f"  {sec_name}: {len(urls_sec)} URLs after filtering.")
            # Process URLs via HKSI
            results_sec = []
            failed_sec = []
            for url in urls_sec:
                attempt = 0
                max_attempts = 3
                last_exc = None
                while attempt < max_attempts:
                    attempt += 1
                    try:
                        print(f"    ({attempt}/{max_attempts}) {url}")
                        try:
                            res = hksi.process_url(url, n_sentences=3, ticker_db=ticker_db)
                        except TypeError:
                            res = hksi.process_url(url, 3, ticker_db)
                        results_sec.append(res)
                        last_exc = None
                        break
                    except Exception as e:
                        last_exc = e
                        time.sleep(1 * attempt)
                if last_exc:
                    failed_sec.append({'url': url, 'error': str(last_exc), 'time': datetime.datetime.utcnow().isoformat()})
            agg_sec = aggregate_entities(results_sec)
            # drop entries without observed tickers
            try:
                agg_sec = {k: v for k, v in agg_sec.items() if v.get('tickers') and len(v.get('tickers')) > 0}
            except Exception:
                pass
            ranked_sec = sorted(agg_sec.items(), key=lambda kv: (kv[1]['avg_score'], kv[1]['pos']), reverse=True)
            out_path_sec = output_dir / f'company_rank_{sec_name.replace(" ", "_")}.json'
            with out_path_sec.open('w', encoding='utf-8') as f:
                json.dump({'sector': sec_name, 'ranked': ranked_sec}, f, ensure_ascii=False, indent=2)
            if args.report_csv:
                csv_path_sec = output_dir / f'company_rank_{sec_name.replace(" ", "_")}.csv'
                with csv_path_sec.open('w', encoding='utf-8-sig', newline='') as cf:
                    writer = csv.writer(cf)
                    writer.writerow(['rank', 'key', 'avg_score', 'pos', 'neg', 'neutral', 'count', 'tickers', 'names'])
                    for i, (key, info) in enumerate(ranked_sec, start=1):
                        writer.writerow([i, key, info.get('avg_score'), info.get('pos'), info.get('neg'), info.get('neutral'), info.get('count'), '|'.join(info.get('tickers') or []), '|'.join(info.get('names') or [])])
        # (disabled here; refresh happens after HKSI is loaded)

    # find latest sector file for selected sector
    sector_file = find_latest_sector_file(output_dir, sector)
    if not sector_file:
        # try alternative: filenames with spaces replaced
        sector_file = find_latest_sector_file(output_dir, sector.replace(' ', '_'))
    if not sector_file:
        raise FileNotFoundError(f"No sector text file found for '{sector}' in {output_dir}")
    print(f"Using sector file: {sector_file}")

    if args.rerun_failed:
        urls = load_failed_urls(output_dir)
        if not urls:
            print('No failed URLs to re-run.')
            return
        print(f"Re-running {len(urls)} failed URLs.")
    else:
        urls = parse_urls_from_sector_file(sector_file)
        if not urls:
            raise ValueError(f"No URLs found in {sector_file}")
        # Filter sources: prefer EastMoney and WallstreetCN; exclude Sina
        allow_domains = ("eastmoney.com", "wallstreetcn.com", "yicai.com", "thepaper.cn", "caixin.com")
        block_domains = ("sina.com.cn", "sina.cn")
        filtered = []
        removed = 0
        for u in urls:
            uu = u.lower()
            if any(b in uu for b in block_domains):
                removed += 1
                continue
            if any(a in uu for a in allow_domains):
                filtered.append(u)
        # If filtering removed all, fall back to original non-Sina URLs
        if not filtered:
            filtered = [u for u in urls if not re.search(r"sina\.(com\.cn|cn)", u, flags=re.I)]
        urls = filtered[:args.max_articles]
        print(f"Found {len(urls)} article URLs after source filtering (removed {removed}).")

    # Load HKSI
    hksi_base = root / 'HKSI-main' / 'HKSI-main'
    hksi = load_hksi_module(hksi_base)

    # optionally install missing deps (guard if HKSI module doesn't expose helpers)
    missing = []
    if hasattr(hksi, 'check_missing_dependencies'):
        try:
            missing = hksi.check_missing_dependencies() or []
        except Exception as e:
            print(f"Warning: check_missing_dependencies failed: {e}")
    else:
        print("HKSI module has no check_missing_dependencies; skipping dependency check.")

    if missing and args.install_deps and hasattr(hksi, 'install_missing_packages'):
        try:
            ok = hksi.install_missing_packages(missing)
            if not ok:
                print('Failed to install HKSI dependencies; proceeding may fail.')
        except Exception as e:
            print(f"Warning: install_missing_packages failed: {e}")

    if hasattr(hksi, 'import_dependencies'):
        try:
            hksi.import_dependencies()
        except Exception as e:
            print(f"Warning: failed to import HKSI dependencies: {e}")
    else:
        print("HKSI module has no import_dependencies; proceeding (may fail at runtime).")

    # optional ticker DB
    ticker_db = None
    if args.ticker_db:
        try:
            ticker_db = hksi.load_ticker_db(args.ticker_db)
        except Exception as e:
            print(f"Failed to load ticker db: {e}")
    # load optional aliases (Chinese names -> ticker)
    alias_db = {}
    alias_path = root / 'ticker_aliases.json'
    if alias_path.exists():
        try:
            with alias_path.open('r', encoding='utf-8') as af:
                alias_db = json.load(af)
        except Exception:
            alias_db = {}
    # build reverse lookup for fuzzy matching
    reverse_names = {}
    all_company_names = []
    if ticker_db:
        for tk, cname in ticker_db.items():
            if cname:
                reverse_names[cname.lower()] = tk
                all_company_names.append(cname)

    # Refresh rankings for all sectors (now that HKSI and DBs are loaded)
    if getattr(args, 'refresh_all', False):
        sec_path = output_dir / 'sector_allocations.csv'
        all_secs = list(_read_sector_allocations(sec_path).keys())
        print(f"Refreshing company rankings for all sectors: {', '.join(all_secs)}")
        SEC_FILE_ALIAS = {
            'real': 'real_estate',
            'real estate': 'real_estate',
        }
        for sec_name in all_secs:
            sf = find_latest_sector_file(output_dir, sec_name) or find_latest_sector_file(output_dir, sec_name.replace(' ', '_'))
            if not sf:
                print(f"  Skip: no sector file for {sec_name}")
                continue
            urls0 = parse_urls_from_sector_file(sf)
            allow_domains = ("eastmoney.com", "wallstreetcn.com", "yicai.com", "thepaper.cn", "caixin.com")
            block_domains = ("sina.com.cn", "sina.cn")
            filtered = []
            for u in urls0:
                uu = u.lower()
                if any(b in uu for b in block_domains):
                    continue
                if any(a in uu for a in allow_domains):
                    filtered.append(u)
            if not filtered:
                filtered = [u for u in urls0 if not re.search(r"sina\.(com\.cn|cn)", u, flags=re.I)]
            urls_sec = filtered[:args.max_articles]
            print(f"  {sec_name}: {len(urls_sec)} URLs after filtering.")
            results_sec = []
            failed_sec = []
            for url in urls_sec:
                attempt = 0
                max_attempts = 3
                last_exc = None
                while attempt < max_attempts:
                    attempt += 1
                    try:
                        print(f"    ({attempt}/{max_attempts}) {url}")
                        try:
                            res = hksi.process_url(url, n_sentences=3, ticker_db=ticker_db)
                        except TypeError:
                            res = hksi.process_url(url, 3, ticker_db)
                        results_sec.append(res)
                        last_exc = None
                        break
                    except Exception as e:
                        last_exc = e
                        time.sleep(1 * attempt)
                if last_exc:
                    failed_sec.append({'url': url, 'error': str(last_exc), 'time': datetime.datetime.utcnow().isoformat()})
            agg_sec = aggregate_entities(results_sec)
            try:
                agg_sec = {k: v for k, v in agg_sec.items() if v.get('tickers') and len(v.get('tickers')) > 0}
            except Exception:
                pass
            ranked_sec = sorted(agg_sec.items(), key=lambda kv: (kv[1]['avg_score'], kv[1]['pos']), reverse=True)
            file_key = SEC_FILE_ALIAS.get(sec_name.lower(), sec_name).replace(' ', '_')
            out_path_sec = output_dir / f"company_rank_{file_key}.json"
            with out_path_sec.open('w', encoding='utf-8') as f:
                json.dump({'sector': sec_name, 'ranked': ranked_sec}, f, ensure_ascii=False, indent=2)
            if args.report_csv:
                try:
                    csv_path_sec = output_dir / f"company_rank_{file_key}.csv"
                    with csv_path_sec.open('w', encoding='utf-8-sig', newline='') as cf:
                        writer = csv.writer(cf)
                        writer.writerow(['rank', 'key', 'avg_score', 'pos', 'neg', 'neutral', 'count', 'tickers', 'names'])
                        for i, (key, info) in enumerate(ranked_sec, start=1):
                            writer.writerow([i, key, info.get('avg_score'), info.get('pos'), info.get('neg'), info.get('neutral'), info.get('count'), '|'.join(info.get('tickers') or []), '|'.join(info.get('names') or [])])
                except Exception as e:
                    print(f"Warning: failed to write CSV for {sec_name}: {e}")

    results = []
    failed = []
    for url in urls:
        attempt = 0
        max_attempts = 3
        last_exc = None
        while attempt < max_attempts:
            attempt += 1
            try:
                print(f"Processing ({attempt}/{max_attempts}): {url}")
                # call HKSI.process_url; guard if signature differs
                res = None
                try:
                    res = hksi.process_url(url, n_sentences=3, ticker_db=ticker_db)
                except TypeError:
                    # fallback if keyword args not supported
                    res = hksi.process_url(url, 3, ticker_db)
                results.append(res)
                last_exc = None
                break
            except Exception as e:
                tb = traceback.format_exc()
                print(f"Attempt {attempt} failed for {url}: {e}")
                print(tb)
                last_exc = e
                # short backoff
                time.sleep(1 * attempt)
        if last_exc:
            print(f"Failed to process {url} after {max_attempts} attempts: {last_exc}")
            failed.append({'url': url, 'error': str(last_exc), 'traceback': traceback.format_exc(), 'time': datetime.datetime.utcnow().isoformat()})

    agg = aggregate_entities(results)
    # merge with any existing per-ticker aggregates (e.g., produced by run_watchlist.py)
    existing_path = output_dir / f'company_rank_{sector.replace(" ", "_")}.json'
    if existing_path.exists():
        try:
            with existing_path.open('r', encoding='utf-8') as ef:
                existing = json.load(ef)
            existing_ranked = existing.get('ranked') if isinstance(existing, dict) else existing
            # existing_ranked expected as list of [key, info]
            if isinstance(existing_ranked, list):
                for item in existing_ranked:
                    if isinstance(item, list) and len(item) >= 2:
                        key, info = item[0], item[1]
                    elif isinstance(item, dict):
                        # older format: {key: info}
                        for k, v in item.items():
                            key, info = k, v
                    else:
                        continue
                    # merge into agg: average avg_score, sum pos/neg/neutral/count; merge tickers/names
                    if key in agg:
                        a = agg[key]
                        # combine avg_score by weighted mean using counts
                        try:
                            existing_avg = float(info.get('avg_score', 0.0) or 0.0)
                            existing_count = int(info.get('count', 0) or 0)
                        except Exception:
                            existing_avg = 0.0
                            existing_count = 0
                        a_count = a.get('count', 0) or 0
                        a_avg = a.get('avg_score', 0.0) or 0.0
                        total_count = a_count + existing_count if (a_count + existing_count) > 0 else 1
                        combined_avg = ((a_avg * a_count) + (existing_avg * existing_count)) / total_count
                        a['avg_score'] = round(combined_avg, 4)
                        a['pos'] = (a.get('pos', 0) or 0) + (info.get('pos', 0) or 0)
                        a['neg'] = (a.get('neg', 0) or 0) + (info.get('neg', 0) or 0)
                        a['neutral'] = (a.get('neutral', 0) or 0) + (info.get('neutral', 0) or 0)
                        a['count'] = total_count
                        # merge tickers/names
                        ticks = set(a.get('tickers', [])) | set(info.get('tickers', []))
                        names = set(a.get('names', [])) | set(info.get('names', []))
                        a['tickers'] = sorted(list(ticks))
                        a['names'] = sorted(list(names))
                        agg[key] = a
                    else:
                        # adopt existing entry
                        agg[key] = info
        except Exception:
            pass

    # After merging with existing per-ticker aggregates, drop any entries without observed tickers
    try:
        agg = {k: v for k, v in agg.items() if v.get('tickers') and len(v.get('tickers')) > 0}
    except Exception:
        pass

    if not agg:
        print('No entities extracted from articles; will use existing rankings for recommendations.')
        # save failures if any
        save_failed_urls(failed, output_dir)
        ranked = []
    else:
        # Rank by avg_score then by positive count
        ranked = sorted(agg.items(), key=lambda kv: (kv[1]['avg_score'], kv[1]['pos']), reverse=True)

    # Save new ranking only when we have fresh entities
    if ranked:
        out_path = output_dir / f'company_rank_{sector.replace(" ", "_")}.json'
        with out_path.open('w', encoding='utf-8') as f:
            json.dump({'sector': sector, 'ranked': ranked}, f, ensure_ascii=False, indent=2)

        # write CSV if requested
        if args.report_csv:
            csv_path = output_dir / f'company_rank_{sector.replace(" ", "_")}.csv'
            with csv_path.open('w', encoding='utf-8-sig', newline='') as cf:
                writer = csv.writer(cf)
                writer.writerow(['rank', 'key', 'avg_score', 'pos', 'neg', 'neutral', 'count', 'tickers', 'names'])
                for i, (key, info) in enumerate(ranked, start=1):
                    writer.writerow([i, key, info.get('avg_score'), info.get('pos'), info.get('neg'), info.get('neutral'), info.get('count'), '|'.join(info.get('tickers') or []), '|'.join(info.get('names') or [])])

    # save failed URLs
    save_failed_urls(failed, output_dir)

    # by default generate recommendation report unless disabled
    if not getattr(args, 'no_report', False):
        try:
            # Load ticker->sector map if available
            ticker_sectors = {}
            ts_path = root / 'ticker_sectors.json'
            if ts_path.exists():
                try:
                    with ts_path.open('r', encoding='utf-8') as tsf:
                        ticker_sectors = json.load(tsf)
                except Exception:
                    ticker_sectors = {}
            rec = generate_recommendation_report(output_dir, ticker_db=ticker_db, portfolio_size=args.portfolio_size, strategy=args.strategy, top_per_sector=args.top_per_sector, alias_db=alias_db, ticker_sectors=ticker_sectors)
            # optionally adjust by volatility
            if getattr(args, 'volatility_adjust', False):
                details = rec.get('details', {})
                details = adjust_allocations_by_volatility(details, args.portfolio_size, vol_window=getattr(args, 'vol_window', 90))
                text = render_details_to_text(details)
                rec = {'text': text, 'details': details}
            when = datetime.date.today().isoformat()
            txt_path = output_dir / f'recommendation_{when}.txt'
            json_path = output_dir / f'recommendation_{when}.json'
            with txt_path.open('w', encoding='utf-8') as tf:
                tf.write(rec['text'])
            with json_path.open('w', encoding='utf-8') as jf:
                json.dump(rec, jf, ensure_ascii=False, indent=2)
            print(f"Saved recommendation report: {txt_path} and {json_path}")

            if getattr(args, 'trade', False):
                details = rec.get('details', {})
                positions_path = Path(args.positions_file) if os.path.isabs(args.positions_file) else (root / args.positions_file)
                current_positions = _load_positions(positions_path)
                market_budgets = {
                    'CN': float(getattr(args, 'budget_cn', 0.0) or 0.0),
                    'HK': float(getattr(args, 'budget_hk', 0.0) or 0.0),
                    'US': float(getattr(args, 'budget_us', 0.0) or 0.0)
                }
                # Build targets with per-market normalization and allowed markets only
                targets = _build_targets_from_details(details, args.portfolio_size or None, market_budgets=market_budgets, allowed_markets={'CN','HK','US'})
                # load optional price overrides
                price_overrides = {}
                if getattr(args, 'price_file', ''):
                    try:
                        p = Path(args.price_file) if os.path.isabs(args.price_file) else (root / args.price_file)
                        if p.exists():
                            with p.open('r', encoding='utf-8') as pf:
                                price_overrides = json.load(pf)
                    except Exception:
                        price_overrides = {}
                trades_payload = _generate_trades(
                    targets,
                    current_positions,
                    min_trade_value=getattr(args, 'min_trade_value', 0.0),
                    market_budgets=market_budgets,
                    min_turnover_ratio=float(getattr(args, 'min_turnover', 0.0) or 0.0),
                    allowed_markets={'CN','HK','US'},
                    price_overrides=price_overrides
                )
                _save_trades(output_dir, trades_payload)
                # save new positions snapshot and update positions file
                snapshot_path = output_dir / f"positions_{when}.json"
                _save_positions(snapshot_path, trades_payload.get('new_positions', {}))
                _save_positions(positions_path, trades_payload.get('new_positions', {}))
                # save distribution CSV
                dist_csv = output_dir / f"positions_distribution_{when}.csv"
                try:
                    import csv as _csv
                    with dist_csv.open('w', encoding='utf-8-sig', newline='') as cf:
                        w = _csv.writer(cf)
                        w.writerow(['ticker','shares','price','value','pct'])
                        for d in trades_payload.get('distribution', []):
                            w.writerow([d.get('ticker'), d.get('shares'), d.get('price'), d.get('value'), d.get('pct')])
                except Exception:
                    pass
                # save daily trading log
                logs_dir = output_dir / 'daily_logs'
                logs_dir.mkdir(parents=True, exist_ok=True)
                log_text = _render_daily_trading_log(when, urls, targets, current_positions, trades_payload, rec.get('details', {}))
                log_path = logs_dir / f'log_{when}.txt'
                try:
                    with log_path.open('w', encoding='utf-8') as lf:
                        lf.write(log_text)
                    print(f"Saved daily trading log: {log_path}")
                except Exception:
                    print("Warning: failed to save daily trading log.")
                print(f"Saved trades: {output_dir / ('trades/trades_' + when + '.csv')} and updated positions: {positions_path}")
        except Exception as e:
            print(f"Failed to generate recommendation report: {e}")
            print(traceback.format_exc())

    if ranked:
        print(f"Saved company ranking to: {out_path}")
        print('\nTop candidates:')
        for i, (name, info) in enumerate(ranked[:args.top], start=1):
            ticker_str = ','.join(info.get('tickers') or [])
            print(f"{i}. {name}  avg_score={info['avg_score']}  pos={info['pos']}  neg={info['neg']}  count={info['count']}  tickers=[{ticker_str}]")
    else:
        print('No new company ranking saved; existing rankings were used for recommendations.')


if __name__ == '__main__':
    main()
