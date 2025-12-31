"""Site-specific connectors for prioritized news sites.

We focus on WallstreetCN (华尔街见闻) and Eastmoney (东方财富网), and keep
lightweight connectors for 163/Caixin/CICC. Sina is supported but can be
excluded at the caller level.

These connectors use simple HTML parsing to extract recent article links
without JS rendering and are compatible with BeautifulSoup-based extractors.
"""
from __future__ import annotations

import re
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup


def _fetch_links(url: str, allowed_domain: str, path_pattern: str = r".+") -> List[str]:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    links = []
    pat = re.compile(path_pattern)
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("//"):
            href = "https:" + href
        if href.startswith("/"):
            href = requests.compat.urljoin(url, href)
        if allowed_domain not in href:
            continue
        if not pat.search(href):
            continue
        links.append(href)
    # dedupe while preserving order
    seen = set()
    out = []
    for l in links:
        if l in seen:
            continue
        seen.add(l)
        out.append(l)
    return out


def get_sina_urls(count: int = 20) -> List[Tuple[str, str]]:
    """Return a list of (topic, url) tuples from Sina News homepage.

    Uses simple heuristics: looks for `.shtml` article pages on `news.sina.com.cn`.
    """
    url = "https://news.sina.com.cn/"
    # Sina article pages often end with .shtml and contain /chn/ or /c/ patterns
    links = _fetch_links(url, allowed_domain="sina.com.cn", path_pattern=r"\.shtml$")
    results = []
    for l in links[:count]:
        # topic: first path segment after domain
        try:
            p = requests.utils.urlparse(l).path
            segs = [s for s in p.split("/") if s]
            topic = segs[0] if segs else "sina"
        except Exception:
            topic = "sina"
        results.append((topic, l))
    return results


def get_163_urls(count: int = 20) -> List[Tuple[str, str]]:
    """Return a list of (topic, url) tuples from NetEase (163) News homepage.

    NetEase article links are often under `news.163.com` and end with `.html`.
    """
    url = "https://news.163.com/"
    links = _fetch_links(url, allowed_domain="163.com", path_pattern=r"\.html$")
    results = []
    for l in links[:count]:
        try:
            p = requests.utils.urlparse(l).path
            segs = [s for s in p.split("/") if s]
            topic = segs[0] if segs else "163"
        except Exception:
            topic = "163"
        results.append((topic, l))
    return results


def get_caixin_urls(count: int = 20) -> List[Tuple[str, str]]:
    """Fetch recent article links from Caixin (caixin.com).

    Caixin pages may be under china.caixin.com or www.caixin.com; article pages
    often end with `.html` and include `/2025/` in the path. Use a broad
    HTML link extraction and filter by domain.
    """
    # Use the headlines landing page which provides recent articles
    url = "https://www.caixin.com/headlines/"
    links = _fetch_links(url, allowed_domain="caixin.com", path_pattern=r"\.html$")
    results = []
    for l in links[:count]:
        try:
            p = requests.utils.urlparse(l).path
            segs = [s for s in p.split("/") if s]
            topic = segs[0] if segs else "caixin"
        except Exception:
            topic = "caixin"
        results.append((topic, l))
    return results


def get_wallstreetcn_urls(count: int = 20) -> List[Tuple[str, str]]:
    """Fetch recent article links from WallstreetCN (华尔街见闻).

    The site uses `wallstreetcn.com`; articles often contain `/articles/` or `.html`.
    We aggregate multiple sections to improve coverage.
    """
    # Aggregate core sections
    url_candidates = [
        "https://wallstreetcn.com/news",
        "https://wallstreetcn.com/",
        "https://wallstreetcn.com/markets",
        "https://wallstreetcn.com/finance",
        "https://wallstreetcn.com/global",
    ]
    links: List[str] = []
    for u in url_candidates:
        try:
            crawled = _fetch_links(u, allowed_domain="wallstreetcn.com", path_pattern=r"(articles|\.html)")
            links.extend(crawled)
        except Exception:
            continue
    # de-duplicate while preserving order
    seen = set()
    uniq: List[str] = []
    for l in links:
        if l in seen:
            continue
        seen.add(l)
        uniq.append(l)
    links = uniq
    # Exclude member-only pages (paths containing '/member/') which require login/subscription
    def _is_public_article(link: str) -> bool:
        try:
            p = requests.utils.urlparse(link).path or ""
            if "/member/" in p or p.startswith("/member"):
                return False
        except Exception:
            return True
        return True
    links = [l for l in links if _is_public_article(l)]
    results = []
    for l in links[:count]:
        try:
            p = requests.utils.urlparse(l).path
            segs = [s for s in p.split("/") if s]
            topic = segs[0] if segs else "wallstreetcn"
        except Exception:
            topic = "wallstreetcn"
        results.append((topic, l))
    return results


def get_cicc_urls(count: int = 20) -> List[Tuple[str, str]]:
    """Fetch recent research/news links from CICC (cicc.com).

    CICC content can appear on `cicc.com` under research or news sections.
    We look for .html links on the domain.
    """
    # Use the CICC Global Institute market-tracking reports landing page
    url = "https://cgi.cicc.com/zh_CN/reports/market-tracking"
    # Report links often contain '/reports/' or end with .html — match both
    links = _fetch_links(url, allowed_domain="cicc.com", path_pattern=r"(reports|\.html)")
    results = []
    for l in links[:count]:
        try:
            p = requests.utils.urlparse(l).path
            segs = [s for s in p.split("/") if s]
            topic = segs[0] if segs else "cicc"
        except Exception:
            topic = "cicc"
        results.append((topic, l))
    return results


def get_yicai_urls(count: int = 20) -> List[Tuple[str, str]]:
    """Fetch recent article links from Yicai (yicai.com).

    Aggregate from main sections: news, finance, industry.
    """
    url_candidates = [
        "https://www.yicai.com/news/",
        "https://www.yicai.com/finance/",
        "https://www.yicai.com/industry/",
        "https://www.yicai.com/",
    ]
    links: List[str] = []
    for u in url_candidates:
        try:
            crawled = _fetch_links(u, allowed_domain="yicai.com", path_pattern=r"\.html$")
            links.extend(crawled)
        except Exception:
            continue
    # dedupe
    seen = set()
    uniq: List[str] = []
    for l in links:
        if l in seen:
            continue
        seen.add(l)
        uniq.append(l)
    links = uniq

    results: List[Tuple[str, str]] = []
    for l in links[:count]:
        try:
            p = requests.utils.urlparse(l).path
            segs = [s for s in p.split("/") if s]
            topic = segs[0] if segs else "yicai"
        except Exception:
            topic = "yicai"
        results.append((topic, l))
    return results


def get_thepaper_urls(count: int = 20) -> List[Tuple[str, str]]:
    """Fetch recent article links from ThePaper (thepaper.cn).

    Aggregate homepage and channels, filter to public newsDetail pages.
    """
    url_candidates = [
        "https://www.thepaper.cn/",
        "https://www.thepaper.cn/channel_25950",  # 财经
        "https://www.thepaper.cn/channel_25951",  # 科技
        "https://www.thepaper.cn/channel_25970",  # 观察
    ]
    links: List[str] = []
    for u in url_candidates:
        try:
            crawled = _fetch_links(u, allowed_domain="thepaper.cn", path_pattern=r"(newsDetail|_\d+\.html)")
            links.extend(crawled)
        except Exception:
            continue
    # dedupe
    seen = set()
    uniq: List[str] = []
    for l in links:
        if l in seen:
            continue
        seen.add(l)
        uniq.append(l)
    links = uniq

    # filter out login/member pages if present
    out: List[str] = []
    for l in links:
        try:
            path = requests.utils.urlparse(l).path or ""
            if "/member/" in path or path.startswith("/member"):
                continue
        except Exception:
            pass
        out.append(l)

    results: List[Tuple[str, str]] = []
    for l in out[:count]:
        try:
            p = requests.utils.urlparse(l).path
            segs = [s for s in p.split("/") if s]
            topic = segs[0] if segs else "thepaper"
        except Exception:
            topic = "thepaper"
        results.append((topic, l))
    return results

def get_eastmoney_urls(count: int = 20) -> List[Tuple[str, str]]:
    """Fetch recent article links from Eastmoney (东方财富网) across key sections.

        We aggregate from multiple landing pages to improve coverage:
            - finance.eastmoney.com (财经)
            - stock.eastmoney.com (股票)
            - fund.eastmoney.com (基金)
            - stock.eastmoney.com/us (美股)
            - stock.eastmoney.com/hk (港股)
            - stock.eastmoney.com/a (A股)

    Extract `.html` article links under the `eastmoney.com` domain and derive
    a rough topic label from the first path segment.
    """
    url_candidates = [
        "https://finance.eastmoney.com/",
        "https://stock.eastmoney.com/",
        "https://fund.eastmoney.com/",
        "https://stock.eastmoney.com/us/",
        "https://stock.eastmoney.com/hk/",
        "https://stock.eastmoney.com/a/",
        "https://www.eastmoney.com/",
    ]
    links: List[str] = []
    for u in url_candidates:
        try:
            crawled = _fetch_links(u, allowed_domain="eastmoney.com", path_pattern=r"\.html$")
            links.extend(crawled)
        except Exception:
            continue
    # de-duplicate while preserving order
    seen = set()
    uniq: List[str] = []
    for l in links:
        if l in seen:
            continue
        seen.add(l)
        uniq.append(l)
    links = uniq

    results: List[Tuple[str, str]] = []
    for l in links[:count]:
        try:
            p = requests.utils.urlparse(l).path
            segs = [s for s in p.split("/") if s]
            topic = segs[0] if segs else "eastmoney"
        except Exception:
            topic = "eastmoney"
        results.append((topic, l))
    return results


if __name__ == "__main__":
    # quick manual test (will run when executed directly)
    print(get_sina_urls(5))
    print(get_163_urls(5))
