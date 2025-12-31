"""Simple heuristics to classify articles into industry sectors and markets.

This rule-based classifier checks URL, title and snippet for sector keywords
and market keywords (CN/HK/US), returning both sector and market classifications.
It's conservative but logs fallbacks in the driver.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional, Dict, Tuple

# Market Keywords for CN/HK/US classification
MARKET_KEYWORDS = {
    "CN": [  # 中国大陆
        r"A股", r"上交所", r"深交所", r"沪深", r"创业板", r"科创板", r"北交所",
        r"中国", r"内地", r"大陆", r"人民币", r"央行", r"证监会", r"银保监会",
        r"上海", r"深圳", r"北京", r"广州", r"杭州", r"苏州",
        r"shanghai", r"shenzhen", r"mainland china", r"pboc", r"csrc",
        # A股公司关键词
        r"腾讯", r"阿里", r"百度", r"字节", r"比亚迪", r"宁德时代", r"小米", r"华为",
        r"工商银行", r"建设银行", r"招商银行", r"平安银行", r"中国平安",
        r"茅台", r"五粮液", r"格力", r"美的", r"万科", r"中石油", r"中石化"
    ],
    "HK": [  # 香港
        r"港股", r"恒指", r"恒生指数", r"港交所", r"香港", r"港元", r"hkd",
        r"恒生科技", r"港股通", r"南下资金", r"北水", r"hkex",
        r"hong kong", r"hang seng", r"hsi", r"hstech",
        # 港股公司关键词 (特别标记在港上市的公司)
        r"腾讯控股", r"腾讯.*00700", r"腾讯.*港股", r"腾讯.*恒指",
        r"阿里.*09988", r"阿里.*港股", r"美团-w", r"美团.*03690",
        r"小米集团", r"小米.*01810", r"快手.*01024", r"哔哩哔哩.*09626",
        r"汇丰银行", r"渣打银行", r"友邦保险", r"中银香港",
        r"长实集团", r"新鸿基", r"恒基地产", r"中国移动.*00941", r"港铁公司"
    ],
    "US": [  # 美股
        r"美股", r"纳指", r"标普", r"道指", r"纳斯达克", r"nyse", r"美元", r"usd",
        r"美联储", r"fed", r"华尔街", r"硅谷", r"中概股", r"adr",
        r"nasdaq", r"s&p", r"dow", r"wall street", r"silicon valley",
        r"us", r"america", r"united states",
        # 美股公司关键词
        r"苹果", r"微软", r"谷歌", r"亚马逊", r"meta", r"特斯拉", r"英伟达",
        r"apple", r"microsoft", r"google", r"amazon", r"tesla", r"nvidia",
        r"netflix", r"uber", r"airbnb", r"zoom", r"salesforce",
        r"摩根大通", r"美国银行", r"花旗", r"高盛", r"jpmorgan", r"goldman sachs",
        r"可口可乐", r"麦当劳", r"星巴克", r"沃尔玛", r"coca-cola", r"mcdonald"
    ]
}

# Exchange code patterns
EXCHANGE_PATTERNS = {
    "CN": [r"\.sh", r"\.sz", r"\.bj"],
    "HK": [r"\.hk", r"\.hkg"],
    "US": [r"nasdaq:", r"nyse:"]
}

SECT_COMMUNICATIONS = [r"通信", r"电信", r"运营商", r"telecom", r"communications"]
SECT_CONS_DISCRETIONARY = [
    r"可选消费", r"消费升级", r"零售", r"百货", r"家电", r"餐饮", r"旅游", r"酒店", r"电商",
    r"汽车", r"整车", r"乘用车", r"新能源车", r"新能源汽车", r"电动车", r"EV", r"交付", r"销量",
    r"智能座舱", r"自动驾驶", r"OTA", r"retail", r"consumer discretionary"
]
SECT_CONS_STAPLES = [r"必需消费", r"食品", r"饮料", r"日化", r"consumer staple", r"consumer staples"]
SECT_ENERGY = [r"能源", r"石油", r"油价", r"天然气", r"煤炭", r"电力", r"原油", r"成品油", r"energy", r"oil", r"gas"]
SECT_FINANCIALS = [r"金融", r"银行", r"证券", r"保险", r"fund", r"financials", r"bank"]
SECT_HEALTH = [r"医疗", r"医药", r"制药", r"健康", r"healthcare", r"pharma"]
SECT_INDUSTRIALS = [
    r"工业", r"制造", r"基建", r"机械", r"工程机械", r"轨交", r"航空", r"航运", r"物流",
    r"供应链", r"工业机器人", r"智能制造", r"电机", r"industrials"
]
SECT_MATERIALS = [r"材料", r"有色", r"钢", r"化工", r"原材料", r"materials"]
SECT_REAL_ESTATE = [
    r"房地产", r"地产", r"楼市", r"房价", r"新房", r"二手房", r"商办", r"物业", r"物管",
    r"土拍", r"土地出让", r"租金", r"房贷", r"按揭", r"开发商", r"real estate"
]
SECT_TECH = [
    r"科技", r"科技股", r"芯片", r"半导体", r"晶圆", r"封测", r"EDA", r"GPU", r"CPU",
    r"云", r"云计算", r"数据中心", r"边缘计算", r"ai", r"人工智能", r"大模型", r"SaaS",
    r"软件", r"硬件", r"互联网", r"电商平台", r"通信设备", r"5G", r"technology",
    # 增强英文科技关键词
    r"tech", r"semiconductor", r"chip", r"software", r"hardware", r"cloud computing", 
    r"artificial intelligence", r"machine learning", r"iphone", r"smartphone", 
    r"mobile", r"app", r"digital", r"innovation", r"startup", r"platform",
    r"gaming", r"game", r"social media", r"e-commerce", r"fintech"
]
SECT_UTILITIES = [r"公用事业", r"电力", r"水务", r"燃气", r"供热", r"utilities"]

SECTOR_MAP = [
    ("communications", SECT_COMMUNICATIONS),
    ("consumer discretionary", SECT_CONS_DISCRETIONARY),
    ("consumer staples", SECT_CONS_STAPLES),
    ("energy", SECT_ENERGY),
    ("financials", SECT_FINANCIALS),
    ("health care", SECT_HEALTH),
    ("industrials", SECT_INDUSTRIALS),
    ("materials", SECT_MATERIALS),
    ("real estate", SECT_REAL_ESTATE),
    ("technology", SECT_TECH),
    ("utilities", SECT_UTILITIES),
]

# Sector-specific exclude patterns to reduce misclassification
SECTOR_EXCLUDES = {
    "real estate": [
        r"汽车", r"新能源车", r"电动车", r"EV", r"交付", r"销量", r"整车", r"智能座舱", r"自动驾驶"
    ],
    "technology": [
        # avoid generic地产/楼市
        r"房地产", r"楼市", r"房价", r"物业"
    ],
    "energy": [
        r"房地产", r"楼市"
    ],
}

# Load optional custom keywords from `custom_keywords.json` if present.
# Format: {"sector_name": ["关键词1", "关键词2", ...], ...}
try:
    cfg_path = Path(__file__).with_name("custom_keywords.json")
    if cfg_path.exists():
        with cfg_path.open("r", encoding="utf-8") as cf:
            extra = json.load(cf)
            if isinstance(extra, dict):
                # merge extras into SECTOR_MAP lists
                name_to_idx = {name: i for i, (name, _) in enumerate(SECTOR_MAP)}
                for k, v in extra.items():
                    if not isinstance(v, list):
                        continue
                    key = k.strip().lower()
                    if key in name_to_idx:
                        idx = name_to_idx[key]
                        # extend patterns (ensure raw strings)
                        SECTOR_MAP[idx][1].extend([str(x) for x in v if x])
except Exception:
    # silently ignore config errors
    pass


def _count_matches(text: str, patterns: list[str]) -> int:
    """Count how many times any of the patterns appear in the text."""
    txt = (text or "").lower()
    count = 0
    for p in patterns:
        try:
            # Use findall to count all occurrences of the pattern
            matches = re.findall(p.lower(), txt)
            count += len(matches)
        except re.error:
            # If regex fails, count simple string occurrences
            count += txt.count(p.lower())
    return count


def _contains_any(text: str, patterns: list[str]) -> bool:
    """Legacy function - kept for backward compatibility."""
    return _count_matches(text, patterns) > 0


def classify(url: str, title: str | None, snippet: str | None, min_keyword_count: int = 2) -> list[str]:
    """Return a list of sector names that match the text (may be empty).

    The order in SECTOR_MAP defines priority but we return all matches so
    articles can be assigned to multiple sectors.
    
    Args:
        url: Article URL
        title: Article title
        snippet: Article snippet/content
        min_keyword_count: Minimum number of keyword occurrences required for classification (default: 2)
    """
    txt = " ".join(filter(None, [url, title or "", snippet or ""]))
    matches: list[str] = []
    
    for sector_name, patterns in SECTOR_MAP:
        # Count how many times sector keywords appear
        keyword_count = _count_matches(txt, patterns)
        
        # Only consider if keywords appear enough times
        if keyword_count >= min_keyword_count:
            # apply excludes if present
            ex = SECTOR_EXCLUDES.get(sector_name, [])
            if ex and _contains_any(txt, ex):
                continue
            matches.append(sector_name)
    
    return matches


def classify_market(url: str, title: str | None, snippet: str | None) -> str:
    """Classify article by market (CN/HK/US).
    
    Returns the most likely market based on keyword matching.
    Defaults to 'CN' if no clear market indicators found.
    
    Args:
        url: Article URL
        title: Article title  
        snippet: Article snippet/content
        
    Returns:
        Market code: 'CN', 'HK', or 'US'
    """
    txt = " ".join(filter(None, [url, title or "", snippet or ""])).lower()
    
    market_scores = {}
    
    # Score each market based on keyword matches
    for market, patterns in MARKET_KEYWORDS.items():
        score = _count_matches(txt, patterns)
        
        # Check exchange patterns with higher weight
        for exchange_pattern in EXCHANGE_PATTERNS.get(market, []):
            exchange_matches = len(re.findall(exchange_pattern, txt, re.IGNORECASE))
            score += exchange_matches * 3  # Exchange codes are strong indicators
            
        market_scores[market] = score
    
    # Return market with highest score, default to CN
    if not any(market_scores.values()):
        return "CN"  # Default to China market
        
    return max(market_scores.items(), key=lambda x: x[1])[0]


def classify_market_and_sector(url: str, title: str | None, snippet: str | None, 
                             min_keyword_count: int = 2) -> Tuple[str, list[str]]:
    """Classify article by both market and sectors.
    
    Args:
        url: Article URL
        title: Article title
        snippet: Article snippet/content
        min_keyword_count: Minimum keyword count for sector classification
        
    Returns:
        Tuple of (market_code, sector_list)
    """
    market = classify_market(url, title, snippet)
    sectors = classify(url, title, snippet, min_keyword_count)
    
    return market, sectors
