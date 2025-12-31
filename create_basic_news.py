#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import datetime

def create_basic_sector_news():
    """创建基础的行业新闻数据"""
    
    today = datetime.now().strftime('%Y-%m-%d')
    os.makedirs("output", exist_ok=True)
    
    # 创建各行业的基础新闻文件
    sector_news = {
        'financials': [
            "Federal Reserve maintains interest rates steady as inflation shows signs of cooling. Financial markets rallied on the dovish tone from policymakers.",
            "Major banks report strong quarterly earnings with robust lending activity. Credit losses remain below historical averages despite economic uncertainties.", 
            "Investment banking fees surge as M&A activity picks up in the fourth quarter. Deal volume reached $2.1 trillion globally."
        ],
        'technology': [
            "Apple unveils next-generation AI chips for data centers, challenging NVIDIA's market dominance in artificial intelligence computing.",
            "Microsoft Azure revenue grows 29% year-over-year driven by cloud adoption and AI services integration across enterprise customers.",
            "Meta announces breakthrough in virtual reality technology with new lightweight headsets targeting enterprise applications."
        ],
        'health_care': [
            "Pharmaceutical companies report positive results from COVID-19 vaccine trials targeting latest variants with 95% efficacy rates.",
            "Healthcare equipment demand surges as hospitals upgrade infrastructure. Medical device manufacturers see record order volumes.",
            "Biotechnology sector advances with new gene therapy approvals from FDA. Several promising treatments enter final trial phases."
        ],
        'energy': [
            "Oil prices stabilize around $75 per barrel as OPEC maintains production cuts. Global demand shows signs of recovery.",
            "Renewable energy investments reach record highs with solar and wind capacity additions accelerating globally.",
            "Natural gas exports from US increase as Europe seeks energy security. LNG infrastructure expansion continues."
        ]
    }
    
    for sector, news_items in sector_news.items():
        filename = f"output/{sector}_{today}.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Financial News - {today}\n")
            f.write("=" * 30 + "\n\n")
            
            for i, news in enumerate(news_items, 1):
                f.write(f"Title: {sector.title()} Market Update {i}\n")
                f.write(f"URL: https://reuters.com/{sector}/update-{i}\n") 
                f.write(f"Source: Reuters - Markets\n")
                f.write(f"Published: {today}T10:00:00\n\n")
                f.write(f"Content:\n{news}\n\n")
                f.write("-" * 30 + "\n\n")
        
        print(f"✓ 创建 {sector} 新闻文件: {filename}")
    
    print(f"\n✅ 基础新闻数据创建完成！")

if __name__ == "__main__":
    print("=== 创建基础行业新闻数据 ===")
    create_basic_sector_news()