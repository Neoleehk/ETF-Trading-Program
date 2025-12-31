#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from pathlib import Path
import json
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from integrate_hksi import generate_recommendation_report
import topic_classifier

def run_full_hksi_analysis():
    """运行完整的HKSI投资分析系统"""
    
    print("=== HKSI 完整投资分析系统 ===\n")
    
    output_dir = Path(__file__).parent / 'output'
    
    # 1. 分析现有新闻文件并生成情感评分
    print("1. 分析新闻文件并生成情感评分...")
    
    # 找到所有新闻文件（仅限行业文件，排除非行业文件）
    news_files = []
    for p in output_dir.glob("*.txt"):
        name = p.name
        # 排除推荐报告与非行业文件
        if name.startswith("recommendation_"):
            continue
        # 允许的行业列表（文件名中下划线或空格均可）
        valid_sectors = {
            "communications",
            "consumer_discretionary",
            "consumer staples",
            "consumer_staples",
            "energy",
            "financials",
            "health care",
            "health_care",
            "industrials",
            "materials",
            "real estate",
            "real_estate",
            "technology",
            "utilities",
        }
        # 解析文件名格式：<MARKET>_<SECTOR>_<DATE>.txt 或 legacy <SECTOR>_<DATE>.txt
        base = name[:-4]
        parts = base.split("_")
        sector_part = None
        if len(parts) >= 3 and parts[0] in {"CN","HK","US"}:
            sector_part = parts[1]
        elif len(parts) >= 2:
            sector_part = parts[0]
        # 规范化 sector 名称用于过滤
        if sector_part:
            sector_norm = sector_part.replace("_", " ").lower()
            if sector_norm in valid_sectors:
                news_files.append(p)
    if not news_files:
        print("❌ 未找到新闻文件！")
        return
    
    # 总体与分市场评分容器
    sector_scores = {}
    sector_summaries = []
    sector_scores_by_market = {"US": {}, "HK": {}, "CN": {}}
    
    print(f"📂 发现 {len(news_files)} 个新闻文件")
    
    for file_path in news_files:
        filename = file_path.name
        print(f"   分析文件: {filename}")
        
        # 从文件名提取行业信息（支持 <MARKET>_<SECTOR>_<DATE>.txt 与 <SECTOR>_<DATE>.txt）
        base = filename[:-4]
        parts = base.split("_")
        market = None
        if len(parts) >= 3 and parts[0] in {"CN","HK","US"}:
            market = parts[0]
            sector = parts[1]
        elif len(parts) >= 2:
            sector = parts[0]
        else:
            sector = base
        
        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 简单情感分析 (模拟)
        positive_words = ['strong', 'growth', 'increase', 'positive', 'record', 'surges', 'success', 'breakthrough', 'robust', 'exceptional']
        negative_words = ['decline', 'fall', 'decrease', 'negative', 'loss', 'weak', 'challenge', 'problem', 'crisis', 'risk']
        
        pos_count = sum(1 for word in positive_words if word.lower() in content.lower())
        neg_count = sum(1 for word in negative_words if word.lower() in content.lower())
        
        if pos_count > neg_count:
            score = min(8.0, 5.0 + (pos_count - neg_count) * 0.5)
        elif neg_count > pos_count:
            score = max(1.0, 5.0 - (neg_count - pos_count) * 0.5)
        else:
            score = 5.0
        
        # 规范化行业键：统一空格与下划线
        sector_key = sector.replace("_", " ").lower()
        # 汇总到总体
        sector_scores[sector_key] = sector_scores.get(sector_key, 0.0) + score
        # 汇总到分市场
        if market in {"US","HK","CN"}:
            m_scores = sector_scores_by_market[market]
            m_scores[sector_key] = m_scores.get(sector_key, 0.0) + score
        
        # 生成简要总结
        if 'technology' in sector_key:
            summary = "科技板块表现强劲，AI和云计算推动营收增长"
        elif 'financial' in sector_key:
            summary = "金融板块受益于利率政策，银行业绩表现良好"
        elif 'health' in sector_key:
            summary = "医疗板块新药研发进展顺利，疫苗效果显著"
        elif 'energy' in sector_key:
            summary = "能源板块油价稳定，新发现提升储量"
        else:
            summary = f"{sector_key}板块整体表现平稳"
        
        sector_summaries.append({
            'sector': sector_key,
            'avg_score': round(score, 2),
            'label': '利好' if score > 6 else '中性' if score >= 4 else '利空',
            'summary': summary
        })
        
        print(f"     评分: {score:.1f} ({'利好' if score > 6 else '中性' if score >= 4 else '利空'})")
    
    # 2. 生成行业分配（总体 + 分市场）
    print("\n2. 计算行业分配权重（总体+分市场）...")
    
    total_score = sum(sector_scores.values())
    if total_score == 0:
        total_score = 1
    
    allocations = {}
    for sector, score in sector_scores.items():
        main_sector = sector
        # 标准化行业名称到核心引擎的键
        if 'health' in main_sector:
            main_sector = 'health'
        elif 'financial' in main_sector:
            main_sector = 'financials'
        elif 'technolog' in main_sector:
            main_sector = 'technology'
        elif 'consumer staples' in main_sector:
            main_sector = 'consumer staples'
        elif 'consumer_discretionary' in main_sector or 'consumer discretionary' in main_sector:
            main_sector = 'consumer_discretionary'
        elif 'real estate' in main_sector:
            main_sector = 'real estate'
        elif 'communications' in main_sector:
            main_sector = 'communications'
        elif 'industrials' in main_sector:
            main_sector = 'industrials'
        elif 'materials' in main_sector:
            main_sector = 'materials'
        elif 'utilities' in main_sector:
            main_sector = 'utilities'

        weight = (score / total_score) * 100
        allocations[main_sector] = allocations.get(main_sector, 0.0) + weight
    
    # 标准化到100%
    total_weight = sum(allocations.values())
    if total_weight > 0:
        for sector in allocations:
            allocations[sector] = round((allocations[sector] / total_weight) * 100, 2)
    
    print("   行业权重分配（总体）:")
    for sector, weight in sorted(allocations.items(), key=lambda x: x[1], reverse=True):
        print(f"     {sector}: {weight}%")
    
    # 计算分市场分配
    allocations_by_market = {"US": {}, "HK": {}, "CN": {}}
    for mkt, m_scores in sector_scores_by_market.items():
        m_total = sum(m_scores.values()) or 1.0
        for sector, score in m_scores.items():
            main_sector = sector
            if 'health' in main_sector:
                main_sector = 'health'
            elif 'financial' in main_sector:
                main_sector = 'financials'
            elif 'technolog' in main_sector:
                main_sector = 'technology'
            elif 'consumer staples' in main_sector:
                main_sector = 'consumer staples'
            elif 'consumer_discretionary' in main_sector or 'consumer discretionary' in main_sector:
                main_sector = 'consumer_discretionary'
            elif 'real estate' in main_sector:
                main_sector = 'real estate'
            elif 'communications' in main_sector:
                main_sector = 'communications'
            elif 'industrials' in main_sector:
                main_sector = 'industrials'
            elif 'materials' in main_sector:
                main_sector = 'materials'
            elif 'utilities' in main_sector:
                main_sector = 'utilities'
            weight = (score / m_total) * 100
            allocations_by_market[mkt][main_sector] = round(allocations_by_market[mkt].get(main_sector, 0.0) + weight, 2)

    print("\n   分市场权重分配:")
    for mkt in ["US","HK","CN"]:
        m_alloc = allocations_by_market[mkt]
        if not m_alloc:
            print(f"     {mkt}: (无数据)")
            continue
        print(f"     {mkt}:")
        for sector, weight in sorted(m_alloc.items(), key=lambda x: x[1], reverse=True):
            print(f"       {sector}: {weight}%")

    # 3. 保存中间结果
    print("\n3. 保存分析结果...")
    
    # 保存行业分配（总体，3列格式）
    with open(output_dir / 'sector_allocations.csv', 'w', encoding='utf-8') as f:
        f.write("sector,weight,allocation_pct\n")
        for sector, pct in allocations.items():
            f.write(f"{sector},1.0,{pct}\n")

    # 保存分市场行业分配（US/HK/CN）
    for mkt in ["US","HK","CN"]:
        m_alloc = allocations_by_market[mkt]
        if not m_alloc:
            # 若没有对应市场数据则跳过
            continue
        path = output_dir / f"sector_allocations_{mkt}.csv"
        with open(path, 'w', encoding='utf-8') as f:
            f.write("sector,weight,allocation_pct\n")
            for sector, pct in m_alloc.items():
                f.write(f"{sector},1.0,{pct}\n")
    
    # 保存行业总结
    with open(output_dir / 'sector_summary.json', 'w', encoding='utf-8') as f:
        json.dump(sector_summaries, f, indent=2, ensure_ascii=False)
    
    # 4. 生成投资建议（分市场 + 总览）
    print("\n4. 生成多市场ETF投资建议（分市场）...")
    
    portfolio_size = 1000000.0
    # 总览建议（可选）
    result = generate_recommendation_report(
        output_dir=output_dir,
        ticker_db=None,
        portfolio_size=portfolio_size,
        strategy='simple',
        top_per_sector=3,
        alias_db=None,
        ticker_sectors=None,
        etf_only=True,
        allowed_markets=None
    )
    # 保存总览（可保留，亦可忽略）
    today_str = datetime.now().strftime("%Y-%m-%d")
    with open(output_dir / f'recommendation_{today_str}.txt', 'w', encoding='utf-8') as f:
        f.write(result['text'])
    with open(output_dir / f'recommendation_{today_str}.json', 'w', encoding='utf-8') as f:
        json.dump(result['details'], f, indent=2, ensure_ascii=False)

    # 逐市场生成与保存
    for mkt in ['US','HK','CN']:
        rec_m = generate_recommendation_report(
            output_dir=output_dir,
            ticker_db=None,
            portfolio_size=portfolio_size,
            strategy='simple',
            top_per_sector=3,
            alias_db=None,
            ticker_sectors=None,
            etf_only=True,
            allowed_markets={mkt}
        )
        with open(output_dir / f'recommendation_{mkt}_{today_str}.txt', 'w', encoding='utf-8') as f:
            f.write(rec_m['text'])
        with open(output_dir / f'recommendation_{mkt}_{today_str}.json', 'w', encoding='utf-8') as f:
            json.dump(rec_m['details'], f, indent=2, ensure_ascii=False)
    
    # 5. 显示最终结果
    print("\n" + "="*60)
    print("📈 HKSI 投资分析完成报告")
    print("="*60)
    print(result['text'])
    
    # 6. 市场覆盖分析
    print("\n" + "="*60)
    print("🌍 多市场ETF覆盖分析")
    print("="*60)
    
    all_tickers = []
    markets = {'US': 0, 'HK': 0, 'CN': 0}
    
    for sector in result['details'].get('sectors', []):
        for suggestion in sector.get('suggestions', []):
            ticker = suggestion.get('ticker', '')
            if ticker:
                all_tickers.append(ticker)
                if '.HK' in ticker:
                    markets['HK'] += 1
                elif '.SH' in ticker or '.SZ' in ticker:
                    markets['CN'] += 1
                else:
                    markets['US'] += 1
    
    print(f"📊 推荐ETF总数: {len(set(all_tickers))}")
    print(f"🇺🇸 美股ETF: {markets['US']} 只")
    print(f"🇭🇰 港股ETF: {markets['HK']} 只")  
    print(f"🇨🇳 A股ETF: {markets['CN']} 只")
    print(f"🌏 市场覆盖率: {len([m for m in markets.values() if m > 0])}/3 个主要市场")
    
    print("\n✅ 系统运行完成！所有文件已保存到 output 目录")
    
    return result

if __name__ == "__main__":
    run_full_hksi_analysis()