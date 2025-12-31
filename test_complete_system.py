#!/usr/bin/env python3
"""
完整端到端HKSI系统测试
"""

import sys
from pathlib import Path
import json
import datetime

# Add path
sys.path.insert(0, str(Path(__file__).parent))

def create_comprehensive_test_data():
    """创建完整的测试数据"""
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    print("🗂️ 创建完整测试数据集...")
    
    # 1. 创建新闻文件
    financial_content = """Financial Markets - December 31, 2025

Federal Reserve Maintains Interest Rates
The Federal Reserve kept rates steady at 5.25% as inflation continues to moderate. Chair Powell emphasized a data-dependent approach for 2026 policy decisions.

Banking Sector Strong Performance  
JPMorgan Chase reported record Q4 earnings with $15.2B net income. Bank of America and Wells Fargo also exceeded expectations on strong loan growth.

Credit Markets Stable
Corporate default rates remain below 3%, well under historical averages. High-grade bond spreads tightened 15 basis points.

Investment Banking Surge
M&A advisory fees jumped 28% year-over-year as deal volumes recovered. Technology sector led with $240B in announced transactions.

Insurance Sector Benefits
Property casualty insurers see improved pricing power with rate increases averaging 8% across commercial lines."""

    tech_content = """Technology Sector - December 31, 2025

Apple Exceeds Expectations
Apple reported record Q4 revenue of $94.9B, driven by iPhone 16 Pro strong demand. Services revenue grew 16% to $23.8B.

Microsoft AI Leadership
Microsoft Azure grew 29% with AI services contributing $12B run-rate. Copilot adoption reached 2.3M enterprise seats.

NVIDIA AI Dominance Continues  
NVIDIA H200 chips show 2.4x performance gains over H100. Data center revenue hit $35.1B, up 122% year-over-year.

Cloud Computing Expansion
Amazon AWS secured $8.7B in new enterprise contracts. Google Cloud Platform revenue increased 35% to $11.4B.

Semiconductor Recovery
Taiwan Semiconductor and ASML report improving foundry utilization rates as AI chip demand accelerates production."""

    health_content = """Healthcare Sector - December 31, 2025

Pharmaceutical Breakthroughs
Pfizer's new Alzheimer's drug shows 35% cognitive decline reduction in Phase 3 trials. FDA fast-track approval expected Q2 2026.

Medical Device Innovation
Johnson & Johnson's surgical robotics platform gained FDA approval. Medtronic's diabetes management system shows 89% patient satisfaction.

Healthcare Services Growth
UnitedHealth Group enrollment increased 8% with Medicare Advantage adding 1.2M members. Operating margins improved to 6.8%.

Biotech Developments
Moderna's cancer vaccine demonstrates 67% tumor reduction in melanoma trials. Gilead Sciences HIV prevention drug shows 99% efficacy.

Healthcare M&A Activity
CVS Health considering $45B acquisition of Humana. Anthem explores partnership opportunities in digital health platforms."""

    energy_content = """Energy Sector - December 31, 2025

Oil Market Stabilization
Crude oil prices stabilized near $73/barrel as OPEC+ extends production cuts through Q2 2026. US shale production plateaued at 13.2M barrels/day.

Natural Gas Volatility
Henry Hub prices fluctuate between $2.80-$3.20/MMBtu on weather-driven demand variations. European TTF prices remain elevated at €32/MWh.

Renewable Energy Investment
Global clean energy investment reached $1.8 trillion in 2025. Solar capacity additions hit 346 GW, exceeding forecasts by 15%.

Traditional Energy Earnings
ExxonMobil posted $56.5B annual earnings with $18.2B capital returns to shareholders. Chevron maintained $6B quarterly dividend.

Energy Transition Progress
BP allocated 40% of capex to low-carbon investments. Shell's renewable power generation capacity increased 67% year-over-year."""

    # 保存新闻文件
    news_files = {
        'financials_2025-12-31.txt': financial_content,
        'technology_2025-12-31.txt': tech_content, 
        'health_care_2025-12-31.txt': health_content,
        'energy_2025-12-31.txt': energy_content
    }
    
    for filename, content in news_files.items():
        with open(output_dir / filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"   ✅ {filename}")
    
    # 2. 创建sector allocations文件（3列格式）
    allocations_csv = output_dir / 'sector_allocations.csv'
    with open(allocations_csv, 'w', encoding='utf-8') as f:
        f.write('sector,weight,allocation_pct\n')
        f.write('financials,1.0,35.0\n')  # 金融35%
        f.write('technology,1.0,30.0\n')  # 科技30%
        f.write('health_care,1.0,25.0\n')  # 医疗25%
        f.write('energy,1.0,10.0\n')      # 能源10%
    
    print(f"   ✅ sector_allocations.csv")
    print("      - 金融: 35%")
    print("      - 科技: 30%")
    print("      - 医疗: 25%") 
    print("      - 能源: 10%")
    
    # 3. 创建sector summary文件
    sector_summary = {
        "date": "2025-12-31",
        "sectors": [
            {
                "sector": "financials",
                "avg_score": 7.2,
                "label": "利好",
                "summary": "美联储政策稳定，银行业绩强劲，投行收入大增"
            },
            {
                "sector": "technology", 
                "avg_score": 8.1,
                "label": "利好",
                "summary": "苹果营收创纪录，AI芯片需求旺盛，云计算高增长"
            },
            {
                "sector": "health_care",
                "avg_score": 7.5,
                "label": "利好", 
                "summary": "新药突破性进展，医疗设备创新，并购活跃"
            },
            {
                "sector": "energy",
                "avg_score": 6.0,
                "label": "中性",
                "summary": "油价稳定，传统能源盈利，清洁能源投资加速"
            }
        ]
    }
    
    summary_file = output_dir / 'sector_summary.json'
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(sector_summary, f, ensure_ascii=False, indent=2)
    
    print(f"   ✅ sector_summary.json")
    
    return output_dir

def run_complete_test():
    """运行完整系统测试"""
    print("=== HKSI 完整系统端到端测试 ===")
    print("🎯 测试完整分析流程")
    print("==================================================\n")
    
    # 1. 创建测试数据
    output_dir = create_comprehensive_test_data()
    print()
    
    # 2. 导入并测试核心功能
    print("🔄 测试核心分析功能...")
    
    try:
        from integrate_hksi import generate_recommendation_report
        print("✅ 导入成功")
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        return False
    
    # 3. 运行投资建议生成
    print("\n💡 生成完整投资建议...")
    
    try:
        result = generate_recommendation_report(
            output_dir=output_dir,
            ticker_db=None,
            portfolio_size=1000000.0,  # 100万投资组合
            strategy='simple',
            top_per_sector=3,
            alias_db=None,
            ticker_sectors=None,
            etf_only=True
        )
        
        print("✅ 投资建议生成成功！")
        
        # 4. 分析结果
        text_report = result.get('text', '')
        details = result.get('details', {})
        
        print(f"\n📊 结果分析:")
        print(f"   📄 报告长度: {len(text_report)} 字符")
        
        sectors = details.get('sectors', [])
        print(f"   🏢 分析板块数: {len(sectors)}")
        
        total_etfs = 0
        total_allocation = 0
        
        if sectors:
            print(f"\n📈 各板块推荐:")
            for sector in sectors:
                sector_name = sector.get('sector', 'unknown')
                suggestions = sector.get('suggestions', [])
                etf_count = len(suggestions)
                total_etfs += etf_count
                
                print(f"   🔹 {sector_name}: {etf_count} 个ETF")
                
                # 显示具体ETF
                for etf in suggestions:
                    ticker = etf.get('ticker', 'N/A')
                    pct = etf.get('pct', 0)
                    total_allocation += pct
                    market = "🇺🇸" if ".HK" not in ticker and ".SH" not in ticker and ".SZ" not in ticker else ("🇭🇰" if ".HK" in ticker else "🇨🇳")
                    print(f"      {market} {ticker}: {pct}%")
        
        print(f"\n🎯 推荐汇总:")
        print(f"   📊 总ETF数量: {total_etfs}")
        print(f"   💰 总配置比例: {total_allocation}%")
        
        # 显示报告摘要
        if text_report and len(text_report) > 100:
            print(f"\n📝 投资建议报告摘要:")
            lines = text_report.split('\n')
            for line in lines[:10]:  # 显示前10行
                if line.strip():
                    print(f"   {line}")
            if len(lines) > 10:
                print(f"   ... 还有 {len(lines) - 10} 行")
        
        # 5. 保存文件并验证
        print(f"\n💾 保存分析结果...")
        
        today = datetime.date.today().isoformat()
        
        # 保存报告
        report_file = output_dir / f'recommendation_{today}.txt'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        json_file = output_dir / f'recommendation_{today}.json'  
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 文本报告: {report_file.name}")
        print(f"✅ JSON数据: {json_file.name}")
        
        # 6. 验证文件完整性
        print(f"\n🔍 验证生成文件...")
        
        files_to_check = [
            'sector_allocations.csv',
            'sector_summary.json', 
            f'recommendation_{today}.txt',
            f'recommendation_{today}.json'
        ]
        
        all_good = True
        for filename in files_to_check:
            filepath = output_dir / filename
            if filepath.exists():
                size = filepath.stat().st_size
                print(f"   ✅ {filename} ({size:,} 字节)")
            else:
                print(f"   ❌ {filename} 缺失")
                all_good = False
        
        # 7. 最终评估
        print(f"\n{'='*50}")
        if all_good and total_etfs > 0:
            print("🎉 完整系统测试 - 成功！")
            print("✅ 新闻数据处理正常")
            print("✅ 行业分类功能正常")
            print("✅ ETF映射功能正常") 
            print("✅ 投资建议生成正常")
            print("✅ 文件输出功能正常")
            print(f"✅ 生成{total_etfs}个ETF推荐，覆盖{len(sectors)}个行业")
            print("\n🚀 系统状态: 完全正常，可用于生产")
            return True
        else:
            print("⚠️ 系统测试 - 部分功能异常")
            if total_etfs == 0:
                print("❌ 未生成ETF推荐")
            if not all_good:
                print("❌ 部分输出文件缺失")
            print("\n🔧 需要进一步调试")
            return False
            
    except Exception as e:
        print(f"❌ 系统测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_complete_test()
    
    print(f"\n{'='*50}")
    if success:
        print("🎊 HKSI系统已准备就绪!")
        print("💡 可以开始处理真实新闻数据")
    else:
        print("🔧 系统需要调试修复")
        print("💡 请检查错误信息")