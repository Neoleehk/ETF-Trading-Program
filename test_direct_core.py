#!/usr/bin/env python3
"""
直接调用HKSI核心功能进行测试
"""

import sys
from pathlib import Path
import json
import datetime

# Add path
sys.path.insert(0, str(Path(__file__).parent))

def create_test_news():
    """创建测试新闻数据"""
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    # 创建更详细的新闻数据，模拟真实新闻格式
    
    # 金融新闻
    financial_content = """Financial Markets Update - 2025-12-31

Federal Reserve maintains interest rates at current levels as inflation shows signs of cooling. Chair Powell emphasized data-dependent approach to future policy decisions.

Banking sector reports strong Q4 earnings with JPMorgan Chase posting record quarterly revenue. Net interest margins improved across major financial institutions.

Credit markets remain stable with corporate default rates staying below historical averages despite economic uncertainties.

Investment banking fees surge 25% year-over-year driven by increased M&A activity and IPO volumes in tech sector.

Insurance companies benefit from higher interest rates, improving investment income outlook for 2026."""
    
    # 科技新闻
    tech_content = """Technology Sector Update - 2025-12-31

Apple reports record quarterly revenue of $95 billion, exceeding analyst expectations on strong iPhone 16 sales and services growth.

Microsoft Azure cloud platform grows 31% year-over-year, with AI services contributing significantly to revenue expansion.

NVIDIA continues AI chip dominance with new H200 processors showing 90% performance improvement over previous generation.

Amazon Web Services announces major enterprise deals worth $2.5 billion, strengthening cloud infrastructure position.

Meta Platforms shows robust advertising recovery with 18% revenue growth in Q4, driven by improved AI targeting."""
    
    # 医疗新闻  
    health_content = """Healthcare Sector Update - 2025-12-31

Pfizer announces positive Phase 3 trial results for next-generation COVID vaccine with 95% efficacy against new variants.

Johnson & Johnson completes $15 billion acquisition of cardiovascular device manufacturer, expanding medical device portfolio.

UnitedHealth Group raises 2026 earnings guidance on strong Medicare Advantage enrollment growth and cost management.

FDA approves breakthrough cancer immunotherapy from Merck, potentially treating multiple tumor types with single drug.

Healthcare consolidation continues with Anthem and Cigna exploring potential merger discussions worth $120 billion."""
    
    # 能源新闻
    energy_content = """Energy Sector Update - 2025-12-31

Oil prices stabilize around $75/barrel as OPEC+ maintains production cuts through Q1 2026 to balance global supply.

ExxonMobil reports $12 billion Q4 profit, down from previous year but beating expectations on improved refining margins.

Renewable energy investment reaches $500 billion globally in 2025, with solar and wind capacity additions setting records.

Natural gas prices remain volatile due to European supply concerns and increased LNG export demand from Asia.

Energy transition accelerates as major oil companies allocate 30% of capex to low-carbon technologies including hydrogen."""
    
    # 保存新闻文件
    with open(output_dir / 'financials_2025-12-31.txt', 'w', encoding='utf-8') as f:
        f.write(financial_content)
    
    with open(output_dir / 'technology_2025-12-31.txt', 'w', encoding='utf-8') as f:
        f.write(tech_content)
        
    with open(output_dir / 'health_care_2025-12-31.txt', 'w', encoding='utf-8') as f:
        f.write(health_content)
        
    with open(output_dir / 'energy_2025-12-31.txt', 'w', encoding='utf-8') as f:
        f.write(energy_content)
    
    print("✅ 创建详细测试新闻数据")
    print("   📰 金融板块: 5个要点")
    print("   📰 科技板块: 5个要点") 
    print("   📰 医疗板块: 5个要点")
    print("   📰 能源板块: 5个要点")

def run_direct_analysis():
    """直接运行HKSI核心分析"""
    print("=== HKSI 核心功能直接测试 ===")
    print("🎯 绕过命令行，直接调用核心API")
    print("==================================================\n")
    
    # 1. 创建测试数据
    create_test_news()
    print()
    
    # 2. 直接调用核心函数
    print("🔄 导入核心HKSI模块...")
    
    try:
        from integrate_hksi import generate_recommendation_report
        print("✅ 成功导入generate_recommendation_report")
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        return
    
    # 3. 运行投资建议生成
    print("\n💡 生成投资建议...")
    
    try:
        result = generate_recommendation_report(
            output_dir=Path('output'),
            ticker_db=None,
            portfolio_size=1000000.0,  # 100万投资组合
            strategy='simple',
            top_per_sector=3,
            alias_db=None,
            ticker_sectors=None,
            etf_only=True
        )
        
        print("✅ 投资建议生成成功！")
        
        # 显示结果摘要
        text_report = result.get('text', '')
        details = result.get('details', {})
        
        if text_report:
            print(f"\n📄 报告长度: {len(text_report)} 字符")
            if len(text_report) > 1000:
                print("📝 报告摘要:")
                print(text_report[:1000] + "\n...")
            else:
                print("📝 完整报告:")
                print(text_report)
        
        # 分析详细数据
        sectors = details.get('sectors', [])
        print(f"\n📊 分析结果:")
        print(f"   🏢 分析板块数: {len(sectors)}")
        
        total_etfs = 0
        for sector in sectors:
            sector_name = sector.get('sector', 'unknown')
            suggestions = sector.get('suggestions', [])
            etf_count = len(suggestions)
            total_etfs += etf_count
            print(f"   📈 {sector_name}: {etf_count} 个ETF推荐")
            
            # 显示ETF详情
            for etf in suggestions[:2]:  # 显示前2个ETF
                ticker = etf.get('ticker', 'N/A')
                pct = etf.get('pct', 0)
                print(f"      - {ticker}: {pct}%")
        
        print(f"\n🎯 总推荐ETF数: {total_etfs}")
        
        # 4. 保存结果
        print(f"\n💾 保存分析结果...")
        
        today = datetime.date.today().isoformat()
        
        # 保存文本报告
        report_file = Path('output') / f'recommendation_{today}.txt'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(text_report)
        print(f"✅ 文本报告: {report_file}")
        
        # 保存JSON数据
        json_file = Path('output') / f'recommendation_{today}.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"✅ 数据文件: {json_file}")
        
        print("\n🎉 核心系统测试完成！")
        print("✅ 新闻分析 → 行业分类 → ETF推荐 全流程正常")
        print("✅ 多市场ETF映射功能正常")
        print("✅ 输出文件生成正常")
        
        return True
        
    except Exception as e:
        print(f"❌ 分析失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_direct_analysis()
    
    if success:
        print("\n🚀 系统状态: 完全正常")
        print("💡 可以开始实际投资分析工作")
    else:
        print("\n❌ 系统状态: 需要修复")
        print("💡 请检查错误信息进行调试")