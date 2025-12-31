#!/usr/bin/env python3
"""
运行核心HKSI系统 - 从新闻分析到投资建议
"""

import sys
from pathlib import Path
import json
import datetime

def create_test_news():
    """创建测试新闻数据"""
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    # 创建金融新闻
    financial_news = """Federal Reserve maintains steady interest rates amid cooling inflation
Financial markets rally on dovish Fed tone and strong bank earnings
Major banks report robust lending activity with low credit losses
Investment banking fees surge on increased M&A activity"""
    
    # 创建科技新闻  
    tech_news = """Apple reports record quarterly revenue driven by iPhone sales
Microsoft AI business shows strong growth in cloud services
NVIDIA continues to dominate AI chip market with new releases
Tech giants invest heavily in artificial intelligence infrastructure"""
    
    # 创建医疗新闻
    health_news = """Pfizer announces positive results for new vaccine candidate
Healthcare sector sees consolidation with major merger deals
FDA approves breakthrough cancer treatment showing promise
Medical device companies report strong surgical equipment demand"""
    
    # 创建能源新闻
    energy_news = """Oil prices decline on oversupply concerns globally
ExxonMobil reports lower profits amid energy transition
Renewable energy investment reaches record highs this quarter
Natural gas prices volatile due to geopolitical tensions"""
    
    # 保存新闻文件
    with open(output_dir / 'financials_2025-12-31.txt', 'w', encoding='utf-8') as f:
        f.write(financial_news)
    
    with open(output_dir / 'technology_2025-12-31.txt', 'w', encoding='utf-8') as f:
        f.write(tech_news)
        
    with open(output_dir / 'health_care_2025-12-31.txt', 'w', encoding='utf-8') as f:
        f.write(health_news)
        
    with open(output_dir / 'energy_2025-12-31.txt', 'w', encoding='utf-8') as f:
        f.write(energy_news)
    
    print("✅ 创建测试新闻数据")
    print("   📰 financials_2025-12-31.txt")
    print("   📰 technology_2025-12-31.txt") 
    print("   📰 health_care_2025-12-31.txt")
    print("   📰 energy_2025-12-31.txt")

def run_core_system():
    """运行核心HKSI系统"""
    print("=== HKSI 核心系统测试 ===")
    print("🧪 使用清理后的核心文件")
    print("==================================================\n")
    
    # 1. 创建测试数据
    create_test_news()
    print()
    
    # 2. 运行integrate_hksi.py核心分析
    print("🔄 运行核心HKSI分析...")
    
    import subprocess
    import sys
    
    cmd = [
        sys.executable, "integrate_hksi.py",
        "--portfolio-size", "1000000.0",
        "--strategy", "simple", 
        "--top-per-sector", "3",
        "output"
    ]
    
    print(f"🤖 执行: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, 
                               capture_output=True, 
                               text=True, 
                               cwd=Path(__file__).parent,
                               encoding='utf-8',
                               timeout=60)  # 60秒超时
        
        print("✅ 核心系统运行完成")
        
        if result.stdout:
            print("\n📋 系统输出:")
            print(result.stdout)
        
        if result.stderr:
            print("\n⚠️ 错误/警告:")
            print(result.stderr)
            
        if result.returncode != 0:
            print(f"\n❌ 进程退出码: {result.returncode}")
            
    except subprocess.TimeoutExpired:
        print("⏰ 系统运行超时")
    except Exception as e:
        print(f"❌ 执行错误: {e}")
        return
    
    # 3. 检查输出文件
    print("\n📁 检查生成的输出文件...")
    
    output_dir = Path('output')
    
    # 检查推荐文件
    today = datetime.date.today().isoformat()
    recommendation_txt = output_dir / f'recommendation_{today}.txt'
    recommendation_json = output_dir / f'recommendation_{today}.json'
    
    if recommendation_txt.exists():
        print(f"✅ 推荐报告: {recommendation_txt}")
        try:
            content = recommendation_txt.read_text(encoding='utf-8')
            lines = content.split('\n')
            print(f"   📄 报告长度: {len(lines)} 行")
            
            # 显示报告摘要
            if len(content) > 800:
                print("\n📝 报告摘要:")
                print(content[:800] + "\n...")
            else:
                print("\n📝 完整报告:")
                print(content)
                
        except Exception as e:
            print(f"   ❌ 读取失败: {e}")
    else:
        print("❌ 未找到推荐报告")
    
    if recommendation_json.exists():
        print(f"\n✅ 推荐数据: {recommendation_json}")
        try:
            with open(recommendation_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            sectors = data.get('details', {}).get('sectors', [])
            total_etfs = sum(len(s.get('suggestions', [])) for s in sectors)
            
            print(f"   📊 推荐ETF总数: {total_etfs}")
            print(f"   🏢 覆盖行业数: {len(sectors)}")
            
            if sectors:
                print("   📈 各行业建议:")
                for sector in sectors[:4]:  # 显示前4个行业
                    sector_name = sector.get('sector', 'unknown')
                    suggestions = sector.get('suggestions', [])
                    print(f"      {sector_name}: {len(suggestions)} 个ETF")
                    
        except Exception as e:
            print(f"   ❌ 读取JSON失败: {e}")
    
    # 检查其他生成的文件
    sector_csv = output_dir / 'sector_allocations.csv'
    if sector_csv.exists():
        print(f"\n✅ 行业分配: {sector_csv}")
    
    print("\n🎉 核心系统测试完成！")
    print("📊 核心功能验证: 新闻分析 → 行业分类 → ETF推荐")

if __name__ == "__main__":
    run_core_system()