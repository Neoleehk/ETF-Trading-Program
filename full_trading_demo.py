#!/usr/bin/env python3

import sys
from pathlib import Path
import json
import datetime

# Add path
sys.path.insert(0, str(Path(__file__).parent))

def create_mock_positions():
    """创建模拟当前持仓（用于演示交易功能）"""
    positions = {
        "date": datetime.date.today().isoformat(),
        "cash_by_market": {
            "US": 200000.0,  # 20万美元现金
            "HK": 150000.0,  # 15万港币现金
            "CN": 100000.0   # 10万人民币现金
        },
        "positions": [
            {"ticker": "XLF", "shares": 1000},      # 美国金融ETF
            {"ticker": "XLK", "shares": 500},       # 美国科技ETF  
            {"ticker": "3086.HK", "shares": 2000},  # 港股金融ETF
            {"ticker": "512760.SH", "shares": 1500} # A股科技ETF
        ]
    }
    
    positions_file = Path('output/positions.json')
    positions_file.parent.mkdir(exist_ok=True)
    
    with open(positions_file, 'w', encoding='utf-8') as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 创建模拟持仓文件: {positions_file}")
    print(f"   📊 当前持仓: {len(positions['positions'])}只ETF")
    for pos in positions['positions']:
        print(f"      - {pos['ticker']}: {pos['shares']:,} 股")
    print(f"   💰 现金余额:")
    for market, cash in positions['cash_by_market'].items():
        flag = "🇺🇸" if market == "US" else ("🇭🇰" if market == "HK" else "🇨🇳")
        currency = "$" if market == "US" else ("HK$" if market == "HK" else "¥")
        print(f"      {flag} {market}: {currency}{cash:,.0f}")
    
    return positions

def create_target_allocations():
    """创建目标配置（基于HKSI推荐）"""
    targets = {
        # 金融板块 (40% = 480,000)
        "XLF": {"allocation_pct": 12.0, "target_amount": 144000.0},        # 美股金融
        "3086.HK": {"allocation_pct": 12.0, "target_amount": 144000.0},    # 港股金融
        "512800.SH": {"allocation_pct": 16.0, "target_amount": 192000.0},  # A股金融
        
        # 科技板块 (30% = 360,000)
        "XLK": {"allocation_pct": 10.0, "target_amount": 120000.0},        # 美股科技
        "3020.HK": {"allocation_pct": 10.0, "target_amount": 120000.0},    # 港股科技
        "512760.SH": {"allocation_pct": 10.0, "target_amount": 120000.0},  # A股科技
        
        # 医疗板块 (20% = 240,000)
        "XLV": {"allocation_pct": 20.0, "target_amount": 240000.0},        # 美股医疗
        
        # 能源板块 (10% = 120,000) - 反向ETF
        "ERY": {"allocation_pct": 10.0, "target_amount": 120000.0}         # 能源反向ETF
    }
    
    print("🎯 目标配置:")
    print("   金融板块 (40%):")
    print("      🇺🇸 XLF: $144,000 (12%)")
    print("      🇭🇰 3086.HK: $144,000 (12%)")
    print("      🇨🇳 512800.SH: $192,000 (16%)")
    print("   科技板块 (30%):")
    print("      🇺🇸 XLK: $120,000 (10%)")
    print("      🇭🇰 3020.HK: $120,000 (10%)")
    print("      🇨🇳 512760.SH: $120,000 (10%)")
    print("   医疗板块 (20%):")
    print("      🇺🇸 XLV: $240,000 (20%)")
    print("   能源板块 (10%):")
    print("      🇺🇸 ERY: $120,000 (10%) [反向ETF]")
    
    return targets

def run_trading_with_execution():
    """执行完整交易流程"""
    print("=== HKSI 智能交易执行系统 ===")
    print("🚀 模拟真实交易环境")
    print("==================================================\n")
    
    # 1. 创建模拟持仓
    current_positions = create_mock_positions()
    print()
    
    # 2. 创建目标配置
    targets = create_target_allocations()
    print()
    
    # 3. 使用integrate_hksi生成交易
    print("🔄 计算交易指令...")
    
    # 使用HKSI命令行参数运行交易
    import subprocess
    import sys
    
    # 运行integrate_hksi.py with --trade参数
    cmd = [
        sys.executable, "integrate_hksi.py",
        "--trade",
        "--positions-file", "output/positions.json",
        "--portfolio-size", "1200000.0",
        "--min-trade-value", "1000.0",
        "--min-turnover", "0.05",  # 5%最小换手率
        "output"  # 输出目录
    ]
    
    print(f"🤖 执行交易命令: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, 
                               capture_output=True, 
                               text=True, 
                               cwd=Path(__file__).parent,
                               encoding='utf-8')
        
        print("✅ 交易计算完成")
        print("\n📋 系统输出:")
        print(result.stdout)
        
        if result.stderr:
            print("\n⚠️ 警告/错误:")
            print(result.stderr)
            
    except Exception as e:
        print(f"❌ 执行错误: {e}")
        return
    
    # 4. 检查生成的交易文件
    print("\n📁 检查生成的交易文件...")
    
    output_dir = Path('output')
    
    # 交易记录
    trades_dir = output_dir / 'trades'
    if trades_dir.exists():
        today = datetime.date.today().isoformat()
        trades_csv = trades_dir / f'trades_{today}.csv'
        trades_json = trades_dir / f'trades_{today}.json'
        
        if trades_csv.exists():
            print(f"✅ 交易CSV: {trades_csv}")
            try:
                with open(trades_csv, 'r', encoding='utf-8-sig') as f:
                    lines = f.readlines()
                    print(f"   📊 交易记录行数: {len(lines)-1}")  # 减去表头
            except Exception as e:
                print(f"   ❌ 读取CSV失败: {e}")
        
        if trades_json.exists():
            print(f"✅ 交易JSON: {trades_json}")
            try:
                with open(trades_json, 'r', encoding='utf-8') as f:
                    trades_data = json.load(f)
                    trades_list = trades_data.get('trades', [])
                    print(f"   📋 交易指令数: {len(trades_list)}")
                    
                    if trades_list:
                        print("\n💼 具体交易指令:")
                        total_buy = 0
                        total_sell = 0
                        
                        for i, trade in enumerate(trades_list[:10], 1):  # 显示前10个
                            action = trade.get('action')
                            ticker = trade.get('ticker')
                            shares = trade.get('shares')
                            price = trade.get('price')
                            amount = trade.get('amount', 0.0)
                            
                            action_cn = "买入" if action == "BUY" else "卖出"
                            market = "🇺🇸" if ".HK" not in ticker and ".SH" not in ticker and ".SZ" not in ticker else ("🇭🇰" if ".HK" in ticker else "🇨🇳")
                            
                            print(f"   {i:2d}. {action_cn} {market} {ticker}")
                            print(f"       数量: {shares:,} 股 @ ${price:.2f}")
                            print(f"       金额: ${abs(amount):,.2f}")
                            
                            if action == "BUY":
                                total_buy += abs(amount)
                            else:
                                total_sell += abs(amount)
                        
                        if len(trades_list) > 10:
                            print(f"   ... 还有 {len(trades_list) - 10} 个交易指令")
                        
                        print(f"\n📊 交易汇总:")
                        print(f"   💰 总买入金额: ${total_buy:,.2f}")
                        print(f"   💰 总卖出金额: ${total_sell:,.2f}")
                        print(f"   💰 净流入: ${total_buy - total_sell:,.2f}")
                        
            except Exception as e:
                print(f"   ❌ 读取JSON失败: {e}")
    
    # 日志文件
    logs_dir = output_dir / 'daily_logs'
    if logs_dir.exists():
        today = datetime.date.today().isoformat()
        log_file = logs_dir / f'log_{today}.txt'
        if log_file.exists():
            print(f"\n✅ 交易日志: {log_file}")
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    lines = content.split('\n')
                    print(f"   📄 日志长度: {len(lines)} 行")
                    
                    # 显示日志摘要
                    if len(content) > 500:
                        print("\n📝 日志摘要:")
                        print(content[:500] + "...")
                    else:
                        print("\n📝 完整日志:")
                        print(content)
            except Exception as e:
                print(f"   ❌ 读取日志失败: {e}")
    
    print("\n🎉 交易系统运行完成！")
    print("📁 所有交易记录已保存到 output 目录")

if __name__ == "__main__":
    run_trading_with_execution()