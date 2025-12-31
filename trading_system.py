#!/usr/bin/env python3

import sys
from pathlib import Path
import json
import datetime

# Add path
sys.path.insert(0, str(Path(__file__).parent))

from integrate_hksi import main as hksi_main, _generate_trades, generate_recommendation_report, _save_trades

def create_initial_positions():
    """创建初始持仓文件（空持仓开始交易）"""
    positions = {
        "date": datetime.date.today().isoformat(),
        "cash_by_market": {
            "US": 400000.0,  # 40万美元
            "HK": 400000.0,  # 40万港币  
            "CN": 400000.0   # 40万人民币
        },
        "positions": []  # 空持仓开始
    }
    
    positions_file = Path('output/positions.json')
    positions_file.parent.mkdir(exist_ok=True)
    
    with open(positions_file, 'w', encoding='utf-8') as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 创建初始持仓文件: {positions_file}")
    print(f"   💰 美股现金: ${positions['cash_by_market']['US']:,.0f}")
    print(f"   💰 港股现金: HK${positions['cash_by_market']['HK']:,.0f}")
    print(f"   💰 A股现金: ¥{positions['cash_by_market']['CN']:,.0f}")
    print(f"   📊 初始持仓: 0只股票（全现金开始）")
    
    return positions

def run_complete_trading_system():
    """运行完整的交易系统"""
    print("=== HKSI 智能交易系统 ===")
    print("🤖 从投资建议到交易执行的完整流程")
    print("==================================================\n")
    
    # 1. 创建初始持仓
    positions = create_initial_positions()
    print()
    
    # 2. 生成投资建议
    print("📊 生成投资建议...")
    output_dir = Path('output')
    
    recommendation = generate_recommendation_report(
        output_dir=output_dir,
        ticker_db=None,
        portfolio_size=1200000.0,  # 120万总投资
        strategy='simple',
        top_per_sector=3,
        alias_db=None,
        ticker_sectors=None,
        etf_only=True
    )
    
    print("✅ 投资建议生成完成")
    print(f"   📈 推荐ETF数量: {len([s['suggestions'] for s in recommendation['details'].get('sectors', [])])}")
    print()
    
    # 3. 生成交易指令
    print("🔄 生成交易指令...")
    
    # 从建议中提取目标配置
    targets = {}
    total_value = 1200000.0
    
    for sector_data in recommendation['details'].get('sectors', []):
        for suggestion in sector_data.get('suggestions', []):
            ticker = suggestion.get('ticker')
            pct = suggestion.get('pct', 0.0)
            if ticker and pct > 0:
                targets[ticker] = {
                    'allocation_pct': pct,
                    'target_amount': round(total_value * (pct / 100.0), 2)
                }
    
    print(f"   🎯 目标持仓: {len(targets)}只ETF")
    
    # 生成交易
    trades_payload = _generate_trades(
        targets=targets,
        positions=positions,
        min_trade_value=1000.0,  # 最小交易金额1000
        min_turnover_ratio=0.1,  # 最小换手率10%
        allowed_markets={'US', 'HK', 'CN'}
    )
    
    print("✅ 交易指令生成完成")
    print(f"   📋 交易指令数: {len(trades_payload.get('trades', []))}")
    print()
    
    # 4. 显示交易计划
    print("💼 今日交易计划:")
    print("==================================================")
    trades = trades_payload.get('trades', [])
    
    if not trades:
        print("   ℹ️  无需交易（目标配置与当前持仓匹配）")
    else:
        total_buy = 0
        total_sell = 0
        
        for i, trade in enumerate(trades, 1):
            action = trade.get('action')
            ticker = trade.get('ticker')
            shares = trade.get('shares')
            price = trade.get('price')
            amount = trade.get('amount', 0.0)
            
            action_cn = "买入" if action == "BUY" else "卖出"
            market = "🇺🇸" if ".HK" not in ticker and ".SH" not in ticker and ".SZ" not in ticker else ("🇭🇰" if ".HK" in ticker else "🇨🇳")
            
            print(f"   {i:2d}. {action_cn} {market} {ticker}")
            print(f"       数量: {shares:,} 股")
            print(f"       价格: ${price:.2f}")
            print(f"       金额: ${abs(amount):,.2f}")
            print()
            
            if action == "BUY":
                total_buy += abs(amount)
            else:
                total_sell += abs(amount)
        
        print(f"📊 交易汇总:")
        print(f"   💰 总买入金额: ${total_buy:,.2f}")
        print(f"   💰 总卖出金额: ${total_sell:,.2f}")
        print(f"   💰 净流入: ${total_buy - total_sell:,.2f}")
        print()
    
    # 5. 保存交易记录
    print("💾 保存交易记录...")
    
    # 保存交易文件
    _save_trades(output_dir, trades_payload)
    
    # 保存新的持仓
    new_positions = trades_payload.get('new_positions', {})
    positions_file = output_dir / 'positions.json'
    with open(positions_file, 'w', encoding='utf-8') as f:
        json.dump(new_positions, f, ensure_ascii=False, indent=2)
    
    print("✅ 交易记录保存完成")
    print(f"   📁 交易记录: {output_dir}/trades/")
    print(f"   📊 持仓更新: {positions_file}")
    print()
    
    # 6. 显示最终持仓
    print("🏦 交易后持仓:")
    print("==================================================")
    
    final_positions = new_positions.get('positions', [])
    final_cash = new_positions.get('cash_by_market', {})
    
    if final_positions:
        for pos in final_positions:
            ticker = pos.get('ticker')
            shares = pos.get('shares', 0)
            if shares > 0:
                market = "🇺🇸" if ".HK" not in ticker and ".SH" not in ticker and ".SZ" not in ticker else ("🇭🇰" if ".HK" in ticker else "🇨🇳")
                price = trades_payload.get('prices', {}).get(ticker, 0.0)
                value = shares * price
                print(f"   {market} {ticker}: {shares:,} 股 (价值: ${value:,.2f})")
    
    print(f"\n💰 剩余现金:")
    for market, cash in final_cash.items():
        flag = "🇺🇸" if market == "US" else ("🇭🇰" if market == "HK" else "🇨🇳")
        currency = "$" if market == "US" else ("HK$" if market == "HK" else "¥")
        print(f"   {flag} {market}: {currency}{cash:,.2f}")
    
    portfolio_value = trades_payload.get('portfolio_value', 0.0)
    print(f"\n📈 总投资组合价值: ${portfolio_value:,.2f}")
    
    print("\n🎉 交易系统运行完成！")
    return trades_payload

if __name__ == "__main__":
    trades_result = run_complete_trading_system()