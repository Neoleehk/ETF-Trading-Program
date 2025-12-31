#!/usr/bin/env python3

import sys
from pathlib import Path
import json
import datetime

# Add path
sys.path.insert(0, str(Path(__file__).parent))

from integrate_hksi import _generate_trades, _save_trades

def create_mock_positions():
    """创建模拟当前持仓"""
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
    
    print(f"✅ 创建模拟持仓文件")
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
    total_target = sum(t['target_amount'] for t in targets.values())
    print(f"   💰 总投资金额: ${total_target:,.0f}")
    print("   📊 按板块分配:")
    print("      金融板块 (40%):")
    print("         🇺🇸 XLF: $144,000 (12%)")
    print("         🇭🇰 3086.HK: $144,000 (12%)")
    print("         🇨🇳 512800.SH: $192,000 (16%)")
    print("      科技板块 (30%):")
    print("         🇺🇸 XLK: $120,000 (10%)")
    print("         🇭🇰 3020.HK: $120,000 (10%)")
    print("         🇨🇳 512760.SH: $120,000 (10%)")
    print("      医疗板块 (20%):")
    print("         🇺🇸 XLV: $240,000 (20%)")
    print("      能源板块 (10%):")
    print("         🇺🇸 ERY: $120,000 (10%) [反向ETF]")
    
    return targets

def execute_smart_trading():
    """执行智能交易系统"""
    print("=== HKSI 智能交易执行系统 ===")
    print("🚀 从持仓分析到交易执行")
    print("==================================================\n")
    
    # 1. 创建模拟持仓
    current_positions = create_mock_positions()
    print()
    
    # 2. 创建目标配置
    targets = create_target_allocations()
    print()
    
    # 3. 生成交易指令
    print("🔄 生成智能交易指令...")
    print("   🤖 分析当前持仓与目标配置差异")
    print("   💡 计算最优交易路径")
    
    try:
        trades_payload = _generate_trades(
            targets=targets,
            positions=current_positions,
            min_trade_value=1000.0,  # 最小交易金额$1000
            min_turnover_ratio=0.05, # 最小换手率5%
            allowed_markets={'US', 'HK', 'CN'},
            market_budgets={
                'US': 600000.0,  # 美股预算60万
                'HK': 400000.0,  # 港股预算40万
                'CN': 400000.0   # A股预算40万
            }
        )
        
        print("✅ 交易指令生成成功")
        
        trades = trades_payload.get('trades', [])
        print(f"   📋 生成交易指令: {len(trades)} 个")
        
        if len(trades) == 0:
            print("   ℹ️  当前持仓已接近目标配置，无需大幅调整")
        
        print()
        
        # 4. 显示交易详情
        if trades:
            print("💼 交易指令详情:")
            print("==================================================")
            
            total_buy = 0
            total_sell = 0
            trades_by_market = {'US': [], 'HK': [], 'CN': []}
            
            for trade in trades:
                ticker = trade.get('ticker')
                if '.HK' in ticker:
                    market = 'HK'
                elif '.SH' in ticker or '.SZ' in ticker:
                    market = 'CN'
                else:
                    market = 'US'
                trades_by_market[market].append(trade)
            
            for market in ['US', 'HK', 'CN']:
                market_trades = trades_by_market[market]
                if not market_trades:
                    continue
                    
                flag = "🇺🇸" if market == "US" else ("🇭🇰" if market == "HK" else "🇨🇳")
                print(f"\n{flag} {market} 市场交易 ({len(market_trades)} 个):")
                
                for i, trade in enumerate(market_trades, 1):
                    action = trade.get('action')
                    ticker = trade.get('ticker')
                    shares = trade.get('shares')
                    price = trade.get('price')
                    amount = trade.get('amount', 0.0)
                    
                    action_cn = "🟢 买入" if action == "BUY" else "🔴 卖出"
                    
                    print(f"   {i}. {action_cn} {ticker}")
                    print(f"      数量: {shares:,} 股")
                    print(f"      价格: ${price:.2f}/股")
                    print(f"      金额: ${abs(amount):,.2f}")
                    print()
                    
                    if action == "BUY":
                        total_buy += abs(amount)
                    else:
                        total_sell += abs(amount)
            
            print(f"📊 交易汇总:")
            print(f"   💰 总买入金额: ${total_buy:,.2f}")
            print(f"   💰 总卖出金额: ${total_sell:,.2f}")
            print(f"   💰 净资金流动: ${total_buy - total_sell:,.2f}")
            
            print()
        
        # 5. 保存交易记录
        print("💾 保存交易记录和日志...")
        
        output_dir = Path('output')
        _save_trades(output_dir, trades_payload)
        
        # 保存新持仓
        new_positions = trades_payload.get('new_positions', {})
        positions_file = output_dir / 'positions.json'
        with open(positions_file, 'w', encoding='utf-8') as f:
            json.dump(new_positions, f, ensure_ascii=False, indent=2)
        
        print("✅ 记录保存成功")
        print(f"   📁 交易记录: {output_dir}/trades/")
        print(f"   📊 更新持仓: {positions_file}")
        print()
        
        # 6. 显示执行后持仓
        print("🏦 交易执行后预期持仓:")
        print("==================================================")
        
        final_positions = new_positions.get('positions', [])
        final_cash = new_positions.get('cash_by_market', {})
        portfolio_value = trades_payload.get('portfolio_value', 0.0)
        prices = trades_payload.get('prices', {})
        
        if final_positions:
            print("📈 ETF持仓:")
            positions_by_market = {'US': [], 'HK': [], 'CN': []}
            
            for pos in final_positions:
                ticker = pos.get('ticker')
                shares = pos.get('shares', 0)
                if shares > 0:
                    if '.HK' in ticker:
                        market = 'HK'
                    elif '.SH' in ticker or '.SZ' in ticker:
                        market = 'CN'
                    else:
                        market = 'US'
                    positions_by_market[market].append(pos)
            
            for market in ['US', 'HK', 'CN']:
                market_positions = positions_by_market[market]
                if not market_positions:
                    continue
                    
                flag = "🇺🇸" if market == "US" else ("🇭🇰" if market == "HK" else "🇨🇳")
                print(f"   {flag} {market} 市场:")
                
                for pos in market_positions:
                    ticker = pos.get('ticker')
                    shares = pos.get('shares', 0)
                    price = prices.get(ticker, 0.0)
                    value = shares * price
                    pct = (value / portfolio_value * 100) if portfolio_value > 0 else 0
                    
                    print(f"      {ticker}: {shares:,} 股")
                    print(f"         价值: ${value:,.0f} ({pct:.1f}%)")
        
        print(f"\n💰 现金余额:")
        total_cash = 0
        for market, cash in final_cash.items():
            flag = "🇺🇸" if market == "US" else ("🇭🇰" if market == "HK" else "🇨🇳")
            currency = "$" if market == "US" else ("HK$" if market == "HK" else "¥")
            print(f"   {flag} {market}: {currency}{cash:,.2f}")
            total_cash += cash
        
        print(f"\n📊 投资组合总值: ${portfolio_value:,.2f}")
        print(f"   💎 ETF投资: ${portfolio_value - total_cash:,.2f}")
        print(f"   💰 现金比例: {(total_cash/portfolio_value*100):.1f}%")
        
        # 7. 生成交易执行总结
        print("\n🎯 交易执行总结:")
        print("==================================================")
        
        if trades:
            print(f"✅ 成功生成 {len(trades)} 个交易指令")
            print("✅ 投资组合已向目标配置调整")
            print("✅ 实现多市场ETF分散投资")
            
            sectors_covered = set()
            for ticker in targets.keys():
                if 'XLF' in ticker or '3086' in ticker or '512800' in ticker:
                    sectors_covered.add('金融')
                elif 'XLK' in ticker or '3020' in ticker or '512760' in ticker:
                    sectors_covered.add('科技')
                elif 'XLV' in ticker:
                    sectors_covered.add('医疗')
                elif 'ERY' in ticker:
                    sectors_covered.add('能源(反向)')
            
            print(f"📊 覆盖板块: {', '.join(sectors_covered)}")
            print(f"🌏 涉及市场: 美股、港股、A股")
            
        else:
            print("ℹ️  当前持仓已优化，无需调整")
        
        print("\n🎉 HKSI智能交易系统运行完成！")
        print("📈 投资组合已优化配置完毕")
        
    except Exception as e:
        print(f"❌ 交易生成失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    execute_smart_trading()