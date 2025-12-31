#!/usr/bin/env python3

import sys
from pathlib import Path
import json
import datetime

# Add path
sys.path.insert(0, str(Path(__file__).parent))

def run_trading_system():
    print("=== HKSI Trading & Logging System ===")
    print(f"Date: {datetime.date.today()}")
    print("Generating trades and execution logs...")
    print("=" * 50)
    
    try:
        # Import HKSI functions
        from integrate_hksi import (
            generate_recommendation_report,
            _build_targets_from_details,
            _generate_trades,
            _save_trades,
            _render_daily_trading_log,
            parse_urls_from_sector_file,
            find_latest_sector_file
        )
        
        output_dir = Path('output')
        
        # 1. Generate investment recommendations (amounts not used; per-market budgets drive sizing)
        print("Step 1: Generating investment recommendations...")
        # General (all markets) recommendation for overview
        result = generate_recommendation_report(
            output_dir=output_dir,
            ticker_db=None,
            portfolio_size=0.0,
            strategy='simple', 
            top_per_sector=3,
            alias_db=None,
            ticker_sectors=None,
            etf_only=True,
            allowed_markets=None
        )
        print("✅ Recommendations generated")

        # Save per-market recommendations
        today = datetime.date.today().isoformat()
        for mkt in ['US','HK','CN']:
            rec_m = generate_recommendation_report(
                output_dir=output_dir,
                ticker_db=None,
                portfolio_size=0.0,
                strategy='simple',
                top_per_sector=3,
                alias_db=None,
                ticker_sectors=None,
                etf_only=True,
                allowed_markets={mkt}
            )
            txt_path = output_dir / f'recommendation_{mkt}_{today}.txt'
            json_path = output_dir / f'recommendation_{mkt}_{today}.json'
            with txt_path.open('w', encoding='utf-8') as tf:
                tf.write(rec_m.get('text',''))
            with json_path.open('w', encoding='utf-8') as jf:
                json.dump(rec_m.get('details',{}), jf, ensure_ascii=False, indent=2)
        
        # 2. Load current positions
        print("\nStep 2: Loading current positions...")
        with open('output/positions.json', 'r') as f:
            current_positions = json.load(f)
        print(f"✅ Loaded {len(current_positions.get('positions', []))} current positions")
        
        # 3/4/5/6/7. Per-market trading: budgets=1,000,000 local currency, turnover>=10%
        print("\nStep 3: Per-market trading (US / HK / CN)...")
        markets = ['US', 'HK', 'CN']
        per_market_budget = 1_000_000.0
        min_turnover = 0.10

        # Helper to save trades per market
        def save_trades_for_market(market_code: str, payload: dict):
            out_dir = output_dir / 'trades'
            out_dir.mkdir(parents=True, exist_ok=True)
            csv_path = out_dir / f"trades_{market_code}_{today}.csv"
            json_path = out_dir / f"trades_{market_code}_{today}.json"
            import csv as _csv
            with csv_path.open('w', encoding='utf-8-sig', newline='') as cf:
                writer = _csv.writer(cf)
                writer.writerow(['datetime','ticker','action','shares','price','amount'])
                for t in payload.get('trades', []):
                    writer.writerow([t.get('datetime'), t.get('ticker'), t.get('action'), t.get('shares'), t.get('price'), t.get('amount')])
            with json_path.open('w', encoding='utf-8') as jf:
                json.dump(payload, jf, ensure_ascii=False, indent=2)

        # Positions state to update incrementally per market
        positions_state = current_positions
        logs_dir = output_dir / 'daily_logs'
        logs_dir.mkdir(parents=True, exist_ok=True)

        all_trades_count = 0
        for market in markets:
            print(f"  ▶ Processing {market} market...")
            allowed = {market}
            market_budgets = {market: per_market_budget}
            # Build targets from recommendations (only this market)
            targets = _build_targets_from_details(
                details=result.get('details', {}),
                portfolio_size=None,
                market_budgets=market_budgets,
                allowed_markets=allowed
            )
            print(f"    - Targets: {len(targets)} tickers")
            # Generate trades with turnover >= 10%
            trades_payload = _generate_trades(
                targets=targets,
                positions=positions_state,
                min_trade_value=1000.0,
                min_turnover_ratio=min_turnover,
                allowed_markets=allowed,
                market_budgets=market_budgets
            )
            trades = trades_payload.get('trades', [])
            all_trades_count += len(trades)
            print(f"    - Generated {len(trades)} orders")
            # Save trades per market
            save_trades_for_market(market, trades_payload)
            # Build references (news URLs) from latest sector files
            refs = []
            seen = set()
            try:
                # Collect URLs from latest sector files (legacy + INTL)
                for sec in (result.get('details', {}).get('sectors', []) or []):
                    sector_name = sec.get('sector', '') or ''
                    path = find_latest_sector_file(output_dir, sector_name)  # legacy
                    if path:
                        urls = parse_urls_from_sector_file(path)
                        for u in urls:
                            if u not in seen:
                                seen.add(u)
                                refs.append(u)
                # Basic market-specific domain filters
                from urllib.parse import urlparse
                def domain(u: str) -> str:
                    try:
                        return urlparse(u).netloc.lower()
                    except Exception:
                        return ''
                def is_us(d: str) -> bool:
                    return any(x in d for x in [
                        'reuters', 'bloomberg', 'wsj', 'cnbc', 'ft.com', 'yahoo.com', 'marketwatch', 'seekingalpha', 'investopedia', 'nytimes', 'forbes'
                    ])
                def is_hk(d: str) -> bool:
                    return any(x in d for x in [
                        'scmp', 'hket', 'etnet', 'hk01', 'thestandard.com.hk', 'hongkongfp', '.hk'
                    ])
                def is_cn(d: str) -> bool:
                    return any(x in d for x in [
                        'eastmoney', 'wallstreetcn', 'yicai', 'thepaper', 'caixin', 'cnstock', 'sohu', 'sina', 'ifeng', 'cctv', '163.com', '.cn'
                    ])
                # Apply market filter
                if market == 'US':
                    refs = [u for u in refs if is_us(domain(u))]
                elif market == 'HK':
                    refs = [u for u in refs if is_hk(domain(u))]
                elif market == 'CN':
                    refs = [u for u in refs if is_cn(domain(u))]
                # Cap references to 20
                refs = refs[:20]
            except Exception:
                refs = []

            # Market-specific daily log
            log_text = _render_daily_trading_log(
                date_str=today,
                urls=refs,
                targets=targets,
                positions_before=positions_state,
                trades_payload=trades_payload,
                rec_details=result.get('details', {})
            )
            log_path = logs_dir / f'log_{market}_{today}.txt'
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(log_text)
            print(f"    - Log saved: {log_path}")
            # Merge positions: replace tickers of this market + update cash of this market
            new_pos_payload = trades_payload.get('new_positions', {})
            new_list = new_pos_payload.get('positions', []) or []
            # Build map for current positions
            pos_map = {p.get('ticker'): int(p.get('shares') or 0) for p in positions_state.get('positions', [])}
            for p in new_list:
                tk = p.get('ticker')
                pos_map[tk] = int(p.get('shares') or 0)
            # write back merged list (keep ordering stable-ish)
            positions_state['positions'] = [{'ticker': tk, 'shares': sh} for tk, sh in pos_map.items()]
            # update cash for this market
            positions_state.setdefault('cash_by_market', {})
            mb_cash = new_pos_payload.get('cash_by_market', {}).get(market)
            if mb_cash is not None:
                positions_state['cash_by_market'][market] = mb_cash
            positions_state['date'] = today

        # Write merged positions once at end
        print("\nStep 4: Updating positions (merged across markets)...")
        with open('output/positions.json', 'w', encoding='utf-8') as f:
            json.dump(positions_state, f, ensure_ascii=False, indent=2)
        print("✅ Positions updated")

        # 8. Display results
        print("\n" + "=" * 50)
        print("TRADING SUMMARY")
        print("=" * 50)
        print(f"Per-market orders generated. See output/trades/trades_<US|HK|CN>_{today}.csv/json")
        
        print(f"\nFiles generated:")
        print(f"  📊 Recommendations (per market): output/recommendation_US_{today}.txt/json, recommendation_HK_{today}.txt/json, recommendation_CN_{today}.txt/json")
        print(f"  💼 Trades CSV: output/trades/trades_US_{today}.csv, trades_HK_{today}.csv, trades_CN_{today}.csv")
        print(f"  💼 Trades JSON: output/trades/trades_US_{today}.json, trades_HK_{today}.json, trades_CN_{today}.json")
        print(f"  📝 Daily logs: output/daily_logs/log_US_{today}.txt, log_HK_{today}.txt, log_CN_{today}.txt")
        print(f"  🏦 Updated positions: output/positions.json")
        
        print(f"\n🎉 HKSI Trading & Logging System - COMPLETE!")
        print("Ready for investment execution!")
        
    except Exception as e:
        print(f"❌ System error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_trading_system()