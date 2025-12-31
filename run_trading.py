#!/usr/bin/env python3

import sys
from pathlib import Path
import json
import datetime
import subprocess

def run_hksi_with_trading():
    print("=== HKSI Trading System ===")
    print(f"Date: {datetime.date.today()}")
    print("Running investment analysis with trading execution...")
    print("=" * 50)
    
    # Run integrate_hksi.py with trading enabled
    cmd = [
        sys.executable, "integrate_hksi.py",
        "--trade",
        "--positions-file", "output/positions.json",
        "--portfolio-size", "1000000.0",
        "--strategy", "simple",
        "--top-per-sector", "3",
        "--min-trade-value", "1000.0",
        "--budget-us", "400000.0",
        "--budget-hk", "300000.0", 
        "--budget-cn", "300000.0"
    ]
    
    print(f"Executing: {' '.join(cmd)}")
    print()
    
    try:
        result = subprocess.run(cmd, 
                               capture_output=True, 
                               text=True, 
                               cwd=Path(__file__).parent,
                               encoding='utf-8')
        
        print("=== SYSTEM OUTPUT ===")
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print("=== WARNINGS/ERRORS ===")
            print(result.stderr)
        
        print(f"Exit code: {result.returncode}")
        
        # Check for generated files
        output_dir = Path('output')
        today = datetime.date.today().isoformat()
        
        print("\n=== GENERATED FILES ===")
        
        # Check trades
        trades_dir = output_dir / 'trades'
        if trades_dir.exists():
            trades_csv = trades_dir / f'trades_{today}.csv'
            trades_json = trades_dir / f'trades_{today}.json'
            
            if trades_csv.exists():
                print(f"✅ Trades CSV: {trades_csv}")
                with open(trades_csv, 'r', encoding='utf-8-sig') as f:
                    lines = f.readlines()
                    print(f"   Trades recorded: {len(lines)-1}")
            
            if trades_json.exists():
                print(f"✅ Trades JSON: {trades_json}")
                with open(trades_json, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    trades = data.get('trades', [])
                    print(f"   Trade orders: {len(trades)}")
                    
                    if trades:
                        print("   Sample trades:")
                        for trade in trades[:3]:
                            action = trade.get('action')
                            ticker = trade.get('ticker')
                            shares = trade.get('shares')
                            price = trade.get('price')
                            amount = trade.get('amount')
                            print(f"     {action} {ticker}: {shares} shares @ ${price:.2f} = ${amount:.2f}")
        
        # Check daily logs
        logs_dir = output_dir / 'daily_logs'
        if logs_dir.exists():
            log_file = logs_dir / f'log_{today}.txt'
            if log_file.exists():
                print(f"✅ Daily log: {log_file}")
                with open(log_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    lines = content.split('\n')
                    print(f"   Log lines: {len(lines)}")
        
        # Check recommendations
        rec_file = output_dir / f'recommendation_{today}.txt'
        if rec_file.exists():
            print(f"✅ Recommendations: {rec_file}")
        
        # Check updated positions
        pos_file = output_dir / 'positions.json'
        if pos_file.exists():
            print(f"✅ Updated positions: {pos_file}")
            with open(pos_file, 'r', encoding='utf-8') as f:
                pos_data = json.load(f)
                positions = pos_data.get('positions', [])
                print(f"   Holdings: {len(positions)} positions")
        
        print("\n🚀 HKSI Trading System execution complete!")
        
    except Exception as e:
        print(f"❌ Execution failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_hksi_with_trading()