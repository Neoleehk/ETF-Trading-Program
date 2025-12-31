#!/usr/bin/env python3

import sys
from pathlib import Path
import json
import datetime

# Add path
sys.path.insert(0, str(Path(__file__).parent))

from integrate_hksi import generate_recommendation_report

def run_hksi_analysis():
    print("=== HKSI Investment Analysis System ===")
    print(f"Date: {datetime.date.today()}")
    print("Generating investment recommendations...")
    print("=" * 50)
    
    try:
        result = generate_recommendation_report(
            output_dir=Path('output'),
            ticker_db=None,
            portfolio_size=1000000.0,  # $1M portfolio
            strategy='simple',
            top_per_sector=3,
            alias_db=None,
            ticker_sectors=None,
            etf_only=True
        )
        
        print("SUCCESS: Analysis completed!")
        
        # Show results
        details = result.get('details', {})
        sectors = details.get('sectors', [])
        
        print(f"\nInvestment Summary:")
        print(f"  Sectors analyzed: {len(sectors)}")
        
        total_etfs = 0
        for sector in sectors:
            sector_name = sector.get('sector', 'unknown')
            suggestions = sector.get('suggestions', [])
            sector_etfs = len(suggestions)
            total_etfs += sector_etfs
            
            print(f"\n{sector_name.upper()} Sector:")
            for etf in suggestions:
                ticker = etf.get('ticker', 'N/A')
                pct = etf.get('pct', 0)
                amount = pct * 10000  # Based on $1M
                
                market = "US" if ".HK" not in ticker and ".SH" not in ticker else ("HK" if ".HK" in ticker else "CN")
                print(f"  {market} {ticker}: {pct}% = ${amount:,.0f}")
        
        print(f"\nTotals:")
        print(f"  Recommended ETFs: {total_etfs}")
        print(f"  Markets covered: US + HK + CN")
        
        # Show text report key lines
        text_report = result.get('text', '')
        if text_report:
            print(f"\nKey recommendations:")
            lines = text_report.split('\n')
            for line in lines:
                if 'ETF' in line and ('$' in line or '%' in line):
                    print(f"  {line.strip()}")
        
        today = datetime.date.today().isoformat()
        print(f"\nFiles saved:")
        print(f"  Report: output/recommendation_{today}.txt")
        print(f"  Data: output/recommendation_{today}.json")
        print(f"\nHKSI analysis complete - ready for investment decisions!")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_hksi_analysis()