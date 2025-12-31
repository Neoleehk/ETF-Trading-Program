#!/usr/bin/env python3

import sys
from pathlib import Path

# Add path
sys.path.insert(0, str(Path(__file__).parent))

from integrate_hksi import _read_sector_allocations, generate_recommendation_report
import json

def test_system():
    output_dir = Path(__file__).parent / 'output'
    
    print("Check files:")
    for f in output_dir.glob("*.csv"):
        print(f"  {f.name}")
    
    print("\nCheck allocations:")
    try:
        allocations = _read_sector_allocations(output_dir / "sector_allocations.csv")
        print("Success! Allocations:")
        for k, v in allocations.items():
            print(f"  {k}: {v}%")
    except Exception as e:
        print(f"Error: {e}")
        return
    
    print("\nGenerating recommendations...")
    try:
        result = generate_recommendation_report(
            output_dir=output_dir,
            ticker_db=None,
            portfolio_size=1000000.0,
            strategy='simple',
            top_per_sector=3,
            alias_db=None,
            ticker_sectors=None,
            etf_only=True
        )
        
        print("Success!")
        print("Text preview:")
        print(result['text'][:500])
        
        print("\nETF count:")
        total_etfs = 0
        for sector in result['details'].get('sectors', []):
            etfs = len(sector.get('suggestions', []))
            print(f"  {sector.get('sector', 'unknown')}: {etfs} ETFs")
            total_etfs += etfs
        print(f"Total: {total_etfs} ETFs")
        
    except Exception as e:
        print(f"Recommendation error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_system()