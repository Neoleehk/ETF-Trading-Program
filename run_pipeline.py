#!/usr/bin/env python3
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

def _stamp(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def main():
    print("=== HKSI Unified Pipeline (Verbose) ===", flush=True)
    _stamp("Step 1/3: Fetching news (CN + International) — starting")
    print("   • This may take a few minutes; network activity is expected.", flush=True)
    print("   • You'll see site-by-site logs and selections below...", flush=True)
    try:
        from fetch_sites import main as fetch_sites_main
        # Moderated limits; use verbose for user visibility
        from datetime import date, timedelta
        prev_day = (date.today() - timedelta(days=1)).isoformat()
        fetch_args = [
            "--sites", "eastmoney,wallstreetcn,yicai,thepaper,caixin,cicc",
            "--count", "50",
            "--include-international",
            "--intl-max", "200",
            "--outdir", "output",
            "--keyword-threshold", "1",
            "--timeout", "15",
            "--target-date", prev_day,
            "--verbose",
        ]
        t0 = time.time()
        fetch_sites_main(fetch_args)
        _stamp(f"Step 1/3: Fetching complete in {int(time.time()-t0)}s; files written to output/")
    except Exception as e:
        _stamp(f"Step 1/3: Fetch encountered an issue (continuing): {e}")

    print("\n", flush=True)
    _stamp("Step 2/3: Recomputing allocations and recommendations — starting")
    try:
        from run_full_system import run_full_hksi_analysis
        t0 = time.time()
        run_full_hksi_analysis()
        _stamp(f"Step 2/3: Analysis complete in {int(time.time()-t0)}s; outputs updated in output/")
    except Exception as e:
        _stamp(f"Step 2/3: Analysis encountered an issue (continuing): {e}")

    print("\n", flush=True)
    _stamp("Step 3/3: Generating trades and daily log — starting")
    try:
        from execute_trading_system import run_trading_system
        t0 = time.time()
        run_trading_system()
        _stamp(f"Step 3/3: Trading complete in {int(time.time()-t0)}s; trades/logs/positions saved")
    except Exception as e:
        _stamp(f"Step 3/3: Trading failed: {e}")

    print("\n", flush=True)
    _stamp("Pipeline complete. Review output/ for artifacts (txt/json/csv/logs)")

if __name__ == "__main__":
    main()
