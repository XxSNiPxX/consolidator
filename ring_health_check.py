#!/usr/bin/env python3
# usage: python3 ring_health_check.py rings/binance_book_chunk0
import sys
from compare_single_instrument import ring_health
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python3 ring_health_check.py rings/binance_book_chunk0")
        sys.exit(1)
    ring_health(sys.argv[1], scan_slots=512)
