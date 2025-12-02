import argparse
import sys
from src.core import SolanaSmartMoneyWorker


def main():
    parser = argparse.ArgumentParser(description='Solana Smart Money Worker')
    parser.add_argument('--limit', type=int, default=10000)
    args = parser.parse_args()

    try:
        worker = SolanaSmartMoneyWorker()
        results = worker.run(limit=args.limit)
        print(f"\nResults: {results}")
        return 0
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())