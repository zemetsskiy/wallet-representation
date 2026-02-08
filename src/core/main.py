import logging
import time
from typing import Dict, Any, Optional
from ..config import setup_logging
from ..processors import SolanaSmartMoneyAnalyzer, EvmSmartMoneyAnalyzer

logger = logging.getLogger(__name__)


class SmartMoneyWorker:

    def __init__(self):
        setup_logging()
        logger.info("SMART MONEY WORKER INITIALIZED")
        self.analyzer = None

    def run(self, job_type: str = 'solana', limit: int = 10000, chain: Optional[str] = None) -> Dict[str, Any]:
        start_time = time.time()

        try:
            if job_type == 'solana':
                self.analyzer = SolanaSmartMoneyAnalyzer()
                results = self.analyzer.analyze_smart_money(limit=limit)
            elif job_type == 'evm':
                if not chain:
                    raise ValueError("Chain must be specified for EVM jobs")
                self.analyzer = EvmSmartMoneyAnalyzer()
                results = self.analyzer.analyze_smart_money(chain=chain, limit=limit)
            else:
                raise ValueError(f"Unknown job type: {job_type}")

            elapsed = time.time() - start_time
            results['elapsed_seconds'] = round(elapsed, 2)
            logger.info(f"Processing time: {elapsed:.2f}s")
            return results
        except Exception as e:
            logger.error(f"Worker failed: {e}", exc_info=True)
            raise
        finally:
            if self.analyzer:
                self.analyzer.close()


def main():
    # Default to Solana for backward compatibility or testing
    worker = SmartMoneyWorker()
    results = worker.run(job_type='solana')
    logger.info(f"Results: {results}")
    return results


if __name__ == '__main__':
    main()
