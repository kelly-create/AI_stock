"""PR3 Portfolio Opening/Reconciliation replay and transaction tests."""

from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import select

from src.config import Config
from src.services.portfolio_reconciliation_service import (
    PortfolioReconciliationConflictError,
    PortfolioReconciliationService,
)
from src.services.portfolio_service import PortfolioService
from src.storage import DatabaseManager, PortfolioReconciliationRecord, utc_naive_now


class PortfolioReconciliationServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.env_path = Path(self.temp_dir.name) / ".env"
        self.db_path = Path(self.temp_dir.name) / "portfolio_reconciliation.db"
        self.env_path.write_text(
            "\n".join(
                [
                    "STOCK_LIST=600519",
                    "GEMINI_API_KEY=test",
                    "ADMIN_AUTH_ENABLED=false",
                    f"DATABASE_PATH={self.db_path}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        os.environ["ENV_FILE"] = str(self.env_path)
        os.environ["DATABASE_PATH"] = str(self.db_path)
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()
        self.portfolio = PortfolioService()
        self.reconciliation = PortfolioReconciliationService(portfolio_service=self.portfolio)
        self.account_id = self.portfolio.create_account(
            name="reconciliation",
            broker="unit-test",
            market="cn",
            base_currency="CNY",
        )["id"]

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("ENV_FILE", None)
        os.environ.pop("DATABASE_PATH", None)
        self.temp_dir.cleanup()

    def _opening_preview(self):
        return self.reconciliation.preview(
            account_id=self.account_id,
            event_type="opening",
            effective_date=date(2026, 8, 1),
            cash=[{"currency": "CNY", "balance": 1000.0}],
            positions=[
                {
                    "stock_code": "600519",
                    "market": "cn",
                    "currency": "CNY",
                    "quantity": 10.0,
                    "total_cost": 900.0,
                }
            ],
            source="broker_statement",
            note="opening",
        )

    def test_opening_token_is_hashed_and_replay_enables_sell(self) -> None:
        preview = self._opening_preview()
        token_hash = hashlib.sha256(preview["preview_token"].encode("utf-8")).hexdigest()
        with self.db.get_session() as session:
            row = session.execute(
                select(PortfolioReconciliationRecord).where(
                    PortfolioReconciliationRecord.id == preview["id"]
                )
            ).scalar_one()
            self.assertEqual(row.preview_token, token_hash)
            self.assertNotEqual(row.preview_token, preview["preview_token"])

        applied = self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=preview["preview_token"],
            idempotency_key="opening-1",
        )
        self.assertEqual(applied["event_version"], 1)
        self.assertEqual(applied["status"], "applied")

        # Opening is replayed before same-day trades and therefore participates
        # in oversell validation rather than being forged as a buy.
        self.portfolio.record_trade(
            account_id=self.account_id,
            symbol="600519",
            market="cn",
            currency="CNY",
            trade_date=date(2026, 8, 1),
            side="sell",
            quantity=2.0,
            price=100.0,
        )
        state = self.portfolio.get_reconciliation_book_state(
            account_id=self.account_id,
            as_of=date(2026, 8, 1),
            event_type="adjustment",
        )
        self.assertEqual(state["positions"][0]["quantity"], 8.0)
        self.assertEqual(state["positions"][0]["total_cost"], 720.0)
        self.assertEqual(state["cash"], [{"currency": "CNY", "balance": 1200.0}])

    def test_adjustment_is_state_set_and_idempotent(self) -> None:
        opening = self._opening_preview()
        self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=opening["preview_token"],
            idempotency_key="opening-1",
        )
        preview = self.reconciliation.preview(
            account_id=self.account_id,
            event_type="adjustment",
            effective_date=date(2026, 8, 2),
            cash=[{"currency": "CNY", "balance": 750.0}],
            positions=[
                {
                    "stock_code": "600519",
                    "market": "cn",
                    "currency": "CNY",
                    "quantity": 5.0,
                    "total_cost": 450.0,
                }
            ],
            source="broker_statement",
            note="daily reconcile",
        )
        first = self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=preview["preview_token"],
            idempotency_key="adjustment-1",
        )
        second = self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=preview["preview_token"],
            idempotency_key="adjustment-1",
        )
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["event_version"], 2)
        state = self.portfolio.get_reconciliation_book_state(
            account_id=self.account_id,
            as_of=date(2026, 8, 2),
            event_type="adjustment",
        )
        self.assertEqual(state["positions"][0]["quantity"], 5.0)
        self.assertEqual(state["positions"][0]["total_cost"], 450.0)
        self.assertEqual(state["cash"][0]["balance"], 750.0)

    def test_state_set_replay_uses_absolute_target_not_only_changed_rows(self) -> None:
        opening = self._opening_preview()
        self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=opening["preview_token"],
            idempotency_key="opening-1",
        )
        preview = self.reconciliation.preview(
            account_id=self.account_id,
            event_type="adjustment",
            effective_date=date(2026, 8, 2),
            cash=[{"currency": "CNY", "balance": 1000.0}],
            positions=[
                {
                    "stock_code": "600519",
                    "market": "cn",
                    "currency": "CNY",
                    "quantity": 10.0,
                    "total_cost": 900.0,
                }
            ],
            source="broker_statement",
            note="unchanged absolute state",
        )
        applied = self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=preview["preview_token"],
            idempotency_key="adjustment-absolute",
        )
        self.assertEqual(applied["adjustment_count"], 0)

        # A later backfill is ordered before the already-applied end-of-day
        # reconciliation.  The absolute target must still win even though no
        # diff rows existed for the unchanged identities at preview time.
        self.portfolio.record_cash_ledger(
            account_id=self.account_id,
            event_date=date(2026, 8, 2),
            direction="in",
            amount=250.0,
            currency="CNY",
        )
        self.portfolio.record_trade(
            account_id=self.account_id,
            symbol="000001",
            market="cn",
            currency="CNY",
            trade_date=date(2026, 8, 2),
            side="buy",
            quantity=1.0,
            price=10.0,
        )

        state = self.portfolio.get_reconciliation_book_state(
            account_id=self.account_id,
            as_of=date(2026, 8, 2),
            event_type="adjustment",
        )
        self.assertEqual(state["cash"], [{"currency": "CNY", "balance": 1000.0}])
        self.assertEqual(
            state["positions"],
            [
                {
                    "stock_code": "600519",
                    "market": "cn",
                    "currency": "CNY",
                    "quantity": 10.0,
                    "total_cost": 900.0,
                }
            ],
        )

    def test_same_day_reconciliations_follow_apply_version_not_preview_id(self) -> None:
        opening = self._opening_preview()
        self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=opening["preview_token"],
            idempotency_key="opening-1",
        )
        change = self.reconciliation.preview(
            account_id=self.account_id,
            event_type="adjustment",
            effective_date=date(2026, 8, 2),
            cash=[{"currency": "CNY", "balance": 2000.0}],
            positions=[],
            source="broker_statement",
            note="created first, applied second",
        )
        no_op = self.reconciliation.preview(
            account_id=self.account_id,
            event_type="adjustment",
            effective_date=date(2026, 8, 2),
            cash=[{"currency": "CNY", "balance": 1000.0}],
            positions=[
                {
                    "stock_code": "600519",
                    "market": "cn",
                    "currency": "CNY",
                    "quantity": 10.0,
                    "total_cost": 900.0,
                }
            ],
            source="broker_statement",
            note="created second, applied first",
        )
        self.assertEqual(no_op["diff"]["adjustments"], [])
        self.assertEqual(no_op["warnings"], ["fifo_lineage_reset:cn:600519:CNY"])
        self.assertLess(change["id"], no_op["id"])
        first_applied = self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=no_op["preview_token"],
            idempotency_key="same-day-noop",
        )
        second_applied = self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=change["preview_token"],
            idempotency_key="same-day-change",
        )
        self.assertLess(first_applied["event_version"], second_applied["event_version"])

        state = self.portfolio.get_reconciliation_book_state(
            account_id=self.account_id,
            as_of=date(2026, 8, 2),
            event_type="adjustment",
        )
        self.assertEqual(state["cash"], [{"currency": "CNY", "balance": 2000.0}])
        self.assertEqual(state["positions"], [])

    def test_concurrent_same_key_apply_is_idempotent(self) -> None:
        preview = self._opening_preview()

        def apply_once():
            return self.reconciliation.apply(
                account_id=self.account_id,
                preview_token=preview["preview_token"],
                idempotency_key="concurrent-opening",
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: apply_once(), range(2)))

        self.assertEqual({item["id"] for item in results}, {preview["id"]})
        self.assertEqual({item["event_version"] for item in results}, {1})

    def test_expired_preview_is_durable_on_read_and_apply(self) -> None:
        preview = self._opening_preview()
        with self.db.get_session() as session:
            row = session.execute(
                select(PortfolioReconciliationRecord).where(
                    PortfolioReconciliationRecord.id == preview["id"]
                )
            ).scalar_one()
            row.expires_at = utc_naive_now() - timedelta(seconds=1)
            session.commit()

        listed = self.reconciliation.list_records(
            account_id=self.account_id,
            include_previews=True,
        )
        self.assertEqual(listed[0]["status"], "expired")
        with self.assertRaisesRegex(PortfolioReconciliationConflictError, "expired"):
            self.reconciliation.apply(
                account_id=self.account_id,
                preview_token=preview["preview_token"],
                idempotency_key="expired-opening",
            )
        detail = self.reconciliation.get_record(
            account_id=self.account_id,
            reconciliation_id=preview["id"],
        )
        self.assertEqual(detail["status"], "expired")

    def test_apply_rejects_stale_preview_without_writing_adjustments(self) -> None:
        opening = self._opening_preview()
        self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=opening["preview_token"],
            idempotency_key="opening-1",
        )
        preview = self.reconciliation.preview(
            account_id=self.account_id,
            event_type="adjustment",
            effective_date=date(2026, 8, 2),
            cash=[{"currency": "CNY", "balance": 1000.0}],
            positions=[],
            source="broker_statement",
            note=None,
        )
        self.portfolio.record_cash_ledger(
            account_id=self.account_id,
            event_date=date(2026, 8, 2),
            direction="in",
            amount=1.0,
            currency="CNY",
        )
        with self.assertRaisesRegex(PortfolioReconciliationConflictError, "ledger changed"):
            self.reconciliation.apply(
                account_id=self.account_id,
                preview_token=preview["preview_token"],
                idempotency_key="stale-1",
            )

    def test_only_one_opening_can_be_applied(self) -> None:
        opening = self._opening_preview()
        self.reconciliation.apply(
            account_id=self.account_id,
            preview_token=opening["preview_token"],
            idempotency_key="opening-1",
        )
        with self.assertRaisesRegex(PortfolioReconciliationConflictError, "already exists"):
            self._opening_preview()


if __name__ == "__main__":
    unittest.main()
