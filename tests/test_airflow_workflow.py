"""Run in the Airflow image to check DAG parsing and retry publication."""

import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


@unittest.skipUnless(importlib.util.find_spec("airflow"), "Requires the Airflow image")
class WorkflowTests(unittest.TestCase):
    def load_dag(self):
        from airflow.models import DagBag

        bag = DagBag(str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
        self.assertEqual(bag.import_errors, {})
        return bag.dags["shorts_user_logs"]

    def test_dependency_and_schedule(self):
        dag = self.load_dag()
        self.assertEqual(dag.get_task("crawl_shorts").downstream_task_ids, {"synthesize_logs"})
        self.assertEqual(dag.schedule_interval, "@daily")
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)

    def test_failed_attempt_then_retry_and_completed_output_reuse(self):
        import pendulum

        simulate = self.load_dag().get_task("synthesize_logs").python_callable
        attempts = []
        start = pendulum.datetime(2026, 10, 21, tz="UTC")

        def fake_pipeline(**kwargs):
            attempts.append(kwargs)
            (Path(kwargs["output_base_dir"]) / "sample.jsonl").write_text("{}\n")
            if len(attempts) == 1:
                raise RuntimeError("Interrupted simulation")

        with tempfile.TemporaryDirectory() as tmp:
            def redirect_path(value):
                if str(value) == "/opt/airflow/data_lake/raw/user_interaction_logs":
                    return Path(tmp) / "logs"
                return Path(value)

            with patch.dict(simulate.__globals__, {
                "Path": redirect_path,
                "get_current_context": lambda: {"run_id": "test_retry", "data_interval_start": start},
                "runpy": SimpleNamespace(run_path=lambda _: {"run_user_simulation_pipeline": fake_pipeline}),
            }):
                with self.assertRaisesRegex(RuntimeError, "Interrupted simulation"):
                    simulate()
                self.assertEqual(list((Path(tmp) / "logs").iterdir()), [])
                destination = simulate()
                self.assertTrue((Path(destination) / "sample.jsonl").exists())
                self.assertEqual(simulate(), destination)
                self.assertEqual(len(attempts), 2)
                self.assertEqual(attempts[-1]["start_date"], start)
                self.assertEqual(attempts[-1]["simulation_days"], 1)


if __name__ == "__main__":
    unittest.main()
