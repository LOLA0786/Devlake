
import os
import requests
from pathlib import Path
import yaml
import polars as pl
from typing import Union, Dict, Any, List
from textwrap import dedent
import hashlib
import json
import shutil
import time
import random

from .devlake_executors import LocalExecutor, CloudExecutor


def run_tests(df: pl.DataFrame, tests: List[Dict[str, Any]]):
    """Executes data quality assertions (REQ-010)."""
    print("\n  -> Running Data Quality Tests...")

    def assert_no_null(df: pl.DataFrame, column: str) -> bool:
        return df[column].null_count() == 0

    def assert_unique(df: pl.DataFrame, column: str) -> bool:
        return len(df[column].unique()) == len(df)

    test_results = True
    for test in tests:
        test_type = list(test.keys())[0]
        test_value = list(test.values())[0]

        if test_type == 'assert_no_null':
            col = test_value
            result = assert_no_null(df, col)
            status = "PASS" if result else "FAIL"
            print(f"    [{status}] assert_no_null on column '{col}'.")
            if not result: test_results = False

        elif test_type == 'assert_unique':
            col = test_value
            result = assert_unique(df, col)
            status = "PASS" if result else "FAIL"
            print(f"    [{status}] assert_unique on column '{col}'.")
            if not result: test_results = False

    return test_results


def generate_pipeline_hash(pipeline_config: Dict[str, Any]) -> str:
    """Generates a hash based on pipeline configuration for data versioning (REQ-006)."""
    config_to_hash = pipeline_config.copy()
    config_to_hash.pop('triggers', None)
    config_json = json.dumps(config_to_hash, sort_keys=True, indent=None)
    return hashlib.sha256(config_json.encode('utf-8')).hexdigest()[:8]


class PipelineRunner:
    """
    Simulates the `devlake run` command, executing the pipeline steps,
    supporting local and cloud execution, branching, testing, and versioning.
    """
    def __init__(self, pipeline_path: str):
        self.pipeline_path = pipeline_path
        self.pipeline_config: Dict[str, Any] = {}
        self.last_output_alias: str = ""

        with open(pipeline_path, 'r') as f:
            self.pipeline_config = yaml.safe_load(f)

        if not self.pipeline_config.get('steps'):
            raise ValueError("Pipeline file must contain a 'steps' section.")

    def checkout(self, project_name: str, branch: str, hash_id: str):
        """
        Simulates `devlake checkout <hash_id>` (REQ-006).
        Swaps the current branch's data to point to the snapshot folder.
        """
        version_path = Path(project_name) / ".devlake/versions" / hash_id
        current_data_path = Path(project_name) / ".devlake/data" / branch

        if not version_path.exists():
            print(f"❌ Error: Data version '{hash_id}' not found.")
            return

        print(f"Checking out data version **{hash_id}**...")

        if current_data_path.exists():
            shutil.rmtree(current_data_path)

        shutil.copytree(version_path, current_data_path)

        print(f"✅ Branch '{branch}' now uses data from version '{hash_id}'.")
        print("Run `devlake console` to query the historical data.")

    def _execute_steps(self, project_name: str, executor: LocalExecutor):
        """Helper to execute the pipeline steps using the provided LocalExecutor."""
        last_df = None
        for i, step_def in enumerate(self.pipeline_config['steps']):
            step_name = list(step_def.keys())[0]
            step_details = step_def[step_name]
            print(f"\n[{i+1}/{len(self.pipeline_config['steps'])}] **{step_name.upper()}** Step...")

            if step_name == 'load':
                if 'csv' in step_details:
                    url = step_details['csv']
                    alias = step_details['alias']
                    print(f"  -> Loading CSV from: {url}")
                    df = pl.read_csv(url)
                    executor.register_dataframe(alias, df)
                    self.last_output_alias = alias
                    last_df = df

            elif step_name == 'transform':
                if 'sql' in step_details:
                    sql_query = step_details['sql']
                    output_alias = step_details['output_alias']
                    df_result = executor.query(sql_query)
                    executor.register_dataframe(output_alias, df_result)
                    self.last_output_alias = output_alias
                    last_df = df_result

                elif 'python' in step_details:
                    python_code = step_details['python']
                    output_alias = step_details.get('output_alias', self.last_output_alias + "_py_transformed")

                    input_df = executor.data_store.get(self.last_output_alias)
                    if input_df is None:
                        print(f"  -> ERROR: Cannot run Python transform. No input data found for alias: {self.last_output_alias}")
                        continue

                    print(f"  -> Running Python Polars transformation on **{self.last_output_alias}**...")
                    exec_context = {'pl': pl, 'df': input_df, 'transformed_df': None}
                    try:
                        exec(python_code, exec_context)
                        df_result = exec_context.get('transformed_df')
                        if isinstance(df_result, pl.DataFrame):
                            executor.register_dataframe(output_alias, df_result)
                            self.last_output_alias = output_alias
                            last_df = df_result
                        else:
                            print("  -> ERROR: Python code did not produce a Polars DataFrame named 'transformed_df'.")
                            continue
                    except Exception as e:
                        print(f"  -> ERROR during Python execution: {e}")
                        continue

            elif step_name == 'save':
                output_path = Path(project_name) / step_details['parquet']
                df_to_save = executor.data_store.get(self.last_output_alias)
                if df_to_save is None:
                    print(f"  -> ERROR: No data found to save for alias: {self.last_output_alias}")
                    continue
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df_to_save.write_parquet(output_path)
                print(f"  -> Data saved to **Parquet** at: {output_path}")

            else:
                print(f"  -> WARNING: Unknown step type '{step_name}'. Skipping.")
        return last_df

    def _create_snapshot(self, project_name: str, branch: str, data_hash: str):
        """Creates a data snapshot of the current branch's data."""
        current_data_path = Path(project_name) / ".devlake/data" / branch
        snapshot_path = Path(project_name) / ".devlake/versions" / data_hash

        if not snapshot_path.exists():
            print(f"\n📸 Creating data snapshot for version **{data_hash}**...")
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            if current_data_path.exists():
                shutil.copytree(current_data_path, snapshot_path)
                print("Snapshot created successfully. Use `devlake checkout` to revert.")
            else:
                print(f"  -> WARNING: No local data found at {current_data_path} to snapshot.")
        else:
            print(f"\n📸 Snapshot for version **{data_hash}** already exists. Skipping creation.")


    def _mock_cloud_snapshot(self, project_name: str, data_hash: str, target: str):
        """Mocks snapshotting cloud results by recording metadata."""
        snapshot_path = Path(project_name) / ".devlake/versions" / data_hash
        snapshot_path.mkdir(parents=True, exist_ok=True)

        metadata = {
            "source": f"Cloud Storage ({target.upper()})",
            "pipeline_hash": data_hash,
            "timestamp": time.time(),
            "output_uri": f"s3://devlake-output/{data_hash}/" if target == "aws" else f"gs://devlake-output/{data_hash}/"
        }

        with open(snapshot_path / "metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)

        print(f"📸 Cloud snapshot metadata recorded for version **{data_hash}**.")

    def run(self, project_name: str, branch: str = "main",
            target: str = "local", size: str = "medium", data_size_gb: float = 1.0):
        """
        Executes the pipeline either locally or on a cloud target (REQ-013).
        """
        data_hash = generate_pipeline_hash(self.pipeline_config)
        print(f"Current Pipeline Hash (Data Version ID): **{data_hash}**")

        if target == "local":
            executor = LocalExecutor(data_dir=Path(project_name) / ".devlake/data", branch=branch)
            print(f"\n--- Running Pipeline: {self.pipeline_config['name']} (Target: LOCAL) ---")

            last_df = self._execute_steps(project_name, executor)

            tests = self.pipeline_config.get('tests', [])
            if last_df is not None and tests:
                if run_tests(last_df, tests):
                    print("\n✅ Data Quality Tests Passed for pipeline output.")
                else:
                    print("\n❌ Data Quality Tests Failed. Pipeline output may be unreliable.")

            self._create_snapshot(project_name, branch, data_hash)
            executor.close()

        else: # Cloud execution
            cloud_executor = CloudExecutor(target=target, size=size)
            success = cloud_executor.dispatch_job(self.pipeline_config, data_size_gb)

            if success:
                print(f"--- Cloud execution simulated successfully. ---")
                self._mock_cloud_snapshot(project_name, data_hash, target)

        print("\n--- Pipeline Execution Complete ---")
