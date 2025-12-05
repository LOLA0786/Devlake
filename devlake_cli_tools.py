
    import sys
    import math
    import os
    import random
    import time
    import json
    import shutil
    import zipfile
    from pathlib import Path
    from textwrap import dedent
    import polars as pl
    import duckdb # For ConsoleRunner's LocalExecutor
    from typing import Union, Dict, Any, List

    from .devlake_executors import LocalExecutor


    class ConsoleRunner:
        """
        Simulates the `devlake console` command (REQ-008).
        Provides a REPL interface for SQL against the embedded DuckDB instance.
        """
        def __init__(self, project_name: str):
            self.executor = LocalExecutor(data_dir=Path(project_name) / ".devlake/data")
            print("
--- Starting DevLake Console ---")
            print("💡 Available tables are registered during pipeline runs (e.g., 'airports', 'country_counts').")
            print("Type SQL and press Enter to execute. Type 'exit' to quit.")

        def start(self):
            while True:
                try:
                    command = input("devlake (sql)> ").strip()
                    if command.lower() == 'exit':
                        print("Exiting console. Goodbye!")
                        break
                    if not command:
                        continue

                    if command.lower().startswith('select') or command.lower().startswith('show'):
                        result_df = self.executor.query(command)
                        print("
--- Query Result (Polars DataFrame) ---")
                        print(result_df.head(5))
                        print(f"Total rows: {len(result_df)}
")
                    else:
                        print("Console only supports SQL SELECT/SHOW commands for now.")
                except Exception as e:
                    print(f"An error occurred: {e}")

        def close(self):
            self.executor.close()


    class CostEstimator:
        """
        Simulates the logic for `devlake estimate` (REQ-014, Section 11).
        Calculates estimated time and cost for different execution targets.
        """
        def __init__(self, pipeline_config: Dict[str, Any], data_size_gb: float = 1.0):
            self.pipeline_config = pipeline_config
            self.data_size_gb = data_size_gb
            self.num_steps = len(pipeline_config.get('steps', []))

            self.models = {
                "local": {
                    "time_factor_steps": 0.3,
                    "time_factor_data": 1.5,
                    "cost_rate": 0.00,
                    "name": "Local execution"
                },
                "aws_glue": {
                    "time_factor_steps": 0.1,
                    "time_factor_data": 0.5,
                    "cost_rate": 0.02,
                    "name": "AWS Glue (EMR)"
                },
                "aws_lambda": {
                    "time_factor_steps": 0.2,
                    "time_factor_data": 0.1,
                    "cost_rate": 0.0075,
                    "name": "AWS Lambda"
                }
            }

        def estimate(self, target: str) -> Dict[str, Union[float, str]]:
            model = self.models.get(target)
            if not model:
                return {"error": "Invalid target"}

            time_estimate_min = (self.num_steps * model["time_factor_steps"]) +                                 (self.data_size_gb * model["time_factor_data"])
            cost_estimate = time_estimate_min * model["cost_rate"]

            return {
                "target": model["name"],
                "time_min": time_estimate_min,
                "cost": cost_estimate
            }

        def run_estimation_report(self, cloud_targets: List[str]):
            print(f"
--- 💰 DevLake Cloud Cost Estimation Report (REQ-014) ---")
            print(f"Pipeline: {self.pipeline_config['name']}")
            print(f"Complexity: {self.num_steps} Steps, {self.data_size_gb} GB Data
")

            results = []
            all_targets = ['local'] + cloud_targets
            for target in all_targets:
                results.append(self.estimate(target))

            min_cost = min(r['cost'] for r in results)

            print(f"{'Target':<20} | {'Time (min)':<12} | {'Cost (USD)':<12} | {'Recommendation'}")
            print("-" * 60)

            recommendation_target = ""
            for result in results:
                time_str = f"{result['time_min']:.2f}"
                cost_str = f"${result['cost']:.2f}"
                is_cheapest = result['cost'] == min_cost
                rec = ""
                if result['target'] == "Local execution":
                    rec = "Baseline (Free & Fast Cold Start)"
                elif is_cheapest:
                    rec = "CHEAPEST OPTION"
                    recommendation_target = result['target']
                print(f"{result['target']:<20} | {time_str:<12} | {cost_str:<12} | {rec}")

            if recommendation_target:
                 print(f"
Recommendation: Use **{recommendation_target}** for the most cost-effective run.")


    class JobMonitor:
        """
        Mocks the state tracking needed for `devlake monitor`.
        """
        def __init__(self):
            self.jobs = [
                {"id": "dl-1001", "name": "user_analytics", "target": "aws_glue", "status": "COMPLETED", "runtime": "12.5s"},
                {"id": "dl-1002", "name": "quickstart", "target": "local", "status": "FAILED", "runtime": "8.1s"},
                {"id": "dl-1003", "name": "dashboard_prep", "target": "gcp", "status": "RUNNING", "runtime": "2m 3s"},
                {"id": "dl-1004", "name": "experiment_branch", "target": "local", "status": "PENDING", "runtime": "-"},
            ]

        def monitor(self):
            print("
--- 📊 DevLake Monitor (REQ-019) ---")
            print(f"{'ID':<10} | {'Pipeline':<20} | {'Target':<10} | {'Status':<10} | {'Runtime'}")
            print("-" * 65)
            for job in self.jobs:
                print(f"{job['id']:<10} | {job['name']:<20} | {job['target']:<10} | {job['status']:<10} | {job['runtime']}")

        def debug_job(self, job_id: str):
            job = next((j for j in self.jobs if j['id'] == job_id), None)
            if job is None:
                print(f"❌ Error: Job ID {job_id} not found.")
                return

            print(f"
--- 🐛 Debug Report for Job {job_id} ---")
            print(f"Status: **{job['status']}**")
            print(f"Pipeline: {job['name']} on {job['target']}")

            if job['status'] == 'FAILED':
                print("
**FAILURE CAUSE (Mock Log Snippet):**")
                print("> ERROR: Column 'user_id' not found in table 'raw_users'.")
                print("> Failure occurred during SQL transformation step 2.")
                print("
Suggested Action: Check pipeline YAML source in `pipelines/user_analytics.yaml`.")
            elif job['status'] == 'RUNNING':
                print("
**CURRENT STATUS:**")
                print(f"> Running Step 3 of 5 (Aggregating metrics). CPU usage: 85%.")
            else:
                print("
Job completed successfully. No major issues detected.")


    class ShareablePackager:
        """
        Implements the logic for `devlake share` (REQ-021).
        Packages the project configuration, pipelines, and data samples into a zip file.
        """
        def __init__(self, project_name: str, branch: str = "main"):
            self.project_path = Path(project_name)
            self.branch = branch
            self.output_dir = self.project_path / ".devlake/share"
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.package_name = f"devlake-share-{self.branch}-{int(time.time())}.zip"
            self.package_path = self.output_dir / self.package_name

        def _create_sample_data_folder(self, temp_dir: Path):
            """Mocks sampling the current branch's data for portability."""
            sample_data_path = temp_dir / ".devlake/data" / self.branch
            sample_data_path.mkdir(parents=True, exist_ok=True)

            branch_data_path = self.project_path / ".devlake/data" / self.branch
            duckdb_file = branch_data_path / "devlake.duckdb"

            if duckdb_file.exists():
                shutil.copy(duckdb_file, sample_data_path / "devlake.duckdb")
                print(f"  -> Included data snapshot for branch **{self.branch}**.")
            else:
                print("  -> WARNING: No persistent DuckDB file found to sample. Sharing code only.")

        def create_package(self):
            temp_dir = Path(f"devlake_staging_temp_{random.randint(100, 999)}")
            if temp_dir.exists(): shutil.rmtree(temp_dir)
            temp_dir.mkdir()

            shutil.copy(self.project_path / "devlake.yaml", temp_dir / "devlake.yaml")
            shutil.copytree(self.project_path / "pipelines", temp_dir / "pipelines")

            self._create_sample_data_folder(temp_dir)

            zip_base_name = str(self.package_path).rsplit('.', 1)[0]
            shutil.make_archive(
                base_name=zip_base_name,
                format='zip',
                root_dir=temp_dir
            )

            final_path = self.output_dir / self.package_name
            os.rename(str(zip_base_name) + ".zip", final_path)

            shutil.rmtree(temp_dir)
            return final_path

        def generate_share_url(self, package_path: Path) -> str:
            unique_id = package_path.name.replace('.zip', '').split('-')[-1]
            mock_url = f"https://devlake.io/share/pipeline/{unique_id}/?access=readonly"

            print(f"
--- 🔗 Shareable Environment URL (REQ-021) ---")
            print("Upload Status: **Uploaded and compressed.**")
            print(f"Share this URL with collaborators for read-only access:")
            print(f"{mock_url}")
            print("
*Collaborator Instructions:*")
            print("1. Collaborator runs: `devlake import-share --url {url}`")
            print("2. The local environment is initialized with the shared data and code.")
            return mock_url


    class MarketplaceClient:
        """
        Mocks the interaction with the public registry for `devlake import` (REQ-022).
        """
        def __init__(self, project_name: str):
            self.project_name = project_name
            self.pipelines_path = Path(project_name) / "pipelines"

            self.registry = {
                "pipeline/transform_github_data": {
                    "version": "v1.2.0",
                    "yaml_content": dedent("""
                        # pipeline/transform_github_data.yaml
                        name: github_transform_v1
                        version: 1
                        triggers:
                          - schedule: "0 0 * * *"
                        inputs:
                          - type: git
                            repo: ${{ input.repo_url }}
                            branch: main
                        transformations:
                          - sql: |
                              SELECT
                                author_email,
                                COUNT(DISTINCT commit_hash) as commit_count
                              FROM github_commits
                              GROUP BY 1
                              HAVING commit_count > 5
                            output_alias: top_contributors
                        output:
                          type: delta
                          path: ./data/processed/github_metrics
                    """)
                },
                "pipeline/s3_to_parquet": {
                    "version": "v0.9.1",
                    "yaml_content": dedent("""
                        # pipeline/s3_to_parquet.yaml
                        name: s3_ingestion
                        version: 1
                        inputs:
                          - type: s3
                            uri: s3://${{ input.bucket }}/${{ input.prefix }}
                            format: csv
                        transformations:
                          - python: |
                              transformed_df = df.filter(pl.col("is_valid") == True)
                        output:
                          type: parquet
                          path: ./data/raw/clean_output
                    """)
                }
            }

        def import_pipeline(self, registry_path: str):
            print(f"
--- 📥 Importing Pipeline from Marketplace (REQ-022) ---")
            pipeline_info = self.registry.get(registry_path)

            if not pipeline_info:
                print(f"❌ Error: Pipeline '{registry_path}' not found in the registry.")
                return

            file_name = registry_path.split('/')[-1] + ".yaml"
            target_path = self.pipelines_path / file_name

            if target_path.exists():
                 print(f"⚠️ Warning: Pipeline file '{file_name}' already exists. Skipping import.")
                 return

            try:
                target_path.write_text(pipeline_info['yaml_content'])
                print(f"✅ Successfully imported '{registry_path}' (Version: {pipeline_info['version']}).")
                print(f"File saved to: **{target_path}**")
                print("
Next step: Review the YAML and run with `devlake run`.")
            except Exception as e:
                print(f"❌ Error writing file: {e}")


    class BenchmarkRunner:
        """
        Simulates `devlake benchmark pipeline.yaml` (REQ-011).
        Measures performance and detects regressions against a historical baseline.
        """
        def __init__(self, project_name: str, pipeline_config: Dict[str, Any]):
            self.project_name = project_name
            self.pipeline_config = pipeline_config
            self.benchmark_dir = Path(project_name) / ".devlake/benchmarks"
            self.benchmark_dir.mkdir(parents=True, exist_ok=True)
            self.pipeline_name = pipeline_config['name']

        def _get_baseline_runtime(self) -> Dict[str, float]:
            baseline_file = self.benchmark_dir / f"{self.pipeline_name}_baseline.json"
            if baseline_file.exists():
                with open(baseline_file, 'r') as f:
                    return json.load(f)
            else:
                return {
                    "step_1_load": 5.0,
                    "step_2_transform": 15.0,
                    "step_3_save": 8.0
                }

        def run_benchmark(self):
            baseline = self._get_baseline_runtime()
            current_runtimes = {}
            total_regression = 0.0

            print(f"
--- ⏱️ Benchmarking Pipeline: {self.pipeline_name} (REQ-011) ---")

            for i, step_def in enumerate(self.pipeline_config['steps']):
                step_key = f"step_{i+1}_{list(step_def.keys())[0]}"
                base_time = baseline.get(step_key, 10.0)
                slowdown_factor = 1.0 + (0.15 * (1 if random.random() < 0.2 else 0))
                current_time = base_time * random.uniform(0.9, 1.1) * slowdown_factor
                current_runtimes[step_key] = current_time

                regression_ms = (current_time - base_time) * 1000
                status = "OK"
                if regression_ms > 0:
                    total_regression += regression_ms
                    status = f"REGRESSION ({regression_ms:.0f}ms)"
                print(f"  {step_key:<20}: {current_time:.2f}s (Baseline: {base_time:.2f}s) -> {status}")

            print("-" * 50)
            if total_regression > 0:
                print(f"❌ **PERFORMANCE ALERT:** Total regression of **{total_regression:.0f}ms** detected.")
                print("  Action: Flagging git commit. Review step 'step_2_transform'.")
            else:
                print("✅ Benchmark passed. No significant performance regression detected.")

            with open(self.benchmark_dir / f"{self.pipeline_name}_baseline.json", 'w') as f:
                json.dump(current_runtimes, f)
                print("  New baseline recorded.")


    class StorageClient:
        """
        Mocks the core Storage Abstraction Layer (REQ-016, REQ-017).
        Handles format negotiation and pushdown optimizations.
        """
        def __init__(self, target: str):
            self.target = target

        def read_optimized(self, uri: str, format: str, columns: List[str] = None, filters: str = None) -> pl.LazyFrame:
            print(f"
--- 🗄️ Storage Read Request ({format.upper()}) ---")
            print(f"  Source URI: {uri} (Target: {self.target.upper()})")

            if columns:
                print(f"  Optimization 1: **Column Pruning** - Reading only columns: {columns}")
            if filters:
                print(f"  Optimization 2: **Push-down Filters** - Applying filter '{filters}' at source.")

            mock_data = {
                col: random.sample(range(1000, 9999), 5) for col in (columns if columns else ["id", "default_col"])
            }
            return pl.LazyFrame(mock_data)

        def write_optimized(self, df: pl.DataFrame, path: str, format: str):
            print(f"
--- 💾 Storage Write Request ({format.upper()}) ---")
            print(f"  Destination Path: {path} (Type: {format})")

            if format.lower() in ["parquet", "delta", "iceberg"]:
                print("  Optimization: Leveraging Apache Arrow/Iceberg Rust for efficient, partitioned write.")
