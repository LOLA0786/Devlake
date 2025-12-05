
    import duckdb
    import polars as pl
    from pathlib import Path
    from typing import Union, Dict, Any, List
    import time
    import random
    from textwrap import dedent

    class LocalExecutor:
        """Core local execution engine using DuckDB (SQL) and Polars (DataFrames), supporting branching."""
        def __init__(self, data_dir: Union[str, Path] = ".devlake/data", branch: str = "main"):
            self.branch = branch
            branch_dir = Path(data_dir) / self.branch
            branch_dir.mkdir(parents=True, exist_ok=True)
            db_path = branch_dir / "devlake.duckdb"

            self.duckdb = duckdb.connect(database=str(db_path))
            self.data_store: Dict[str, pl.DataFrame] = {} # Polars data cache for save/polars ops
            print(f"✅ LocalExecutor initialized. Branch: **{self.branch}**. Database: {db_path}")

        def register_dataframe(self, name: str, df: pl.DataFrame):
            """Registers a Polars DataFrame as a virtual table in DuckDB."""
            self.duckdb.register(name, df.to_arrow())
            self.data_store[name] = df
            print(f"  -> Registered table **{name}** ({len(df)} rows).")

        def query(self, sql: str) -> pl.DataFrame:
            """Executes SQL and returns the result as a Polars DataFrame."""
            try:
                arrow_table = self.duckdb.execute(sql).fetch_arrow_table()
                df_result = pl.from_arrow(arrow_table)
                return df_result
            except Exception as e:
                print(f"  -> ERROR executing query: {e}")
                raise

        def get_catalog_schema(self) -> Dict[str, Dict[str, str]]:
            """
            Retrieves the catalog and schema information (REQ-008, REQ-018).
            Used by the CLI for auto-completion and the VS Code extension for IntelliSense.
            """
            schema = {}
            tables_query = self.duckdb.execute("SHOW TABLES;").fetch_df()['name'].tolist()

            for table_name in tables_query:
                try:
                    table_schema = self.duckdb.execute(f"PRAGMA table_info('{table_name}')").fetch_df()
                    columns = {row['name']: row['type'] for index, row in table_schema.iterrows()}
                    schema[table_name] = columns
                except Exception:
                    continue
            return schema

        def close(self):
            self.duckdb.close()
            print(f"**LocalExecutor** shut down for branch '{self.branch}'.")


    class CloudExecutor:
        """
        Simulates sending the pipeline job to a Cloud-Agnostic target (REQ-013).
        """
        def __init__(self, target: str, size: str):
            self.target = target.lower()
            self.size = size.lower()

            self.config = {
                "aws": {"service": "AWS Glue/EMR", "time_multiplier": 0.5},
                "azure": {"service": "Azure Synapse/HDInsight", "time_multiplier": 0.6},
                "gcp": {"service": "GCP Dataproc/Cloud Run", "time_multiplier": 0.45},
            }

        def dispatch_job(self, pipeline_config: Dict[str, Any], data_size_gb: float):
            """Mocks the deployment and execution process."""
            target_info = self.config.get(self.target)
            if not target_info:
                print(f"❌ Error: Cloud target '{self.target}' is not supported yet.")
                return False

            num_steps = len(pipeline_config.get('steps', []))

            estimated_time_min = (num_steps * 0.2) + (data_size_gb * target_info["time_multiplier"])
            estimated_time_sec = estimated_time_min * 60

            print(f"
--- 🚀 Deploying to Cloud Target: {target_info['service']} ({self.target.upper()}) ---")
            print(f"  Configuration: Size='{self.size}', Steps={num_steps}, Data={data_size_gb} GB")
            print(f"  Estimated Runtime: {estimated_time_min:.2f} minutes")

            setup_delay = random.uniform(5, 15)
            print(f"  [1/3] Setting up Cloud Environment... (Simulated {setup_delay:.1f}s delay)")
            time.sleep(1)

            print(f"  [2/3] Submitting pipeline to {target_info['service']}...")
            print("  Job ID: devlake-cloud-run-" + str(random.randint(1000, 9999)))
            time.sleep(1)

            print(f"  [3/3] Monitoring Execution...")
            print(f"  (Simulating {estimated_time_sec:.1f} seconds of processing...)")
            time.sleep(1)

            actual_runtime = estimated_time_min * random.uniform(0.9, 1.1)
            print(f"  Job Finished in **{actual_runtime:.2f} minutes**.")
            print(f"--- ✅ Cloud Job Complete: Data written to cloud storage. ---")
            return True
