
    import time
    import random
    from pathlib import Path
    import polars as pl
    from typing import Dict, Any

    from .devlake_pipeline import PipelineRunner, generate_pipeline_hash
    from .devlake_executors import LocalExecutor, CloudExecutor


    class HybridPipelineRunner(PipelineRunner):
        """
        Extends PipelineRunner to support step-level execution targets (REQ-015).
        """
        def __init__(self, pipeline_path: str):
            super().__init__(pipeline_path)
            self.local_executor = None

        def _get_executor(self, step_target: str, project_name: str, branch: str, size: str):
            """Returns the appropriate executor instance for the current step."""
            if step_target == 'local':
                if self.local_executor is None:
                    self.local_executor = LocalExecutor(data_dir=Path(project_name) / ".devlake/data", branch=branch)
                return self.local_executor
            else:
                return CloudExecutor(target=step_target, size=size)

        def run_hybrid(self, project_name: str, branch: str = "main", size: str = "medium", data_size_gb: float = 100.0):
            """
            Executes pipeline steps, dynamically switching between local and cloud executors.
            """
            data_hash = generate_pipeline_hash(self.pipeline_config)
            print(f"Current Pipeline Hash: **{data_hash}**")

            print(f"
--- 🌐 Running Pipeline: {self.pipeline_config['name']} (HYBRID Execution) ---")

            last_df = None

            for i, step_def in enumerate(self.pipeline_config['steps']):
                step_name = list(step_def.keys())[0]
                step_details = step_def[step_name]

                step_target = step_details.get('target', 'local').lower()

                print(f"
[{i+1}/{len(self.pipeline_config['steps'])}] **{step_name.upper()}** -> Target: **{step_target.upper()}**")

                executor = self._get_executor(step_target, project_name, branch, size)

                if step_target == 'local':
                    if step_name == 'load':
                        print("  -> Local LOAD: Reading small lookup table...")
                        # Mock data for local load step
                        last_df = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
                        executor.register_dataframe("lookup_table", last_df)
                        self.last_output_alias = "lookup_table"
                    elif step_name == 'save':
                        print("  -> Local SAVE: Writing final result to local disk...")
                        # This would normally save the last_output_alias DataFrame
                    else:
                        print("  -> Local TRANSFORM: Executing SQL/Python on local DuckDB...")
                        # Here, you would call self._execute_steps for local transform logic.
                        # For this mock, we assume it processes correctly.

                else: # Cloud Step Execution
                    if step_name == 'transform':
                        cloud_executor = executor
                        cloud_executor.dispatch_job(self.pipeline_config, data_size_gb)

                        print("  -> Cloud result ready. Downloading result metadata to local cache...")
                        self._mock_cloud_result_registration(self.local_executor, "large_result_table")
                        self.last_output_alias = "large_result_table" # Update alias for next step
                    else:
                        print(f"  -> WARNING: Cloud target not fully supported for step type: {step_name} in mock hybrid runner.")

            if self.local_executor:
                self.local_executor.close()

            print("
--- ✅ Hybrid Pipeline Execution Complete ---")

        def _mock_cloud_result_registration(self, local_executor: LocalExecutor, alias: str):
            """Simulates registering a pointer to the cloud result data into the local DuckDB."""
            metadata_df = pl.DataFrame({"cloud_path": ["s3://temp-results/job-1234"], "record_count": [100000000]})
            local_executor.register_dataframe(alias, metadata_df)
            self.last_output_alias = alias
