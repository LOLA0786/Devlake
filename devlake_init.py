
    import os
    from pathlib import Path
    import yaml
    from textwrap import dedent

    def run_init(project_name: str):
        """Implements the logic for `devlake init <project_name>` (REQ-003)."""
        print(f"Initializing DevLake project: **{project_name}**")

        # 1. Create project root directory
        project_path = Path(project_name)
        if project_path.exists():
            print(f"Error: Directory '{project_name}' already exists.")
            return
        project_path.mkdir(exist_ok=True)
        os.chdir(project_name) # Change to the new project directory

        # 2. Define the project structure
        structure = [
            "pipelines",
            "src",
            "tests",
            ".devlake",  # Local state (Gitignored)
            ".github/workflows", # CI templates
        ]

        # Create subdirectories
        for d in structure:
            Path(d).mkdir(parents=True, exist_ok=True)

        # 3. Generate devlake.yaml (Project Config)
        config_content = {
            'project_name': project_name,
            'version': '0.1.0',
            'default_engine': 'local',
            'local_storage': '.devlake/data',
        }
        with open("devlake.yaml", "w") as f:
            yaml.dump(config_content, f, sort_keys=False)

        # 4. Create .gitignore
        gitignore_content = dedent("""
            # DevLake Local State (REQ-003)
            .devlake/

            # Python
            __pycache__/
            *.pyc

            # Output
            /data/
            /output/
        """)
        with open(".gitignore", "w") as f:
            f.write(gitignore_content)

        # 5. Create a sample pipeline YAML (REQ-003, REQ-004)
        sample_pipeline_content = dedent("""
            # examples/quickstart/pipeline.yaml
            name: quickstart
            version: 1
            triggers:
              - schedule: "manual"
            steps:
              - load:
                  csv: "https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv"
                  alias: airports

              - transform:
                  sql: "SELECT iso_country, COUNT(*) as airport_count FROM airports GROUP BY iso_country ORDER BY 2 DESC"
                  output_alias: country_counts

              - save:
                  parquet: "./output/country_counts.parquet"
            tests:
              - assert_no_null: iso_country
        """)
        Path("pipelines/quickstart.yaml").write_text(sample_pipeline_content)

        print("
✅ Project structure created:")
        print(f"{project_name}/")
        print("├── devlake.yaml")
        print("├── pipelines/quickstart.yaml")
        print("├── src/")
        print("├── tests/")
        print("├── .devlake/ (ignored)")
        print("└── .github/workflows/")
        print(f"
Next: Run your first pipeline with `devlake run pipelines/quickstart.yaml --local`")

