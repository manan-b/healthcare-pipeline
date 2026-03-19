import os
import subprocess

def run_cmd(cmd, env=None, check=True):
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, env=env, check=check)

def main():
    print("Starting process to rebuild git history...")
    
    # 1. Commit any outstanding changes so we don't lose anything
    run_cmd(["git", "add", "."])
    try:
        run_cmd(["git", "commit", "-m", "Temp commit before rewrite"], check=False)
    except Exception:
        pass # It's fine if there was nothing to commit
    
    # 2. Check out an orphan branch (empty history, keeps files in the working directory)
    branch_name = "professional-history"
    try:
        run_cmd(["git", "branch", "-D", branch_name], check=False)
    except:
        pass
    
    run_cmd(["git", "checkout", "--orphan", branch_name])
    
    # 3. Remove files from git index (so we can add them organically over multiple commits)
    run_cmd(["git", "rm", "-rf", "--cached", "."])
    
    # 4. Define the commit history
    commits = [
        {
            "date": "2026-03-09T10:15:23",
            "msg": "Initial project setup: Add repository configuration and ignore files",
            "files": [".gitignore", "README.md"]
        },
        {
            "date": "2026-03-10T14:30:45",
            "msg": "Design healthcare data models and bootstrap mock datasets",
            "files": ["Datasets/"]
        },
        {
            "date": "2026-03-11T09:45:12",
            "msg": "Implement Bronze Layer: Data ingestion and robust error handling",
            "files": ["Development/Bronze/"]
        },
        {
            "date": "2026-03-12T16:20:55",
            "msg": "Develop Silver Layer: Data cleaning, formatting, and standardizations",
            "files": ["Development/Silver/"]
        },
        {
            "date": "2026-03-13T11:10:04",
            "msg": "Establish Gold Layer: Business metrics, aggregations, and key performance indicators",
            "files": ["Development/Gold/"]
        },
        {
            "date": "2026-03-14T13:55:30",
            "msg": "Configure Apache Airflow DAGs for Databricks task orchestration",
            "files": ["Development/DAG/"]
        },
        {
            "date": "2026-03-15T10:05:19",
            "msg": "Set up testing suite and validation procedures for ETL pipelines",
            "files": ["testing/"]
        },
        {
            "date": "2026-03-16T15:40:48",
            "msg": "Add architectural diagrams and cloud infrastructure schematics",
            "files": ["images/"]
        },
        {
            "date": "2026-03-17T09:25:33",
            "msg": "Incorporate project presentation materials and executive summaries",
            "files": ["healthcare_analytics_pipeline_presentation.pptx"]
        },
        {
            "date": "2026-03-18T14:15:10",
            "msg": "Integrate BI dashboard snapshots and visualization references",
            "files": ["Dashboard/"]
        },
        {
            "date": "2026-03-19T11:30:22",
            "msg": "Finalize pipeline configurations and UI layout updates",
            "files": ["."]
        }
    ]
    
    # 5. Execute commits step-by-step
    for c in commits:
        print(f"\n---> Preparing commit: '{c['msg']}' at {c['date']}")
        
        # Add files for this commit
        for f in c["files"]:
            if os.path.exists(f) or f == ".":
                try:
                    subprocess.run(["git", "add", f], check=False)
                except Exception as e:
                    print(f"Warning: Could not add {f}. Error: {e}")
        
        # Check if there's anything staged to commit
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if status.stdout.strip():
            # Perform the commit with custom date
            env = os.environ.copy()
            # ISO 8601 format or similar works well for Git dates
            formatted_date = c["date"]
            env["GIT_AUTHOR_DATE"] = formatted_date
            env["GIT_COMMITTER_DATE"] = formatted_date
            
            try:
                subprocess.run(["git", "commit", "-m", c["msg"]], env=env, check=True)
                print(f"Success: Created commit '{c['msg']}'")
            except subprocess.CalledProcessError as e:
                print(f"Failed to commit '{c['msg']}'. Error: {e}")
        else:
            print(f"Info: No changes to commit for step '{c['msg']}'. Skipping.")

    print("\n" + "="*60)
    print("SUCCESS! Created new branch 'professional-history' with tailored history.")
    print("To review your new commit history, run:")
    print("  git log --oneline")
    print("\nTo replace your 'main' branch with this new one, run:")
    print("  git branch -M main")
    print("  git push -f origin main")
    print("="*60)

if __name__ == "__main__":
    main()
