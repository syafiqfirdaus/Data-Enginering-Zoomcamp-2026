# Homework 05 Answers

1. Required files/directories: `.bruin.yml` and a `pipeline/` directory containing `pipeline.yml` and `assets/` (i.e. the pipeline spec must live under `pipeline/`).
2. Incremental strategy: `time_interval`
3. Override variable: `bruin run --var 'taxi_types=["yellow"]'`
4. Run downstream: `bruin run --select ingestion.trips+`
5. Quality check: `name: not_null`
6. Visualize graph: `bruin graph`
7. First-time flag: `--full-refresh`

> **Running the pipeline**
> 
> Windows PowerShell:
> ```powershell
> cd C:\Users\muhds\.gemini\antigravity\scratch\Data-Enginering-Zoomcamp-2026\05-data-platform
> bruin run --full-refresh
> ```
> Ubuntu/WSL:
> ```bash
> cd /mnt/c/Users/muhds/.gemini/antigravity/scratch/Data-Enginering-Zoomcamp-2026/05-data-platform
> bruin run --full-refresh
> ```

(Submitted via form: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw5)
