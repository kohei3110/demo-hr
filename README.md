# Microsoft Fabric × Power BI — HR demo scaffolding

This repo generates synthetic HR datasets (CSV) designed for a Microsoft Fabric Lakehouse + Power BI demo.

## What you get

Running the generator produces **9 CSV files** in `data/bronze/`:

- `ats_candidates.csv` (PK: `candidate_id`)
- `hr_employees.csv` (PK: `employee_id`)
- `hr_transfers.csv` (PK: `employee_id`, `start_date`)
- `perf_reviews.csv` (PK: `employee_id`, `review_period`)
- `lms_courses.csv` (PK: `course_id`)
- `lms_enrollments.csv` (PK: `employee_id`, `course_id`, `enroll_date`)
- `survey_responses.csv` (PK: `employee_id`, `survey_date`)
- `org_departments.csv` (PK: `dept_id`)
- `org_managers.csv` (PK: `manager_id`)

The synthetic data includes **showcase causal rules**:

- Higher manager `coaching_factor` (in `org_managers.csv`) → higher performance `rating` (in `perf_reviews.csv`)
- Manager-level `rating_bias` (in `org_managers.csv`) affects `rating`
- More LMS enrollments (see `lms_enrollments.csv`, last ~6 months) → higher `rating`
- Ratings may dip shortly after a transfer (see `hr_transfers.csv` assignment start dates)

## Requirements

- Python 3.10+
- Dependencies: pandas, numpy, faker

Install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Generate demo data

Default scale (recommended): employees=2500, newgrad_ratio=0.78

```bash
python generate_demo_data.py --seed 123
```

Outputs go to:

- `data/bronze/*.csv`

Useful options:

- `--outdir` (default `data/bronze`)
- `--employees` (default `2500`)
- `--start-year` (default current_year-3)
- `--end-year` (default current_year)
- `--newgrad-ratio` (default `0.78`)

All date fields are ISO format: `yyyy-mm-dd`.

## Load into Fabric Lakehouse (high level)

1. Create a Lakehouse in Microsoft Fabric.
2. Upload the CSVs from `data/bronze/` into the Lakehouse **Files** area.
3. Use a Spark notebook (Spark SQL) to create Bronze Delta tables from the CSVs in Files.
   (The Lakehouse SQL endpoint is often read-only for table DDL.)
4. Use the templates in `sql/` to build Silver/Gold tables (CTAS).
5. Connect Power BI to the Lakehouse (Direct Lake or Import) and build visuals.

SQL templates (Spark SQL):

- `sql/bronze_ddl_template.sql`
- `sql/silver_ctas_template.sql`
- `sql/gold_ctas_template.sql`

Power BI page guidance:

- `docs/powerbi_wireframe.md`

## Run tests

```bash
pytest
```
