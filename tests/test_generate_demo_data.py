from __future__ import annotations

import hashlib
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd


FILES = {
    # Authoritative DDL (must match exactly: names + semantics)
    "ats_candidates.csv": {
        "pk": ["candidate_id"],
        "columns": [
            "candidate_id",
            "join_date",
            "source",
            "interview_score",
            "offer_made",
            "offer_accepted",
            "university",
            "major",
        ],
        "date_cols": ["join_date"],
        "nullable_date_cols": [],
        "numeric_cols": ["interview_score"],
        "bool_cols": ["offer_made", "offer_accepted"],
        "int_cols": [],
    },
    "hr_employees.csv": {
        "pk": ["employee_id"],
        "columns": [
            "employee_id",
            "join_date",
            "is_newgrad",
            "gender",
            "university",
            "major",
            "initial_dept_id",
            "initial_level",
            "email",
            "employee_name",
            "status",
        ],
        "date_cols": ["join_date"],
        "nullable_date_cols": [],
        "numeric_cols": [],
        "bool_cols": ["is_newgrad"],
        "int_cols": [],
    },
    "hr_transfers.csv": {
        "pk": ["employee_id", "start_date"],
        "columns": [
            "employee_id",
            "dept_id",
            "manager_id",
            "start_date",
            "end_date",
            "move_reason",
        ],
        "date_cols": ["start_date", "end_date"],
        "nullable_date_cols": ["end_date"],
        "numeric_cols": [],
        "bool_cols": [],
        "int_cols": [],
    },
    "perf_reviews.csv": {
        "pk": ["employee_id", "review_period"],
        "columns": [
            "employee_id",
            "review_period",
            "dept_id",
            "manager_id",
            "rating",
            "promoted",
            "comp_change_rate",
            "manager_comment",
        ],
        "date_cols": [],
        "nullable_date_cols": [],
        "numeric_cols": ["rating", "comp_change_rate"],
        "bool_cols": ["promoted"],
        "int_cols": [],
    },
    "lms_courses.csv": {
        "pk": ["course_id"],
        "columns": ["course_id", "course_name", "category", "hours"],
        "date_cols": [],
        "nullable_date_cols": [],
        "numeric_cols": [],
        "bool_cols": [],
        "int_cols": ["hours"],
    },
    "lms_enrollments.csv": {
        "pk": ["employee_id", "course_id", "enroll_date"],
        "columns": [
            "employee_id",
            "course_id",
            "enroll_date",
            "completed",
            "completion_date",
        ],
        "date_cols": ["enroll_date", "completion_date"],
        "nullable_date_cols": ["completion_date"],
        "numeric_cols": [],
        "bool_cols": ["completed"],
        "int_cols": [],
    },
    "survey_responses.csv": {
        "pk": ["employee_id", "survey_date"],
        "columns": [
            "employee_id",
            "survey_date",
            "engagement",
            "trust_in_manager",
            "psych_safety",
        ],
        "date_cols": ["survey_date"],
        "nullable_date_cols": [],
        "numeric_cols": ["engagement", "trust_in_manager", "psych_safety"],
        "bool_cols": [],
        "int_cols": [],
    },
    "org_departments.csv": {
        "pk": ["dept_id"],
        "columns": ["dept_id", "dept_name_ja", "cost_center", "location"],
        "date_cols": [],
        "nullable_date_cols": [],
        "numeric_cols": [],
        "bool_cols": [],
        "int_cols": [],
    },
    "org_managers.csv": {
        "pk": ["manager_id"],
        "columns": [
            "manager_id",
            "management_years",
            "rating_bias",
            "team_attrition_rate",
            "coaching_factor",
        ],
        "date_cols": [],
        "nullable_date_cols": [],
        "numeric_cols": ["rating_bias", "team_attrition_rate", "coaching_factor"],
        "bool_cols": [],
        "int_cols": ["management_years"],
    },
}


ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _run_generator(outdir: Path, seed: int, employees: int = 300) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "generate_demo_data.py"
    subprocess.check_call(
        [
            sys.executable,
            str(script),
            "--seed",
            str(seed),
            "--outdir",
            str(outdir),
            "--employees",
            str(employees),
        ]
    )


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _hash_sorted_keys(df: pd.DataFrame, key_cols: list[str]) -> str:
    k = df[key_cols].astype(str).sort_values(key_cols, kind="mergesort")
    blob = ("\n".join("|".join(row) for row in k.to_numpy())).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def test_generator_creates_all_csvs_and_required_columns(tmp_path: Path) -> None:
    outdir = tmp_path / "bronze"
    _run_generator(outdir, seed=123, employees=250)

    for fname, spec in FILES.items():
        fpath = outdir / fname
        assert fpath.exists(), f"Missing {fname}"
        df = _read_csv(fpath)
        assert list(df.columns) == spec["columns"], (
            f"{fname} schema mismatch.\n"
            f"Expected columns: {spec['columns']}\n"
            f"Actual columns:   {list(df.columns)}"
        )

        nullable_dates = set(spec.get("nullable_date_cols", []))
        for col in spec["date_cols"]:
            vals = df[col].astype(str)
            if col in nullable_dates:
                bad = vals[(vals != "") & (~vals.str.match(ISO))]
            else:
                bad = vals[(~vals.str.match(ISO))]
            assert bad.empty, f"{fname}.{col} contains non-ISO dates: {bad.head(3).to_list()}"

        for col in spec.get("bool_cols", []):
            vals = df[col].astype(str)
            bad = vals[~vals.isin(["True", "False"])]
            assert bad.empty, f"{fname}.{col} contains non-boolean values: {bad.head(3).to_list()}"

        for col in spec.get("int_cols", []):
            vals = df[col].astype(str)
            bad = vals[(vals != "") & (~vals.str.fullmatch(r"-?\d+"))]
            assert bad.empty, f"{fname}.{col} contains non-int values: {bad.head(3).to_list()}"

        for col in spec.get("numeric_cols", []):
            vals = df[col].astype(str)
            # allow empty for nullable numerics (none in this spec currently)
            def _is_floatish(x: str) -> bool:
                if x == "":
                    return False
                try:
                    float(x)
                    return True
                except ValueError:
                    return False

            bad = vals[~vals.map(_is_floatish)]
            assert bad.empty, f"{fname}.{col} contains non-numeric values: {bad.head(3).to_list()}"


def test_primary_key_uniqueness(tmp_path: Path) -> None:
    outdir = tmp_path / "bronze"
    _run_generator(outdir, seed=7, employees=400)

    for fname, spec in FILES.items():
        df = _read_csv(outdir / fname)
        pk = spec["pk"]
        dup = df.duplicated(subset=pk)
        assert not bool(dup.any()), f"Duplicate PKs in {fname} on {pk}"


def test_referential_integrity(tmp_path: Path) -> None:
    outdir = tmp_path / "bronze"
    _run_generator(outdir, seed=99, employees=350)

    employees = _read_csv(outdir / "hr_employees.csv")
    transfers = _read_csv(outdir / "hr_transfers.csv")
    reviews = _read_csv(outdir / "perf_reviews.csv")
    enrollments = _read_csv(outdir / "lms_enrollments.csv")
    surveys = _read_csv(outdir / "survey_responses.csv")
    courses = _read_csv(outdir / "lms_courses.csv")
    depts = _read_csv(outdir / "org_departments.csv")
    managers = _read_csv(outdir / "org_managers.csv")

    emp_ids = set(employees["employee_id"].astype(str))
    dept_ids = set(depts["dept_id"].astype(str))
    mgr_ids = set(managers["manager_id"].astype(str))
    course_ids = set(courses["course_id"].astype(str))

    # Employees reference departments via initial_dept_id.
    assert set(employees["initial_dept_id"]).issubset(dept_ids)

    assert set(transfers["employee_id"]).issubset(emp_ids)
    assert set(transfers["dept_id"]).issubset(dept_ids)
    assert set(transfers["manager_id"]).issubset(mgr_ids)

    assert set(reviews["employee_id"]).issubset(emp_ids)
    assert set(reviews["dept_id"]).issubset(dept_ids)
    assert set(reviews["manager_id"]).issubset(mgr_ids)

    assert set(enrollments["employee_id"]).issubset(emp_ids)
    assert set(enrollments["course_id"]).issubset(course_ids)

    assert set(surveys["employee_id"]).issubset(emp_ids)


def test_assignment_periods_valid_and_non_overlapping(tmp_path: Path) -> None:
    outdir = tmp_path / "bronze"
    _run_generator(outdir, seed=1234, employees=500)

    employees = pd.read_csv(outdir / "hr_employees.csv", dtype=str, keep_default_na=False)
    status_map = employees.set_index("employee_id")["status"].to_dict()

    transfers = pd.read_csv(outdir / "hr_transfers.csv", dtype=str, keep_default_na=False)
    transfers["start_date_dt"] = pd.to_datetime(transfers["start_date"]).dt.date
    transfers["end_date_dt"] = pd.to_datetime(transfers["end_date"].replace({"": None})).dt.date

    for eid, grp in transfers.groupby("employee_id"):
        g = grp.sort_values("start_date_dt", kind="mergesort")

        starts = g["start_date_dt"].to_list()
        ends = [None if pd.isna(x) else x for x in g["end_date_dt"].to_list()]

        for s, e in zip(starts, ends):
            if e is not None:
                assert s <= e, f"employee {eid}: start_date > end_date"

        for i in range(len(starts) - 1):
            # next start must be after current end (no overlap). If end is None, it must be last.
            if ends[i] is None:
                raise AssertionError(f"employee {eid}: null end_date not last period")
            assert ends[i] < starts[i + 1], f"employee {eid}: overlapping periods"

        # Allow end_date empty for the current assignment (last row only), and only for ACTIVE employees.
        if len(ends) > 0:
            is_active = status_map.get(str(eid), "").upper() == "ACTIVE"
            if is_active:
                assert ends[-1] is None, f"employee {eid}: ACTIVE employees must have empty end_date on last period"
            else:
                assert ends[-1] is not None, f"employee {eid}: non-ACTIVE employees must have end_date on last period"


def test_deterministic_output_with_seed(tmp_path: Path) -> None:
    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"
    _run_generator(out1, seed=2026, employees=220)
    _run_generator(out2, seed=2026, employees=220)

    for fname, spec in FILES.items():
        df1 = _read_csv(out1 / fname)
        df2 = _read_csv(out2 / fname)
        assert len(df1) == len(df2), f"Row count differs for {fname}"
        h1 = _hash_sorted_keys(df1, spec["pk"])
        h2 = _hash_sorted_keys(df2, spec["pk"])
        assert h1 == h2, f"Determinism hash differs for {fname}"


def _review_period_to_date(review_period: str):
    m = re.match(r"^(\d{4})-(H1|H2)$", str(review_period))
    if not m:
        raise ValueError(f"Invalid review_period: {review_period}")
    y = int(m.group(1))
    half = m.group(2)
    return pd.Timestamp(year=y, month=6, day=30).date() if half == "H1" else pd.Timestamp(year=y, month=12, day=31).date()


def test_causal_relationships_present(tmp_path: Path) -> None:
    """Validate the intended directionality (coaching/bias/LMS -> ratings, recent transfer dip).

    This is intentionally coarse-grained (correlation/sign tests) to avoid flaky tests.
    """
    outdir = tmp_path / "bronze"
    _run_generator(outdir, seed=31415, employees=1200)

    employees = pd.read_csv(outdir / "hr_employees.csv", dtype=str, keep_default_na=False)
    transfers = pd.read_csv(outdir / "hr_transfers.csv", dtype=str, keep_default_na=False)
    reviews = pd.read_csv(outdir / "perf_reviews.csv", dtype=str, keep_default_na=False)
    managers = pd.read_csv(outdir / "org_managers.csv", dtype=str, keep_default_na=False)
    enrollments = pd.read_csv(outdir / "lms_enrollments.csv", dtype=str, keep_default_na=False)

    reviews["rating_f"] = reviews["rating"].astype(float)
    managers["coaching_f"] = managers["coaching_factor"].astype(float)
    managers["bias_f"] = managers["rating_bias"].astype(float)

    # 1) Manager coaching_factor influences ratings (positive correlation on manager averages).
    mgr_avg = reviews.groupby("manager_id", as_index=False)["rating_f"].mean().rename(columns={"rating_f": "avg_rating"})
    mgr_avg = mgr_avg.merge(managers[["manager_id", "coaching_f", "bias_f"]], on="manager_id", how="inner")

    coach_corr = mgr_avg[["avg_rating", "coaching_f"]].corr(method="spearman").iloc[0, 1]
    assert coach_corr > 0.15, f"Expected positive coaching->rating correlation, got {coach_corr:.3f}"

    # 2) Manager rating_bias affects ratings (positive correlation on manager averages).
    bias_corr = mgr_avg[["avg_rating", "bias_f"]].corr(method="spearman").iloc[0, 1]
    assert bias_corr > 0.10, f"Expected positive bias->rating correlation, got {bias_corr:.3f}"

    # 3) LMS enrollment volume influences ratings (employee-period correlation).
    enrollments["enroll_dt"] = pd.to_datetime(enrollments["enroll_date"]).dt.date
    reviews["review_dt"] = reviews["review_period"].map(_review_period_to_date)

    # Count enrollments in the last ~6 months per employee per review.
    lms_counts = []
    for r in reviews.itertuples(index=False):
        rd = r.review_dt
        start = rd - pd.Timedelta(days=183)
        cnt = int(
            ((enrollments["employee_id"] == r.employee_id)
            & (enrollments["enroll_dt"] >= start)
            & (enrollments["enroll_dt"] <= rd)).sum()
        )
        lms_counts.append(cnt)
    reviews["lms_6m"] = lms_counts

    lms_corr = reviews[["rating_f", "lms_6m"]].corr(method="spearman").iloc[0, 1]
    assert lms_corr > 0.05, f"Expected positive LMS->rating correlation, got {lms_corr:.3f}"

    # 4) Ratings may dip shortly after transfer (within 60 days after non-initial move).
    transfers["start_dt"] = pd.to_datetime(transfers["start_date"]).dt.date
    transfers["end_dt"] = pd.to_datetime(transfers["end_date"].replace({"": None})).dt.date
    employees["join_dt"] = pd.to_datetime(employees["join_date"]).dt.date
    join_map = employees.set_index("employee_id")["join_dt"].to_dict()

    def _active_assignment_start(eid: str, asof):
        t = transfers[transfers["employee_id"] == eid].sort_values("start_dt", kind="mergesort")
        active = t[t["start_dt"] <= asof]
        row = (active.tail(1) if not active.empty else t.head(1)).iloc[0]
        return row.start_dt

    recent_flags = []
    for r in reviews.itertuples(index=False):
        eid = str(r.employee_id)
        rd = r.review_dt
        astart = _active_assignment_start(eid, rd)
        join = join_map[eid]
        recent = (astart > join) and ((rd - astart).days <= 60)
        recent_flags.append(bool(recent))
    reviews["transfer_recent"] = recent_flags

    recent = reviews[reviews["transfer_recent"]]
    steady = reviews[~reviews["transfer_recent"]]
    # Ensure we actually have both groups.
    assert len(recent) >= 50
    assert len(steady) >= 200
    dip = float(steady["rating_f"].mean() - recent["rating_f"].mean())
    assert dip > 0.05, f"Expected recent-transfer ratings to be lower; observed mean dip {dip:.3f}"
