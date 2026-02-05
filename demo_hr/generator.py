from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker


def _iso(d: date | None) -> str | None:
    if d is None:
        return None
    return d.isoformat()


def _parse_iso(s: str) -> date:
    # Internal utility; generator stores dates as yyyy-mm-dd strings.
    return datetime.strptime(s, "%Y-%m-%d").date()


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(min(hi, max(lo, x)))


def _sha_pseudo_email(employee_id: int) -> str:
    # Stable, deterministic emails without requiring Faker uniqueness.
    return f"{employee_id.lower()}@contoso.com"


def _month_range_every_2(month_start: date, month_end: date) -> list[date]:
    """Return month-start dates every 2 months within [month_start, month_end].

    We emit dates on the 1st of the month, stepping by 2 months. If month_start
    is not itself the 1st, we start from the next month boundary to avoid
    returning dates earlier than month_start.
    """
    if month_end < month_start:
        return []

    dates: list[date] = []

    cur = date(month_start.year, month_start.month, 1)
    if cur < month_start:
        # Move to next month boundary.
        nm = cur.month + 1
        ny = cur.year + (nm - 1) // 12
        nm = ((nm - 1) % 12) + 1
        cur = date(ny, nm, 1)

    while cur <= month_end:
        dates.append(cur)
        nm = cur.month + 2
        ny = cur.year + (nm - 1) // 12
        nm = ((nm - 1) % 12) + 1
        cur = date(ny, nm, 1)

    return dates


def _half_year_periods(start_year: int, end_year: int) -> list[tuple[str, date]]:
    """Return [(review_period, review_date)] from start_year..end_year inclusive."""
    periods: list[tuple[str, date]] = []
    for y in range(start_year, end_year + 1):
        periods.append((f"{y}-H1", date(y, 6, 30)))
        periods.append((f"{y}-H2", date(y, 12, 31)))
    return periods


def _weighted_choice(rng: np.random.Generator, items: np.ndarray, probs: np.ndarray):
    idx = rng.choice(len(items), p=probs)
    return items[idx]


@dataclass(frozen=True)
class GenerateConfig:
    seed: int | None = None
    outdir: Path = Path("data/bronze")
    employees: int = 2500
    start_year: int = date.today().year - 3
    end_year: int = date.today().year
    newgrad_ratio: float = 0.78


REQUIRED_FILES: tuple[str, ...] = (
    "ats_candidates.csv",
    "hr_employees.csv",
    "hr_transfers.csv",
    "perf_reviews.csv",
    "lms_courses.csv",
    "lms_enrollments.csv",
    "survey_responses.csv",
    "org_departments.csv",
    "org_managers.csv",
)


def generate_and_write(cfg: GenerateConfig) -> None:
    cfg.outdir.mkdir(parents=True, exist_ok=True)
    dfs = generate_all(cfg)
    for name, df in dfs.items():
        out_path = cfg.outdir / name
        # Ensure consistent column order and deterministic rows
        df.to_csv(out_path, index=False, na_rep="")


def generate_all(cfg: GenerateConfig) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(cfg.seed)
    faker = Faker("en_US")
    if cfg.seed is not None:
        faker.seed_instance(cfg.seed)

    depts = _gen_departments(rng)
    managers, mgr_by_dept = _gen_managers(rng, depts)
    employees, join_map, term_map, initial_mgr_map = _gen_employees(rng, faker, cfg, depts, mgr_by_dept)
    transfers = _gen_transfers(rng, cfg, employees, join_map, term_map, depts, mgr_by_dept, initial_mgr_map)
    courses = _gen_courses(rng)
    enrollments = _gen_enrollments(rng, cfg, employees, join_map, term_map, courses)
    surveys = _gen_surveys(rng, cfg, employees, join_map, term_map, transfers, managers)
    reviews = _gen_reviews(rng, cfg, employees, join_map, term_map, transfers, managers, enrollments)
    candidates = _gen_candidates(rng, cfg, employees)

    # Sort by key for deterministic file output.
    depts = depts.sort_values(["dept_id"], kind="mergesort").reset_index(drop=True)
    managers = managers.sort_values(["manager_id"], kind="mergesort").reset_index(drop=True)
    employees = employees.sort_values(["employee_id"], kind="mergesort").reset_index(drop=True)
    transfers = transfers.sort_values(["employee_id", "start_date"], kind="mergesort").reset_index(drop=True)
    courses = courses.sort_values(["course_id"], kind="mergesort").reset_index(drop=True)
    enrollments = enrollments.sort_values(["employee_id", "course_id", "enroll_date"], kind="mergesort").reset_index(drop=True)
    surveys = surveys.sort_values(["employee_id", "survey_date"], kind="mergesort").reset_index(drop=True)
    reviews = reviews.sort_values(["employee_id", "review_period"], kind="mergesort").reset_index(drop=True)
    candidates = candidates.sort_values(["candidate_id"], kind="mergesort").reset_index(drop=True)

    return {
        "ats_candidates.csv": candidates,
        "hr_employees.csv": employees,
        "hr_transfers.csv": transfers,
        "perf_reviews.csv": reviews,
        "lms_courses.csv": courses,
        "lms_enrollments.csv": enrollments,
        "survey_responses.csv": surveys,
        "org_departments.csv": depts,
        "org_managers.csv": managers,
    }


def _gen_departments(rng: np.random.Generator) -> pd.DataFrame:
    # Authoritative schema: dept_id, dept_name_ja, cost_center, location
    dept_names_ja = [
        "エンジニアリング",
        "営業",
        "マーケティング",
        "財務",
        "人事",
        "カスタマーサクセス",
        "IT",
        "オペレーション",
        "法務",
        "プロダクト",
        "セキュリティ",
        "データ",
    ]
    locations = np.array(["Tokyo", "Osaka", "Nagoya", "Fukuoka", "Remote"], dtype=object)
    rows = []
    for i, name_ja in enumerate(dept_names_ja, start=1):
        rows.append(
            {
                "dept_id": f"D{i:03d}",
                "dept_name_ja": name_ja,
                "cost_center": f"CC{i:04d}",
                "location": str(rng.choice(locations)),
            }
        )
    return pd.DataFrame(rows, columns=["dept_id", "dept_name_ja", "cost_center", "location"])


def _gen_managers(
    rng: np.random.Generator,
    depts: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    """Generate manager dimension + internal mapping from dept -> manager_ids.

    Output schema must match authoritative DDL for org_managers.csv:
      manager_id STRING, management_years INT, rating_bias DOUBLE,
      team_attrition_rate DOUBLE, coaching_factor DOUBLE
    """

    n_managers = 220
    dept_ids = depts["dept_id"].to_numpy(dtype=object)
    rows = []
    mgr_by_dept: dict[str, list[str]] = {str(d): [] for d in dept_ids}

    for i in range(1, n_managers + 1):
        manager_id = f"M{i:04d}"
        home_dept = str(rng.choice(dept_ids))
        coaching = float(rng.beta(3.0, 2.0))  # skew high-ish
        bias = float(_clamp(rng.normal(0.0, 0.18), -0.45, 0.45))
        mgmt_years = int(_clamp(rng.normal(8.0, 5.5), 0, 30))
        # Attrition rate is a team outcome; loosely anti-correlated with coaching.
        team_attr = float(_clamp(0.22 - 0.18 * coaching + rng.normal(0, 0.06), 0.02, 0.55))

        mgr_by_dept[home_dept].append(manager_id)
        rows.append(
            {
                "manager_id": manager_id,
                "management_years": mgmt_years,
                "rating_bias": round(bias, 3),
                "team_attrition_rate": round(team_attr, 3),
                "coaching_factor": round(coaching, 3),
            }
        )

    df = pd.DataFrame(
        rows,
        columns=["manager_id", "management_years", "rating_bias", "team_attrition_rate", "coaching_factor"],
    )
    return df, mgr_by_dept


def _gen_employees(
    rng: np.random.Generator,
    faker: Faker,
    cfg: GenerateConfig,
    depts: pd.DataFrame,
    mgr_by_dept: dict[str, list[str]],
) -> tuple[pd.DataFrame, dict[str, date], dict[str, date | None], dict[str, str]]:
    """Generate employees with authoritative schema.

    hr_employees.csv columns:
      employee_id STRING, join_date DATE, is_newgrad BOOLEAN, gender STRING,
      university STRING, major STRING, initial_dept_id STRING, initial_level STRING,
      email STRING, employee_name STRING, status STRING

    Returns:
      (employees_df, join_map, term_map, initial_manager_map)
    """

    start = date(cfg.start_year - 1, 1, 1)
    end = date(cfg.end_year, 12, 31)

    dept_ids = depts["dept_id"].to_numpy(dtype=object)
    genders = np.array(["F", "M", "X"], dtype=object)
    universities = np.array(
        [
            "University of Tokyo",
            "Kyoto University",
            "Osaka University",
            "Tohoku University",
            "Waseda University",
            "Keio University",
            "Hokkaido University",
            "Nagoya University",
        ],
        dtype=object,
    )
    majors = np.array(
        [
            "Computer Science",
            "Information Systems",
            "Economics",
            "Business",
            "Mathematics",
            "Statistics",
            "Psychology",
            "Mechanical Engineering",
        ],
        dtype=object,
    )

    join_map: dict[str, date] = {}
    term_map: dict[str, date | None] = {}
    initial_mgr_map: dict[str, str] = {}

    rows = []
    for i in range(1, cfg.employees + 1):
        employee_id = f"E{i:06d}"
        is_new = rng.random() < cfg.newgrad_ratio

        if is_new:
            join_min = date(max(cfg.start_year, cfg.end_year - 2), 1, 1)
        else:
            join_min = start

        join_days = (end - join_min).days
        join_date = join_min + timedelta(days=int(rng.integers(0, max(1, join_days + 1))))

        initial_dept_id = str(rng.choice(dept_ids))
        # Choose an initial manager from the dept pool; if pool empty, fall back to any known manager.
        mgr_pool = mgr_by_dept.get(initial_dept_id) or [m for ms in mgr_by_dept.values() for m in ms]
        initial_manager_id = str(rng.choice(np.array(mgr_pool, dtype=object)))

        # Small attrition.
        terminated = (rng.random() < 0.06) and (join_date < date(cfg.end_year, 10, 1))
        term_date: date | None = None
        if terminated:
            min_term = join_date + timedelta(days=60)
            max_term = date(cfg.end_year, 12, 31)
            if min_term < max_term:
                term_date = min_term + timedelta(days=int(rng.integers(0, (max_term - min_term).days + 1)))

        employee_name = faker.name()
        rows.append(
            {
                "employee_id": employee_id,
                "join_date": _iso(join_date),
                "is_newgrad": bool(is_new),
                "gender": str(rng.choice(genders)),
                "university": str(rng.choice(universities)),
                "major": str(rng.choice(majors)),
                "initial_dept_id": initial_dept_id,
                "initial_level": "NG" if is_new else str(rng.choice(["IC2", "IC3", "IC4", "M1"])),
                "email": _sha_pseudo_email(employee_id),
                "employee_name": employee_name,
                "status": "TERMINATED" if term_date is not None else "ACTIVE",
            }
        )

        join_map[employee_id] = join_date
        term_map[employee_id] = term_date
        initial_mgr_map[employee_id] = initial_manager_id

    df = pd.DataFrame(
        rows,
        columns=[
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
    )
    return df, join_map, term_map, initial_mgr_map


def _gen_transfers(
    rng: np.random.Generator,
    cfg: GenerateConfig,
    employees: pd.DataFrame,
    join_map: dict[str, date],
    term_map: dict[str, date | None],
    depts: pd.DataFrame,
    mgr_by_dept: dict[str, list[str]],
    initial_mgr_map: dict[str, str],
) -> pd.DataFrame:
    """Generate employee assignment periods.

    Notes:
    - We include the initial post-join assignment as the first row (move_reason=HIRE).
    - `end_date` is nullable for the current assignment (ACTIVE employees).
    """

    dept_ids = depts["dept_id"].to_numpy(dtype=object)
    reasons = np.array(["Reorg", "Promotion", "Role Change", "Backfill", "Relocation"], dtype=object)

    rows: list[dict[str, object]] = []

    for r in employees.itertuples(index=False):
        eid = str(r.employee_id)
        join = join_map[eid]
        term = term_map.get(eid)
        horizon = term if term is not None else date(cfg.end_year, 12, 31)

        # Transfers 0-4 per person (after initial assignment)
        n_transfers = int(rng.integers(0, 5))
        total_days = (horizon - join).days
        max_events = min(n_transfers, max(0, total_days // 180))

        event_dates: list[date] = []
        if total_days > 120 and max_events > 0:
            day_choices = np.arange(90, max(91, total_days - 30))
            if len(day_choices) >= max_events:
                sampled = rng.choice(day_choices, size=max_events, replace=False)
                event_dates = sorted([join + timedelta(days=int(d)) for d in sampled])

        starts = [join] + event_dates

        cur_dept = str(r.initial_dept_id)
        cur_mgr = str(initial_mgr_map[eid])

        for i, s in enumerate(starts):
            next_start = starts[i + 1] if i + 1 < len(starts) else None
            if next_start is None:
                # Last period: open-ended for ACTIVE employees, else closed on termination.
                end_date = None if term is None else horizon
            else:
                end_date = next_start - timedelta(days=1)

            rows.append(
                {
                    "employee_id": eid,
                    "dept_id": cur_dept,
                    "manager_id": cur_mgr,
                    "start_date": _iso(s),
                    "end_date": _iso(end_date),
                    "move_reason": "HIRE" if i == 0 else str(rng.choice(reasons)),
                }
            )

            if next_start is not None:
                if rng.random() < 0.75:
                    other = dept_ids[dept_ids != cur_dept]
                    cur_dept = str(rng.choice(other)) if len(other) else cur_dept
                # else same department
                pool = mgr_by_dept.get(cur_dept) or [m for ms in mgr_by_dept.values() for m in ms]
                cur_mgr = str(rng.choice(np.array(pool, dtype=object)))

    transfers = pd.DataFrame(
        rows,
        columns=["employee_id", "dept_id", "manager_id", "start_date", "end_date", "move_reason"],
    )
    return transfers


def _gen_courses(rng: np.random.Generator) -> pd.DataFrame:
    categories = np.array(["Leadership", "Technical", "Compliance", "Productivity", "Security"], dtype=object)
    rows = []
    for i in range(1, 121):
        cat = str(rng.choice(categories))
        hours = int(rng.integers(1, 17))
        rows.append(
            {
                "course_id": f"C{i:03d}",
                "course_name": f"{cat} Course {i:03d}",
                "category": cat,
                "hours": hours,
            }
        )
    return pd.DataFrame(rows, columns=["course_id", "course_name", "category", "hours"])


def _gen_enrollments(
    rng: np.random.Generator,
    cfg: GenerateConfig,
    employees: pd.DataFrame,
    join_map: dict[str, date],
    term_map: dict[str, date | None],
    courses: pd.DataFrame,
) -> pd.DataFrame:
    course_ids = courses["course_id"].to_numpy(dtype=object)
    rows = []
    seen_keys: set[tuple[str, str, str]] = set()

    for e in employees.itertuples(index=False):
        eid = str(e.employee_id)
        join = join_map[eid]
        term = term_map.get(eid)
        horizon = term if term is not None else date(cfg.end_year, 12, 31)

        # LMS activity: heavier for new grads.
        lam = 4.0 if bool(e.is_newgrad) else 2.2
        n = int(_clamp(rng.poisson(lam), 0, 30))
        if n == 0:
            continue

        for _ in range(n):
            cid = str(rng.choice(course_ids))
            span = max(1, (horizon - join).days + 1)
            enroll_date = join + timedelta(days=int(rng.integers(0, span)))
            enroll_s = _iso(enroll_date)
            key = (eid, cid, str(enroll_s))
            if key in seen_keys:
                continue
            seen_keys.add(key)

            completed = bool(rng.random() < 0.9)
            completion: date | None = None
            if completed:
                completion = enroll_date + timedelta(days=int(rng.integers(2, 45)))
                if completion > horizon:
                    completion = horizon
            rows.append(
                {
                    "employee_id": eid,
                    "course_id": cid,
                    "enroll_date": enroll_s,
                    "completed": completed,
                    "completion_date": _iso(completion),
                }
            )

    return pd.DataFrame(rows, columns=["employee_id", "course_id", "enroll_date", "completed", "completion_date"])


def _assignment_asof(transfers: pd.DataFrame, employee_id: str, asof: date) -> tuple[str, str, date]:
    """Return (dept_id, manager_id, start_date_of_period) active at asof."""
    t = transfers[transfers["employee_id"] == employee_id]
    if t.empty:
        raise ValueError(f"No transfers/assignments for employee {employee_id}")

    # transfers are non-overlapping; pick the latest start_date <= asof.
    tt = t.copy()
    tt["start_date_dt"] = pd.to_datetime(tt["start_date"], format="%Y-%m-%d").dt.date
    tt["end_date_dt"] = pd.to_datetime(tt["end_date"].replace({"": None}), format="%Y-%m-%d").dt.date
    tt = tt.sort_values(["start_date_dt"], kind="mergesort")

    active = tt[tt["start_date_dt"] <= asof]
    row = (active.tail(1) if not active.empty else tt.head(1)).iloc[0]
    return str(row.dept_id), str(row.manager_id), _parse_iso(str(row.start_date))


def _gen_surveys(
    rng: np.random.Generator,
    cfg: GenerateConfig,
    employees: pd.DataFrame,
    join_map: dict[str, date],
    term_map: dict[str, date | None],
    transfers: pd.DataFrame,
    managers: pd.DataFrame,
) -> pd.DataFrame:
    mgr_map = managers.set_index("manager_id")
    start = date(cfg.start_year, 1, 1)
    end = date(cfg.end_year, 12, 1)
    survey_dates = _month_range_every_2(start, end)

    rows = []
    for e in employees.itertuples(index=False):
        eid = str(e.employee_id)
        join = join_map[eid]
        term = term_map.get(eid)

        for sd in survey_dates:
            if sd < join:
                continue
            if term is not None and sd > term:
                continue
            if rng.random() > 0.65:
                continue

            _dept_id, manager_id, _period_start = _assignment_asof(transfers, eid, sd)
            coaching = float(mgr_map.loc[manager_id, "coaching_factor"])

            engagement = _clamp(55 + 22 * coaching + rng.normal(0, 10), 1, 100)
            trust = _clamp(50 + 35 * coaching + rng.normal(0, 9), 1, 100)
            psych = _clamp(48 + 38 * coaching + rng.normal(0, 9), 1, 100)
            rows.append(
                {
                    "employee_id": eid,
                    "survey_date": _iso(sd),
                    "engagement": round(float(engagement), 1),
                    "trust_in_manager": round(float(trust), 1),
                    "psych_safety": round(float(psych), 1),
                }
            )
    return pd.DataFrame(
        rows,
        columns=["employee_id", "survey_date", "engagement", "trust_in_manager", "psych_safety"],
    )


def _gen_reviews(
    rng: np.random.Generator,
    cfg: GenerateConfig,
    employees: pd.DataFrame,
    join_map: dict[str, date],
    term_map: dict[str, date | None],
    transfers: pd.DataFrame,
    managers: pd.DataFrame,
    enrollments: pd.DataFrame,
) -> pd.DataFrame:
    periods = _half_year_periods(cfg.start_year, cfg.end_year)
    mgr_map = managers.set_index("manager_id")

    if enrollments.empty:
        enrollments = pd.DataFrame(columns=["employee_id", "enroll_date"])  # guard

    enr = enrollments.copy()
    if not enr.empty:
        enr["enroll_date_dt"] = pd.to_datetime(enr["enroll_date"], format="%Y-%m-%d").dt.date

    rows = []
    for e in employees.itertuples(index=False):
        eid = str(e.employee_id)
        join = join_map[eid]
        term = term_map.get(eid)

        e_enr = enr[enr["employee_id"] == eid] if not enr.empty else enr

        for review_period, review_date in periods:
            if review_date < join:
                continue
            if term is not None and review_date > term:
                continue

            dept_id, manager_id, period_start = _assignment_asof(transfers, eid, review_date)
            coaching = float(mgr_map.loc[manager_id, "coaching_factor"])
            bias = float(mgr_map.loc[manager_id, "rating_bias"])

            # LMS enrollments in last ~6 months.
            l6_start = review_date - timedelta(days=183)
            if not e_enr.empty:
                lms_6m = int(((e_enr["enroll_date_dt"] >= l6_start) & (e_enr["enroll_date_dt"] <= review_date)).sum())
            else:
                lms_6m = 0
            # Transfer dip if the active assignment started recently (excluding hire).
            days_since_assignment = (review_date - period_start).days
            transfer_recent = (period_start > join) and (days_since_assignment <= 60)
            dip = -0.35 if transfer_recent else 0.0

            # Showcase causal rules:
            # - Higher coaching_factor -> higher rating
            # - More LMS enrollments -> higher rating
            # - Immediately after transfer -> dip
            # - Manager rating_bias affects rating
            base = 3.0
            rating = (
                base
                + 0.95 * (coaching - 0.5)
                + 0.18 * np.log1p(lms_6m)
                + bias
                + dip
                + rng.normal(0, 0.25)
            )
            rating = round(_clamp(float(rating), 1.0, 5.0), 1)

            promoted = bool((rating >= 4.3) and (rng.random() < 0.20))
            comp_change = (
                0.02
                + 0.03 * (rating - 3.0)
                + (0.06 if promoted else 0.0)
                + rng.normal(0, 0.01)
            )
            comp_change = round(_clamp(float(comp_change), -0.08, 0.25), 3)

            if rating >= 4.5:
                comment = "Exceeds expectations; strong impact and execution."
            elif rating >= 3.6:
                comment = "Solid performance; keep building momentum and sharing wins."
            elif rating >= 2.8:
                comment = "Meets some expectations; focus on prioritization and follow-through."
            else:
                comment = "Performance below expectations; needs support and a clear improvement plan."

            if transfer_recent:
                comment = f"Recent move adjustment noted. {comment}"
            if lms_6m == 0 and rating < 3.4:
                comment = f"Consider targeted learning via LMS. {comment}"

            rows.append(
                {
                    "employee_id": eid,
                    "review_period": review_period,
                    "dept_id": dept_id,
                    "manager_id": manager_id,
                    "rating": rating,
                    "promoted": promoted,
                    "comp_change_rate": comp_change,
                    "manager_comment": comment,
                }
            )
    return pd.DataFrame(
        rows,
        columns=[
            "employee_id",
            "review_period",
            "dept_id",
            "manager_id",
            "rating",
            "promoted",
            "comp_change_rate",
            "manager_comment",
        ],
    )


def _gen_candidates(
    rng: np.random.Generator,
    cfg: GenerateConfig,
    employees: pd.DataFrame,
) -> pd.DataFrame:
    # Generate an ATS pipeline.
    n = int(max(cfg.employees * 3, 7000))
    sources = np.array(["LinkedIn", "Referral", "Campus", "Agency", "Website"], dtype=object)
    universities = np.array(
        [
            "University of Tokyo",
            "Kyoto University",
            "Osaka University",
            "Waseda University",
            "Keio University",
            "Hokkaido University",
            "Nagoya University",
        ],
        dtype=object,
    )
    majors = np.array(
        [
            "Computer Science",
            "Information Systems",
            "Economics",
            "Business",
            "Mathematics",
            "Statistics",
            "Psychology",
        ],
        dtype=object,
    )

    start = date(cfg.start_year - 1, 1, 1)
    end = date(cfg.end_year, 12, 31)
    span = (end - start).days + 1

    rows = []
    for i in range(1, n + 1):
        join_date = start + timedelta(days=int(rng.integers(0, span)))
        source = str(rng.choice(sources))

        # Score in [0,100], slightly right-skewed.
        score = float(_clamp(rng.normal(68, 12) + 8 * rng.beta(2, 5), 0, 100))
        score = round(score, 1)

        offer_made = bool(score >= 72 and rng.random() < 0.65)
        offer_accepted = bool(offer_made and (rng.random() < _clamp(0.35 + 0.006 * (score - 70), 0.05, 0.90)))

        rows.append(
            {
                "candidate_id": f"CAND{i:07d}",
                "join_date": _iso(join_date),
                "source": source,
                "interview_score": score,
                "offer_made": offer_made,
                "offer_accepted": offer_accepted,
                "university": str(rng.choice(universities)),
                "major": str(rng.choice(majors)),
            }
        )

    return pd.DataFrame(
        rows,
        columns=[
            "candidate_id",
            "join_date",
            "source",
            "interview_score",
            "offer_made",
            "offer_accepted",
            "university",
            "major",
        ],
    )
