-- Gold layer CTAS template (Fabric Lakehouse, Spark SQL)
--
-- Goal: business-ready star schema / aggregates for Power BI.
-- This file is a TEMPLATE.

-- CREATE SCHEMA IF NOT EXISTS gold;

-- Example: employee performance fact (semiannual)
CREATE TABLE IF NOT EXISTS gold.fact_employee_performance USING DELTA AS
SELECT
  pr.employee_id,
  pr.review_period,
  CASE
    WHEN pr.review_period LIKE '%-H1' THEN TO_DATE(CONCAT(SUBSTR(pr.review_period, 1, 4), '-06-30'))
    WHEN pr.review_period LIKE '%-H2' THEN TO_DATE(CONCAT(SUBSTR(pr.review_period, 1, 4), '-12-31'))
    ELSE NULL
  END AS review_date,
  pr.dept_id,
  pr.manager_id,
  pr.rating,
  pr.promoted,
  pr.comp_change_rate,
  pr.manager_comment
FROM silver.performance_reviews pr;

-- Example: manager rollup
CREATE TABLE IF NOT EXISTS gold.fact_manager_performance USING DELTA AS
SELECT
  manager_id,
  review_period,
  AVG(rating) AS avg_rating,
  AVG(CASE WHEN promoted THEN 1.0 ELSE 0.0 END) AS promotion_rate,
  AVG(comp_change_rate) AS avg_comp_change_rate
FROM silver.performance_reviews
GROUP BY manager_id, review_period;
