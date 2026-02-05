-- Silver layer CTAS template (Fabric Lakehouse, Spark SQL)
--
-- Goal: clean, typed, and conformed tables (e.g., date parsing, standard dims).
-- This file is a TEMPLATE.

-- Example patterns (adapt paths/table names to your environment):
-- CREATE SCHEMA IF NOT EXISTS silver;

-- Employees (conformed)
CREATE TABLE IF NOT EXISTS silver.employees USING DELTA AS
SELECT
  CAST(employee_id AS STRING) AS employee_id,
  CAST(join_date AS DATE) AS join_date,
  CAST(is_newgrad AS BOOLEAN) AS is_newgrad,
  CAST(gender AS STRING) AS gender,
  CAST(university AS STRING) AS university,
  CAST(major AS STRING) AS major,
  CAST(initial_dept_id AS STRING) AS initial_dept_id,
  CAST(initial_level AS STRING) AS initial_level,
  CAST(email AS STRING) AS email,
  CAST(employee_name AS STRING) AS employee_name,
  CAST(status AS STRING) AS status
FROM bronze.hr_employees;

-- Assignment periods
CREATE TABLE IF NOT EXISTS silver.employee_assignments USING DELTA AS
SELECT
  CAST(employee_id AS STRING) AS employee_id,
  CAST(dept_id AS STRING) AS dept_id,
  CAST(manager_id AS STRING) AS manager_id,
  CAST(start_date AS DATE) AS start_date,
  CAST(end_date AS DATE) AS end_date,
  CAST(move_reason AS STRING) AS move_reason
FROM bronze.hr_transfers;

-- Performance reviews
CREATE TABLE IF NOT EXISTS silver.performance_reviews USING DELTA AS
SELECT
  CAST(employee_id AS STRING) AS employee_id,
  CAST(review_period AS STRING) AS review_period,
  CAST(dept_id AS STRING) AS dept_id,
  CAST(manager_id AS STRING) AS manager_id,
  CAST(rating AS DOUBLE) AS rating,
  CAST(promoted AS BOOLEAN) AS promoted,
  CAST(comp_change_rate AS DOUBLE) AS comp_change_rate,
  CAST(manager_comment AS STRING) AS manager_comment
FROM bronze.perf_reviews;

-- LMS courses
CREATE TABLE IF NOT EXISTS silver.lms_courses USING DELTA AS
SELECT
  CAST(course_id AS STRING) AS course_id,
  CAST(course_name AS STRING) AS course_name,
  CAST(category AS STRING) AS category,
  CAST(hours AS INT) AS hours
FROM bronze.lms_courses;

-- LMS enrollments
CREATE TABLE IF NOT EXISTS silver.lms_enrollments USING DELTA AS
SELECT
  CAST(employee_id AS STRING) AS employee_id,
  CAST(course_id AS STRING) AS course_id,
  CAST(enroll_date AS DATE) AS enroll_date,
  CAST(completed AS BOOLEAN) AS completed,
  CAST(completion_date AS DATE) AS completion_date
FROM bronze.lms_enrollments;

-- Survey responses
CREATE TABLE IF NOT EXISTS silver.survey_responses USING DELTA AS
SELECT
  CAST(employee_id AS STRING) AS employee_id,
  CAST(survey_date AS DATE) AS survey_date,
  CAST(engagement AS DOUBLE) AS engagement,
  CAST(trust_in_manager AS DOUBLE) AS trust_in_manager,
  CAST(psych_safety AS DOUBLE) AS psych_safety
FROM bronze.survey_responses;

-- Departments
CREATE TABLE IF NOT EXISTS silver.org_departments USING DELTA AS
SELECT
  CAST(dept_id AS STRING) AS dept_id,
  CAST(dept_name_ja AS STRING) AS dept_name_ja,
  CAST(cost_center AS STRING) AS cost_center,
  CAST(location AS STRING) AS location
FROM bronze.org_departments;

-- Managers
CREATE TABLE IF NOT EXISTS silver.org_managers USING DELTA AS
SELECT
  CAST(manager_id AS STRING) AS manager_id,
  CAST(management_years AS INT) AS management_years,
  CAST(rating_bias AS DOUBLE) AS rating_bias,
  CAST(team_attrition_rate AS DOUBLE) AS team_attrition_rate,
  CAST(coaching_factor AS DOUBLE) AS coaching_factor
FROM bronze.org_managers;

-- ATS candidates
CREATE TABLE IF NOT EXISTS silver.ats_candidates USING DELTA AS
SELECT
  CAST(candidate_id AS STRING) AS candidate_id,
  CAST(join_date AS DATE) AS join_date,
  CAST(source AS STRING) AS source,
  CAST(interview_score AS DOUBLE) AS interview_score,
  CAST(offer_made AS BOOLEAN) AS offer_made,
  CAST(offer_accepted AS BOOLEAN) AS offer_accepted,
  CAST(university AS STRING) AS university,
  CAST(major AS STRING) AS major
FROM bronze.ats_candidates;
