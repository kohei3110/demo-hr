-- Bronze layer DDL template (Fabric Lakehouse, Spark SQL)
--
-- Note: these statements are intended for a Spark notebook. The Lakehouse SQL endpoint
-- uses a T-SQL dialect and may not support DDL for Lakehouse tables.
--
-- This file is a TEMPLATE. Copy/paste into a Fabric Lakehouse Spark notebook.
-- Do not execute blindly in automation.

-- Example: create schema/database as needed
-- CREATE SCHEMA IF NOT EXISTS bronze;

-- Departments
CREATE TABLE IF NOT EXISTS bronze.org_departments (
  dept_id STRING,
  dept_name_ja STRING,
  cost_center STRING,
  location STRING
) USING DELTA;

-- Managers
CREATE TABLE IF NOT EXISTS bronze.org_managers (
  manager_id STRING,
  management_years INT,
  rating_bias DOUBLE,
  team_attrition_rate DOUBLE,
  coaching_factor DOUBLE
) USING DELTA;

-- Employees
CREATE TABLE IF NOT EXISTS bronze.hr_employees (
  employee_id STRING,
  join_date DATE,
  is_newgrad BOOLEAN,
  gender STRING,
  university STRING,
  major STRING,
  initial_dept_id STRING,
  initial_level STRING,
  email STRING,
  employee_name STRING,
  status STRING
) USING DELTA;

-- Transfers / assignment periods
CREATE TABLE IF NOT EXISTS bronze.hr_transfers (
  employee_id STRING,
  dept_id STRING,
  manager_id STRING,
  start_date DATE,
  end_date DATE,
  move_reason STRING
) USING DELTA;

-- Performance reviews
CREATE TABLE IF NOT EXISTS bronze.perf_reviews (
  employee_id STRING,
  review_period STRING,
  dept_id STRING,
  manager_id STRING,
  rating DOUBLE,
  promoted BOOLEAN,
  comp_change_rate DOUBLE,
  manager_comment STRING
) USING DELTA;

-- LMS courses
CREATE TABLE IF NOT EXISTS bronze.lms_courses (
  course_id STRING,
  course_name STRING,
  category STRING,
  hours INT
) USING DELTA;

-- LMS enrollments
CREATE TABLE IF NOT EXISTS bronze.lms_enrollments (
  employee_id STRING,
  course_id STRING,
  enroll_date DATE,
  completed BOOLEAN,
  completion_date DATE
) USING DELTA;

-- Survey responses
CREATE TABLE IF NOT EXISTS bronze.survey_responses (
  employee_id STRING,
  survey_date DATE,
  engagement DOUBLE,
  trust_in_manager DOUBLE,
  psych_safety DOUBLE
) USING DELTA;

-- ATS candidates
CREATE TABLE IF NOT EXISTS bronze.ats_candidates (
  candidate_id STRING,
  join_date DATE,
  source STRING,
  interview_score DOUBLE,
  offer_made BOOLEAN,
  offer_accepted BOOLEAN,
  university STRING,
  major STRING
) USING DELTA;
