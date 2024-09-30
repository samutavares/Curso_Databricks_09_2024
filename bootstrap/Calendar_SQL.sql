-- Databricks notebook source
WITH _d  AS (SELECT EXPLODE(SEQUENCE(DATE'2024-09-01', DATE'2024-01-10' - INTERVAL 1 DAY)) AS c_date)
SELECT c_date,
       EXTRACT(YEAR FROM c_date) AS c_year,
       EXTRACT(MONTH FROM c_date) AS c_month,
       EXTRACT(DAY FROM c_date) AS c_day
 FROM _d
