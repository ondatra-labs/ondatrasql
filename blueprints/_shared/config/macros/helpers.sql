-- ===============================================================
-- OndatraSQL helper macros
--
-- General-purpose functions available in any SQL model.
-- Use directly in SELECT, WHERE, or any SQL expression.
--
-- Edit freely -- this file is yours.
-- ===============================================================

-- Safe division -- returns NULL instead of error on divide by zero.
-- SELECT safe_divide(revenue, orders) AS avg_order
CREATE OR REPLACE MACRO safe_divide(a, b) AS
  CASE WHEN b = 0 THEN NULL ELSE a / b END;

-- Cents to dollars.
-- SELECT cents_to_dollars(amount_cents) AS amount
CREATE OR REPLACE MACRO cents_to_dollars(cents) AS
  cents / 100.0;
