-- ===============================================================
-- OndatraSQL constant variables
--
-- Literal values set once at init. Use for thresholds, rates,
-- and settings that differ per environment (dev vs prod).
--
-- Available everywhere via getvariable('name').
--
-- Edit freely -- this file is yours.
-- ===============================================================

-- Amount threshold for approaching-limit warnings.
-- Used by: ondatra_warning_approaching_limit
SET VARIABLE alert_amount_threshold = 1000000;

-- Example: tax and currency settings
-- SET VARIABLE vat_rate = 0.25;
-- SET VARIABLE default_currency = 'SEK';
