-- ===============================================================
-- OndatraSQL masking macros
--
-- Used by @column directives to mask sensitive data during
-- materialization. Tags like mask_email, mask_ssn, hash_pii
-- in @column trigger these macros automatically.
--
-- Convention: tag name = macro name.
--   @column: email = Contact email | mask_email | PII
--   => calls mask_email(email) during materialization
--
-- Edit freely -- this file is yours.
-- ===============================================================

-- Mask email: keeps first char + domain, hides the rest
CREATE OR REPLACE MACRO mask_email(val) AS
  regexp_replace(val::VARCHAR, '(.).*@', '\1***@');

-- Mask SSN: keeps last 4 digits, hides the rest
CREATE OR REPLACE MACRO mask_ssn(val) AS
  '***-**-' || right(val::VARCHAR, 4);

-- Hash PII: any value => SHA256 hash (irreversible)
CREATE OR REPLACE MACRO hash_pii(val) AS
  sha256(val::VARCHAR);

-- Redact: any value => '[REDACTED]'
CREATE OR REPLACE MACRO redact(val) AS
  '[REDACTED]';
