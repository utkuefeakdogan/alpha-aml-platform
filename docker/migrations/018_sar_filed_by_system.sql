-- Auto-generated SARs (sar-worker) should show filed_by = System.

UPDATE aml.sar_reports
SET filed_by = 'System'
WHERE filed_by IS NULL OR TRIM(filed_by) = '';
