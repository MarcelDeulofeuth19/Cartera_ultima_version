-- Agrega campos de auditoria avanzada al historial de asignaciones
-- Tabla objetivo: alocreditindicators.contract_advisors_history

ALTER TABLE alocreditindicators.contract_advisors_history
    ADD COLUMN IF NOT EXISTS tipo VARCHAR(50);

ALTER TABLE alocreditindicators.contract_advisors_history
    ADD COLUMN IF NOT EXISTS dpd_inicial VARCHAR(20);

ALTER TABLE alocreditindicators.contract_advisors_history
    ADD COLUMN IF NOT EXISTS dpd_terminal VARCHAR(20);

ALTER TABLE alocreditindicators.contract_advisors_history
    ADD COLUMN IF NOT EXISTS dias_atraso_inicial INTEGER;

ALTER TABLE alocreditindicators.contract_advisors_history
    ADD COLUMN IF NOT EXISTS dias_atraso_terminal INTEGER;

CREATE INDEX IF NOT EXISTS idx_contract_advisors_history_tipo
    ON alocreditindicators.contract_advisors_history (tipo);

