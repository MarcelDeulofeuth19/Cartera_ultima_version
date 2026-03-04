-- Agrega campo de estado operativo actual en asignaciones activas
ALTER TABLE alocreditindicators.contract_advisors
    ADD COLUMN IF NOT EXISTS estado_actual VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_contract_advisors_estado_actual
    ON alocreditindicators.contract_advisors (estado_actual);
