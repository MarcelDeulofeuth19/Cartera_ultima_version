-- Migración: Agregar columna is_fixed a la tabla managements
-- Esta columna indica si un contrato aún es considerado como "fijo"
-- Valores: 1 = fijo activo, 0 = ya no es fijo (por expiración de condiciones)

-- Agregar columna is_fixed (si no existe)
ALTER TABLE alocreditindicators.managements 
ADD COLUMN IF NOT EXISTS is_fixed INTEGER DEFAULT 1;

-- Crear índice para mejorar el rendimiento de las consultas
CREATE INDEX IF NOT EXISTS idx_managements_is_fixed 
ON alocreditindicators.managements(is_fixed);

-- Comentarios para documentación
COMMENT ON COLUMN alocreditindicators.managements.is_fixed IS 
'Indica si el contrato es considerado fijo: 1=activo, 0=inactivo por expiración';

-- Actualizar todos los registros existentes a is_fixed=1 por defecto
UPDATE alocreditindicators.managements 
SET is_fixed = 1 
WHERE is_fixed IS NULL;
