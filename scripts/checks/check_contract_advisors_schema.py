"""
Script para verificar el constraint UNIQUE de la tabla contract_advisors
"""
import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

async def check_contract_advisors_schema():
    """
    Verifica el esquema de container_advisors incluyendo constraints
    """
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DATABASE")
    port = int(os.getenv("POSTGRES_PORT", "5432"))

    missing = [
        key
        for key, value in {
            "POSTGRES_HOST": host,
            "POSTGRES_USER": user,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_DATABASE": database,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Faltan variables de entorno requeridas: " + ", ".join(missing)
        )

    conn = await asyncpg.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    
    try:
        print("=" * 80)
        print("ESQUEMA DE LA TABLA contract_advisors")
        print("=" * 80)
        
        # Query para obtener constraints
        query = """
        SELECT
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            tc.is_deferrable,
            tc.initially_deferred
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
        WHERE 
            tc.table_schema = 'alocreditindicators'
            AND tc.table_name = 'contract_advisors'
            AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
        ORDER BY tc.constraint_name;
        """
        
        constraints = await conn.fetch(query)
        
        print("\n🔒 CONSTRAINTS ÚNICOS:")
        for c in constraints:
            print(f"\n  Constraint: {c['constraint_name']}")
            print(f"    Tipo: {c['constraint_type']}")
            print(f"    Columna: {c['column_name']}")
            print(f"    Deferrable: {c['is_deferrable']}")
            print(f"    Initially Deferred: {c['initially_deferred']}")
        
        # Query para verificar si hay contratos duplicados
        query_duplicates = """
        SELECT contract_id, COUNT(*) as count, array_agg(user_id) as users
        FROM alocreditindicators.contract_advisors
        WHERE contract_id = 41985
        GROUP BY contract_id;
        """
        
        duplicates = await conn.fetch(query_duplicates)
        
        print(f"\n\n🔍 ANÁLISIS DEL CONTRATO 41985:")
        if duplicates:
            for d in duplicates:
                print(f"  Contract ID: {d['contract_id']}")
                print(f"  Asignaciones: {d['count']}")
                print(f"  Usuarios: {d['users']}")
        else:
            print("  No encontrado en contract_advisors")
        
        # Verificar índices
        query_indexes = """
        SELECT
            i.relname as index_name,
            a.attname as column_name,
            ix.indisunique as is_unique
        FROM
            pg_class t,
            pg_class i,
            pg_index ix,
            pg_attribute a,
            pg_namespace n
        WHERE
            t.oid = ix.indrelid
            AND i.oid = ix.indexrelid
            AND a.attrelid = t.oid
            AND a.attnum = ANY(ix.indkey)
            AND t.relkind = 'r'
            AND n.oid = t.relnamespace
            AND n.nspname = 'alocreditindicators'
            AND t.relname = 'contract_advisors'
        ORDER BY i.relname, a.attnum;
        """
        
        indexes = await conn.fetch(query_indexes)
        
        print(f"\n\n📑 ÍNDICES:")
        for idx in indexes:
            unique_str = "UNIQUE" if idx['is_unique'] else "NON-UNIQUE"
            print(f"  {idx['index_name']} ({unique_str}) -> {idx['column_name']}")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(check_contract_advisors_schema())
