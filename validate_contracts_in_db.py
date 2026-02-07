"""
Script de validación de contratos fijos contra base de datos.
Compara los contratos definidos en el código con los registros en BD.
"""

import asyncio
import os
from datetime import datetime
from sqlalchemy import select, create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

# Configuración de bases de datos
MYSQL_URL = "mysql+aiomysql://alo_estadisticas:4K9ml8e2vqlj@57.130.40.1:3306/alocreditprod"
POSTGRES_URL = "postgresql+asyncpg://nexus_dev_84:ZehK7wQTpq95eU8r@3.95.195.63:5432/nexus_db"

Base = declarative_base()

# Engines
mysql_engine = create_async_engine(MYSQL_URL, pool_pre_ping=True, pool_recycle=3600, echo=False)
postgres_engine = create_async_engine(POSTGRES_URL, pool_pre_ping=True, pool_recycle=3600, echo=False)

# Session makers
MySQLSession = async_sessionmaker(mysql_engine, class_=AsyncSession, expire_on_commit=False)
PostgresSession = async_sessionmaker(postgres_engine, class_=AsyncSession, expire_on_commit=False)

# Importar contratos fijos
from app.data.manual_fixed_contracts import (
    COBYSER_MANUAL_FIXED,
    SERLEFIN_MANUAL_FIXED
)


async def validate_contracts_in_mysql(contract_ids: list[int], session: AsyncSession) -> dict:
    """Valida qué contratos existen en la base de datos MySQL de contratos."""
    
    result = {
        "total_contracts": len(contract_ids),
        "found_in_db": [],
        "not_found_in_db": [],
        "contract_details": {}
    }
    
    # Consultar en lotes de 1000
    batch_size = 1000
    for i in range(0, len(contract_ids), batch_size):
        batch = contract_ids[i:i + batch_size]
        
        # Query SQL directa usando la tabla real 'contract'
        ids_str = ','.join(map(str, batch))
        query = text(f"""
            SELECT c.id
            FROM contract c
            WHERE c.id IN ({ids_str})
        """)
        
        db_result = await session.execute(query)
        contracts = db_result.fetchall()
        
        found_ids = []
        for contract in contracts:
            contract_id = contract[0]
            found_ids.append(contract_id)
            result["contract_details"][contract_id] = {
                "id": contract_id,
                "status": "Encontrado en MySQL"
            }
        
        result["found_in_db"].extend(found_ids)
    
    # Identificar contratos no encontrados
    found_set = set(result["found_in_db"])
    result["not_found_in_db"] = [cid for cid in contract_ids if cid not in found_set]
    
    return result


async def validate_contracts_in_postgres(contract_ids: list[int], user_id: int, session: AsyncSession) -> dict:
    """Valida qué contratos ya están asignados en PostgreSQL."""
    
    result = {
        "total_contracts": len(contract_ids),
        "already_assigned": [],
        "not_assigned": [],
        "assignment_details": {}
    }
    
    # Consultar en lotes
    batch_size = 1000
    for i in range(0, len(contract_ids), batch_size):
        batch = contract_ids[i:i + batch_size]
        
        ids_str = ','.join(map(str, batch))
        query = text(f"""
            SELECT contract_id, user_id
            FROM alocreditindicators.contract_advisors 
            WHERE contract_id IN ({ids_str}) AND user_id = {user_id}
        """)
        
        db_result = await session.execute(query)
        assignments = db_result.fetchall()
        
        assigned_ids = []
        for assignment in assignments:
            contract_id = assignment[0]
            assigned_ids.append(contract_id)
            result["assignment_details"][contract_id] = {
                "contract_id": contract_id,
                "user_id": assignment[1],
                "status": "Ya asignado en PostgreSQL"
            }
        
        result["already_assigned"].extend(assigned_ids)
    
    # Identificar contratos no asignados
    assigned_set = set(result["already_assigned"])
    result["not_assigned"] = [cid for cid in contract_ids if cid not in assigned_set]
    
    return result


async def generate_validation_report():
    """Genera un reporte completo de validación."""
    
    print("=" * 100)
    print("VALIDACIÓN DE CONTRATOS FIJOS - CÓDIGO vs BASE DE DATOS")
    print("=" * 100)
    print(f"Fecha de validación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    
    async with MySQLSession() as mysql_session:
        async with PostgresSession() as postgres_session:
            
            # Validar Cobyser (Usuario 45)
            print(f"\n{'='*100}")
            print("COBYSER - Usuario 45")
            print(f"{'='*100}")
            print(f"Total de contratos en código: {len(COBYSER_MANUAL_FIXED)}")
            
            # Validar en MySQL
            print("\n[1/2] Validando existencia en MySQL (contratos)...")
            cobyser_mysql = await validate_contracts_in_mysql(COBYSER_MANUAL_FIXED, mysql_session)
            print(f"  ✓ Encontrados en MySQL: {len(cobyser_mysql['found_in_db'])}")
            print(f"  ✗ No encontrados en MySQL: {len(cobyser_mysql['not_found_in_db'])}")
            
            if cobyser_mysql['not_found_in_db']:
                print(f"\n  Contratos NO encontrados en MySQL (primeros 20):")
                for contract_id in cobyser_mysql['not_found_in_db'][:20]:
                    print(f"    - {contract_id}")
                if len(cobyser_mysql['not_found_in_db']) > 20:
                    print(f"    ... y {len(cobyser_mysql['not_found_in_db']) - 20} más")
            
            # Validar en PostgreSQL
            print("\n[2/2] Validando asignaciones en PostgreSQL (contract_advisors)...")
            cobyser_postgres = await validate_contracts_in_postgres(COBYSER_MANUAL_FIXED, 45, postgres_session)
            print(f"  ✓ Ya asignados: {len(cobyser_postgres['already_assigned'])}")
            print(f"  ✗ No asignados: {len(cobyser_postgres['not_assigned'])}")
            
            if cobyser_postgres['already_assigned']:
                print(f"\n  Contratos YA asignados (primeros 10):")
                for contract_id in cobyser_postgres['already_assigned'][:10]:
                    print(f"    - {contract_id}")
                if len(cobyser_postgres['already_assigned']) > 10:
                    print(f"    ... y {len(cobyser_postgres['already_assigned']) - 10} más")
            
            # Validar Serlefin (Usuario 81)
            print(f"\n{'='*100}")
            print("SERLEFIN - Usuario 81")
            print(f"{'='*100}")
            print(f"Total de contratos en código: {len(SERLEFIN_MANUAL_FIXED)}")
            
            # Validar en MySQL
            print("\n[1/2] Validando existencia en MySQL (contratos)...")
            serlefin_mysql = await validate_contracts_in_mysql(SERLEFIN_MANUAL_FIXED, mysql_session)
            print(f"  ✓ Encontrados en MySQL: {len(serlefin_mysql['found_in_db'])}")
            print(f"  ✗ No encontrados en MySQL: {len(serlefin_mysql['not_found_in_db'])}")
            
            if serlefin_mysql['not_found_in_db']:
                print(f"\n  Contratos NO encontrados en MySQL (primeros 20):")
                for contract_id in serlefin_mysql['not_found_in_db'][:20]:
                    print(f"    - {contract_id}")
                if len(serlefin_mysql['not_found_in_db']) > 20:
                    print(f"    ... y {len(serlefin_mysql['not_found_in_db']) - 20} más")
            
            # Validar en PostgreSQL
            print("\n[2/2] Validando asignaciones en PostgreSQL (contract_advisors)...")
            serlefin_postgres = await validate_contracts_in_postgres(SERLEFIN_MANUAL_FIXED, 81, postgres_session)
            print(f"  ✓ Ya asignados: {len(serlefin_postgres['already_assigned'])}")
            print(f"  ✗ No asignados: {len(serlefin_postgres['not_assigned'])}")
            
            if serlefin_postgres['already_assigned']:
                print(f"\n  Contratos YA asignados (primeros 10):")
                for contract_id in serlefin_postgres['already_assigned'][:10]:
                    print(f"    - {contract_id}")
                if len(serlefin_postgres['already_assigned']) > 10:
                    print(f"    ... y {len(serlefin_postgres['already_assigned']) - 10} más")
            
            # Resumen general
            print(f"\n{'='*100}")
            print("RESUMEN GENERAL")
            print(f"{'='*100}")
            
            total_contracts = len(COBYSER_MANUAL_FIXED) + len(SERLEFIN_MANUAL_FIXED)
            total_found_mysql = len(cobyser_mysql['found_in_db']) + len(serlefin_mysql['found_in_db'])
            total_not_found_mysql = len(cobyser_mysql['not_found_in_db']) + len(serlefin_mysql['not_found_in_db'])
            total_assigned = len(cobyser_postgres['already_assigned']) + len(serlefin_postgres['already_assigned'])
            total_not_assigned = len(cobyser_postgres['not_assigned']) + len(serlefin_postgres['not_assigned'])
            
            print(f"\nTotal de contratos fijos en código: {total_contracts}")
            print(f"\nMYSQL (tabla contracts):")
            print(f"  ✓ Encontrados: {total_found_mysql} ({total_found_mysql/total_contracts*100:.1f}%)")
            print(f"  ✗ No encontrados: {total_not_found_mysql} ({total_not_found_mysql/total_contracts*100:.1f}%)")
            
            print(f"\nPOSTGRESQL (tabla contract_advisors):")
            print(f"  ✓ Ya asignados: {total_assigned} ({total_assigned/total_contracts*100:.1f}%)")
            print(f"  ✗ Pendientes de asignar: {total_not_assigned} ({total_not_assigned/total_contracts*100:.1f}%)")
            
            # Calcular contratos listos para insertar
            cobyser_ready = set(cobyser_mysql['found_in_db']) - set(cobyser_postgres['already_assigned'])
            serlefin_ready = set(serlefin_mysql['found_in_db']) - set(serlefin_postgres['already_assigned'])
            total_ready = len(cobyser_ready) + len(serlefin_ready)
            
            print(f"\n📊 CONTRATOS LISTOS PARA INSERTAR:")
            print(f"  Cobyser: {len(cobyser_ready)}")
            print(f"  Serlefin: {len(serlefin_ready)}")
            print(f"  Total: {total_ready}")
            
            if total_ready > 0:
                print(f"\n✅ Puedes ejecutar el endpoint POST /api/v1/process-manual-fixed para insertar {total_ready} contratos")
            else:
                print(f"\n⚠️  No hay contratos nuevos para insertar. Todos ya están asignados.")
            
            print("\n" + "=" * 100)
            
            # Guardar resultado en archivo
            report_file = f"reports/validation_contracts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            try:
                with open(report_file, 'w', encoding='utf-8') as f:
                    f.write("REPORTE DE VALIDACIÓN - CONTRATOS FIJOS\n")
                    f.write("=" * 100 + "\n")
                    f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    
                    f.write("COBYSER (Usuario 45):\n")
                    f.write(f"  Total en código: {len(COBYSER_MANUAL_FIXED)}\n")
                    f.write(f"  Encontrados en MySQL: {len(cobyser_mysql['found_in_db'])}\n")
                    f.write(f"  Ya asignados: {len(cobyser_postgres['already_assigned'])}\n")
                    f.write(f"  Listos para insertar: {len(cobyser_ready)}\n\n")
                    
                    if cobyser_mysql['not_found_in_db']:
                        f.write("  Contratos NO encontrados en MySQL:\n")
                        for cid in cobyser_mysql['not_found_in_db']:
                            f.write(f"    {cid}\n")
                        f.write("\n")
                    
                    f.write("SERLEFIN (Usuario 81):\n")
                    f.write(f"  Total en código: {len(SERLEFIN_MANUAL_FIXED)}\n")
                    f.write(f"  Encontrados en MySQL: {len(serlefin_mysql['found_in_db'])}\n")
                    f.write(f"  Ya asignados: {len(serlefin_postgres['already_assigned'])}\n")
                    f.write(f"  Listos para insertar: {len(serlefin_ready)}\n\n")
                    
                    if serlefin_mysql['not_found_in_db']:
                        f.write("  Contratos NO encontrados en MySQL:\n")
                        for cid in serlefin_mysql['not_found_in_db']:
                            f.write(f"    {cid}\n")
                        f.write("\n")
                    
                    f.write("=" * 100 + "\n")
                    f.write(f"Total contratos listos para insertar: {total_ready}\n")
                
                print(f"📄 Reporte guardado en: {report_file}")
            except Exception as e:
                print(f"⚠️  No se pudo guardar el reporte: {e}")


async def main():
    """Función principal."""
    try:
        await generate_validation_report()
    except Exception as e:
        print(f"\n❌ Error durante la validación: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
