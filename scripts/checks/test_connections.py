"""
Script de prueba para verificar las conexiones de bases de datos
y la estructura del proyecto antes de ejecutar la API completa.
"""
# --- bootstrap: ejecutable desde cualquier ruta (anade la raiz del repo al path) ---
import sys as _sys, pathlib as _pathlib
for _cand in (_pathlib.Path(__file__).resolve(), *_pathlib.Path(__file__).resolve().parents):
    if (_cand / "app").is_dir() and (_cand / "main.py").exists():
        _sys.path.insert(0, str(_cand)); break
# --- fin bootstrap ---
import sys
from app.core.config import settings
from app.database.connections import db_manager

def test_mysql_connection():
    """Prueba la conexión a MySQL"""
    print("\n" + "="*60)
    print("🔍 Probando conexión a MySQL...")
    print("="*60)
    try:
        with db_manager.get_mysql_session() as session:
            result = session.execute("SELECT VERSION()").fetchone()
            print(f"✅ MySQL conectado exitosamente!")
            print(f"   Versión: {result[0]}")
            print(f"   Host: {settings.MYSQL_HOST}")
            print(f"   Database: {settings.MYSQL_DATABASE}")
            return True
    except Exception as e:
        print(f"❌ Error al conectar a MySQL: {e}")
        return False

def test_postgres_connection():
    """Prueba la conexión a PostgreSQL"""
    print("\n" + "="*60)
    print("🔍 Probando conexión a PostgreSQL...")
    print("="*60)
    try:
        with db_manager.get_postgres_session() as session:
            result = session.execute("SELECT version()").fetchone()
            print(f"✅ PostgreSQL conectado exitosamente!")
            print(f"   Versión: {result[0][:50]}...")
            print(f"   Host: {settings.POSTGRES_HOST}")
            print(f"   Database: {settings.POSTGRES_DATABASE}")
            return True
    except Exception as e:
        print(f"❌ Error al conectar a PostgreSQL: {e}")
        return False

def test_tables_exist():
    """Verifica que las tablas necesarias existan"""
    print("\n" + "="*60)
    print("🔍 Verificando tablas requeridas...")
    print("="*60)
    
    # Verificar en MySQL
    print("\n📋 MySQL (alocreditprod):")
    try:
        with db_manager.get_mysql_session() as session:
            tables = ['contract_amortization', 'contract_status']
            for table in tables:
                result = session.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables "
                    f"WHERE table_schema = '{settings.MYSQL_DATABASE}' "
                    f"AND table_name = '{table}'"
                ).fetchone()
                if result[0] > 0:
                    print(f"   ✅ {table}")
                else:
                    print(f"   ⚠️  {table} - NO ENCONTRADA")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Verificar en PostgreSQL
    print("\n📋 PostgreSQL (nexus_db):")
    try:
        with db_manager.get_postgres_session() as session:
            tables = [
                ('alocreditindicators', 'contract_advisors'),
                ('alocreditindicators', 'managements')
            ]
            for schema, table in tables:
                result = session.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables "
                    f"WHERE table_schema = '{schema}' "
                    f"AND table_name = '{table}'"
                ).fetchone()
                if result[0] > 0:
                    print(f"   ✅ {schema}.{table}")
                else:
                    print(f"   ⚠️  {schema}.{table} - NO ENCONTRADA")
    except Exception as e:
        print(f"   ❌ Error: {e}")

def main():
    """Ejecuta todas las pruebas"""
    print("\n" + "="*60)
    print("🚀 TEST DE CONECTIVIDAD Y ESTRUCTURA")
    print("   Sistema de Asignación de Contratos")
    print("="*60)
    
    mysql_ok = test_mysql_connection()
    postgres_ok = test_postgres_connection()
    
    if mysql_ok and postgres_ok:
        test_tables_exist()
        print("\n" + "="*60)
        print("✅ TODAS LAS CONEXIONES ESTÁN ACTIVAS")
        print("="*60)
        print("\n💡 Puedes iniciar la API con: python main.py")
        print("   o ejecuta: start.bat\n")
        return 0
    else:
        print("\n" + "="*60)
        print("❌ HAY PROBLEMAS DE CONECTIVIDAD")
        print("="*60)
        print("\n⚠️  Revisa las credenciales en el archivo .env")
        print("   y verifica la conectividad de red.\n")
        return 1

if __name__ == "__main__":
    sys.exit(main())
