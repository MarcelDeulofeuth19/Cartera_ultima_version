"""
Pruebas unitarias de la configuracion (`app/core/config.py`).

Toda la logica probada es pura (parseo de cadenas de entorno). Los metodos
estaticos se prueban sin instanciar; las propiedades se prueban construyendo
`Settings(...)` con valores explicitos (los kwargs de init tienen prioridad
sobre el `.env`, asi que las pruebas son deterministas y no dependen del entorno).
"""
import pytest

from app.core.config import Settings

pytestmark = pytest.mark.unit


# --- metodos estaticos de parseo (puros) -----------------------------------
def test_parse_recipients_normaliza_y_deduplica():
    parsed = Settings._parse_recipients("a@x.co, A@X.co , ,b@y.co, b@y.co")
    assert parsed == ["a@x.co", "b@y.co"]


def test_parse_recipients_vacio():
    assert Settings._parse_recipients("") == []
    assert Settings._parse_recipients(None) == []


def test_parse_weekdays_valida_rango_y_deduplica():
    # 7 y -1 estan fuera de [0,6]; 'x' no es entero; 3 duplicado se colapsa.
    assert Settings._parse_weekdays("1,3,3,7,-1,x", [0]) == [1, 3]


def test_parse_weekdays_usa_fallback_si_vacio():
    assert Settings._parse_weekdays("", [1, 3]) == [1, 3]
    assert Settings._parse_weekdays("nada,foo", [2, 4]) == [2, 4]


@pytest.mark.parametrize(
    "raw, expected",
    [
        (" 1.234-567 ", "1234567"),
        ("ABC123", "123"),
        ("", ""),
        (None, ""),
        ("10.987.654-3", "109876543"),
    ],
)
def test_normalize_document_value(raw, expected):
    assert Settings._normalize_document_value(raw) == expected


# --- propiedades de negocio -------------------------------------------------
def test_pagare_excluded_status_ids_habilitado():
    cfg = Settings(PAGARE_EXCLUDE_ENABLED=True, PAGARE_EXCLUDED_STATUS_IDS="1,2,3,3,x")
    assert cfg.pagare_excluded_status_id_list == [1, 2, 3]


def test_pagare_excluded_status_ids_deshabilitado():
    cfg = Settings(PAGARE_EXCLUDE_ENABLED=False, PAGARE_EXCLUDED_STATUS_IDS="1,2,3")
    assert cfg.pagare_excluded_status_id_list == []


def test_excluded_contract_status_ids():
    cfg = Settings(EXCLUDED_CONTRACT_STATUS_IDS="5, 7 ,7,foo")
    assert cfg.excluded_contract_status_id_list == [5, 7]


def test_all_users_une_ambas_casas():
    cfg = Settings(COBYSER_USERS=[45, 46], SERLEFIN_USERS=[81, 82])
    assert cfg.all_users == [45, 46, 81, 82]


def test_cors_wildcard():
    assert Settings(CORS_ALLOWED_ORIGINS="*").cors_allowed_origin_list == ["*"]
    assert Settings(CORS_ALLOWED_ORIGINS="").cors_allowed_origin_list == ["*"]


def test_cors_lista_explicita_deduplica():
    cfg = Settings(CORS_ALLOWED_ORIGINS="https://a.co, https://b.co ,https://a.co")
    assert cfg.cors_allowed_origin_list == ["https://a.co", "https://b.co"]


def test_cors_credentials_flag():
    assert Settings(CORS_ALLOW_CREDENTIALS=True).cors_credentials_enabled is True
    assert Settings(CORS_ALLOW_CREDENTIALS=False).cors_credentials_enabled is False


def test_debug_acepta_valores_no_estandar():
    assert Settings(DEBUG="release").DEBUG is False
    assert Settings(DEBUG="on").DEBUG is True
    assert Settings(DEBUG="production").DEBUG is False


def test_redis_url_con_y_sin_password():
    con = Settings(REDIS_HOST="r", REDIS_PORT=6379, REDIS_DB=1, REDIS_PASSWORD="s3cr3t")
    assert con.redis_url == "redis://:s3cr3t@r:6379/1"
    sin = Settings(REDIS_HOST="r", REDIS_PORT=6379, REDIS_DB=0, REDIS_PASSWORD="")
    assert sin.redis_url == "redis://r:6379/0"


def test_notification_recipient_lists_normalizan():
    cfg = Settings(
        NOTIFICATION_RECIPIENTS="A@x.co, a@x.co ,c@x.co",
        COBYSER_NOTIFICATION_RECIPIENTS="cob@x.co",
        SERLEFIN_NOTIFICATION_RECIPIENTS="ser@x.co, ser@x.co",
    )
    assert cfg.notification_recipient_list == ["a@x.co", "c@x.co"]
    assert cfg.cobyser_notification_recipient_list == ["cob@x.co"]
    assert cfg.serlefin_notification_recipient_list == ["ser@x.co"]


def test_blocked_customer_documents_desde_csv():
    cfg = Settings(
        CLIENT_DOCUMENT_BLACKLIST="10.200, 10200, 3-3-3",
        CLIENT_DOCUMENT_BLACKLIST_FILE="ruta/que/no/existe.txt",
    )
    docs = cfg.blocked_customer_documents
    assert "10200" in docs   # "10.200" y "10200" colapsan a uno
    assert "333" in docs
    assert docs.count("10200") == 1


def test_blocked_customer_documents_desde_archivo(tmp_path):
    # El lector de archivo extrae grupos de digitos (\d+): cada numero en su
    # propia linea. Se combina con el CSV y se deduplica.
    archivo = tmp_path / "bloqueados.txt"
    archivo.write_text("111\n222\n111\nabc\n", encoding="utf-8")
    cfg = Settings(
        CLIENT_DOCUMENT_BLACKLIST="999",
        CLIENT_DOCUMENT_BLACKLIST_FILE=str(archivo),
    )
    docs = cfg.blocked_customer_documents
    assert "999" in docs        # del CSV
    assert "111" in docs        # del archivo
    assert "222" in docs        # del archivo
    assert docs.count("111") == 1


def test_auto_weekday_lists():
    cfg = Settings(AUTO_ASSIGNMENT_WEEKDAYS="0,1,2", AUTO_NOTIFICATION_WEEKDAYS="1,3")
    assert cfg.auto_assignment_weekday_list == [0, 1, 2]
    assert cfg.auto_notification_weekday_list == [1, 3]


def test_auto_weekday_lists_fallback():
    # Valores invalidos -> se usan los fallback de cada propiedad.
    cfg = Settings(AUTO_ASSIGNMENT_WEEKDAYS="", AUTO_NOTIFICATION_WEEKDAYS="")
    assert cfg.auto_assignment_weekday_list == [0, 1, 2, 3, 4, 5, 6]
    assert cfg.auto_notification_weekday_list == [1, 3]


def test_monthly_report_lists():
    cfg = Settings(
        MONTHLY_REPORT_TO="a@x.co, A@x.co",
        MONTHLY_REPORT_CC="cc@x.co",
    )
    assert cfg.monthly_report_to_list == ["a@x.co"]
    assert cfg.monthly_report_cc_list == ["cc@x.co"]


def test_serlefin_attachment_exception_list():
    cfg = Settings(SERLEFIN_ATTACHMENT_EXCEPTION_RECIPIENTS="X@x.co, x@x.co ,y@y.co")
    assert cfg.serlefin_attachment_exception_recipient_list == ["x@x.co", "y@y.co"]


def test_mysql_y_postgres_url():
    cfg = Settings(
        MYSQL_USER="u", MYSQL_PASSWORD="p", MYSQL_HOST="h", MYSQL_PORT=3306,
        MYSQL_DATABASE="db",
        POSTGRES_USER="pu", POSTGRES_PASSWORD="pp", POSTGRES_HOST="ph",
        POSTGRES_PORT=5432, POSTGRES_DATABASE="pdb",
    )
    assert cfg.mysql_url == "mysql+pymysql://u:p@h:3306/db"
    assert cfg.postgres_url == "postgresql+psycopg2://pu:pp@ph:5432/pdb"
