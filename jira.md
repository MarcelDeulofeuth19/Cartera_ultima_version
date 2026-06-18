# JIRA — Plan de mejoras estructurales y de calidad

**Proyecto:** Sistema de Asignación de Cartera (FastAPI · MySQL + PostgreSQL)
**Objetivo:** Elevar la estructura de la API a estándar *senior*, dejarla lista para análisis con **SonarQube** y mantener **100% de la funcionalidad**.
**Alcance acordado:** *Incremental seguro* — red de pruebas + limpieza + deduplicación de bajo riesgo + endurecimiento de seguridad **ahora**; la descomposición profunda del *God Object* queda como tickets planificados.
**Fecha:** 2026-06-03 · **Autor:** Equipo de Ingeniería

---

## 0. Convenciones

- **Tipos:** `Epic` · `Story` · `Task` · `Bug` · `Security` · `Tech-Debt`
- **Prioridad:** `P0` (crítico) · `P1` (alto) · `P2` (medio) · `P3` (bajo)
- **Severidad SonarQube:** `Blocker` · `Critical` · `Major` · `Minor`
- **Estado:** `✅ Hecho` · `🔄 En progreso` · `📋 Pendiente` · `🚫 Bloqueado (acción del equipo)`

---

## 1. Tablero resumen

| Epic | Descripción | Tickets | Hechos en esta sesión |
|------|-------------|---------|------------------------|
| [SEC](#epic-sec--seguridad) | Seguridad y secretos | 6 | 4 |
| [QA](#epic-qa--pruebas-automatizadas) | Pruebas automatizadas | 5 | 2 |
| [SONAR](#epic-sonar--calidad-de-código-sonarqube) | Calidad de código / SonarQube | 7 | 5 |
| [ARCH](#epic-arch--arquitectura-y-refactor-estructural) | Arquitectura / refactor | 6 | 0 (ARCH-1 diferido) |
| [HYG](#epic-hyg--higiene-del-repositorio) | Higiene del repositorio | 4 | 1 |
| [DOC](#epic-doc--documentación-y-consistencia) | Documentación / consistencia | 3 | 1 |

> **Verificación de esta sesión:** `119 pruebas unitarias` en verde · todo el paquete `app/` + `main.py` compila sin errores · todos los módulos importan sin fallos · `settings` resuelve los secretos desde el entorno · CORS preserva el comportamiento de producción. **Producción no se ve afectada:** los secretos ya se inyectan vía `docker-compose.yml` (`API_HMAC_SECRET`, `INTERNAL_CONFIG_DATABASE_URL`), por lo que quitar los *defaults* del código **no cambia el comportamiento del proceso automático**.

---

## 2. Flujo del sistema (estado actual / as-is)

> Documentación de la lógica vigente. El sistema ejecuta **tres procesos de negocio distintos** (rangos de días disjuntos) más un scheduler y configuración dinámica.

### 2.1 Mapa de componentes
- `main.py` → arranque, *lifespan*, CORS, registro de routers, scheduler.
- `app/api/routes/*` → capa HTTP (`assignment`, `collection_agency`, `reports`).
- `app/services/*` → lógica de negocio.
- `app/database/*` → conexiones (MySQL + PostgreSQL) y modelos ORM.
- `app/core/*` → `config`, `dpd` (rangos de mora), `file_lock` (singleton).
- `app/runtime_config/*` → config dinámica + auditoría en BD interna.

**Bases de datos:** MySQL `alocreditprod` (lectura de mora) · PostgreSQL `nexus_db` (asignaciones/historial) · PostgreSQL interna `:5559` (config/auditoría) · 2 PostgreSQL externas para reportes.

### 2.2 Proceso de ASIGNACIÓN (casas de cobranza) — `assignment_service.execute_assignment_process`
Rango **61–240 días**. Reparte entre **Serlefin (81)** y **Cobyser (45)** con proporción **60/40 configurable**.
1. Política de notificación del día (zona `America/Bogota`).
2. Carga config dinámica (porcentajes + rango) con pisos operativos (`DAYS_THRESHOLD=61`).
3. Lista negra por contrato y por cédula → retira asignaciones afectadas (`BLACKLIST_CLIENTE`).
4. Contratos fijos por **promesa activa** (`acuerdo_de_pago` con `promise_date >= hoy`) → se **excluyen** del reparto y se retiran si estaban asignados (`FIJO_PROMESA_ACTIVA`).
5. Consulta de mora en MySQL (excluyendo bloqueados + promesas).
6. **Balanceo:** agrupa por *bucket* DPD (`dpd.py`), calcula cuotas (`_compute_house_quotas`) y genera secuencia alternada 81/45 (`_build_alternating_user_sequence`) **dentro de cada bucket**.
7. Persiste en `contract_advisors` + historial; sincroniza `estado_actual`/`dpd_actual` desde MySQL.
8. Genera Excel por casa y envía correos segmentados + correo de cierre.
- **Modo append-only:** nunca elimina por antigüedad. Cierre de jornada aparte: `finalize_all_active_assignments` (`CIERRE_MASIVO`).

### 2.3 Proceso de DIVISIÓN (asesores internos) — `division_service.execute_division_process`
Rango **1–60 días**. Reparte entre **14 asesores** (`DIVISION_USER_IDS`) con criterio **equitativo** (~1/14).
1. Lista negra por cédula → retira afectados.
2. Contratos fijos/promesas (`acuerdo_de_pago` vigente + `pago_total` ≤30 días) → se excluyen.
3. Lee asignaciones actuales y la cartera candidata (MySQL, 1–60 días).
4. **Balance equitativo (greedy):** ordena por mora desc. y asigna cada contrato nuevo "al asesor de menor carga" → diferencia máxima de 1 entre asesores nuevos.
5. Persiste nuevos + historial.
- **Limitación documentada:** *append-only*, solo equilibra los nuevos (no corrige desbalances heredados).

### 2.4 Proceso de REPORTES
- **Automático (correo):** `assignment_service.generate_and_send_reports` → `report_service_extended` (Excel por casa con capital/descuentos/opciones de pago + métricas 60/40 + HTML) → `email_service` (envío segmentado Cobyser/Serlefin/general).
- **Manual (endpoint):** `collection_agency_report_service` vía `POST /informe-casa-cobranza`.
- **Interno:** `report_service` (TXT + Excel resumen del resultado del algoritmo).
- **Fin de ciclo/mensual:** `cycle_end_report_service` (en desarrollo activo).

### 2.5 Scheduler y configuración dinámica
- `scheduler_service` (asyncio): dispara **solo la asignación** (casas de cobranza) a la hora/días configurados, bajo *file lock*; la división es manual por endpoint. Bucle de limpieza de reportes cada 24h.
- `runtime_config`: persiste porcentajes/rango + auditoría en la BD interna; doble HMAC (sesión de panel y firma de API).

> El detalle por archivo:línea de cada hallazgo está en los EPICs siguientes.

---

## EPIC SEC — Seguridad

> SonarQube marca secretos embebidos como *Vulnerability/Blocker*. Estos tickets atacan los hallazgos de seguridad priorizando no romper el arranque.

### SEC-1 · Secreto HMAC hardcodeado en el código — `✅ Hecho`
- **Tipo:** Security · **Prioridad:** P0 · **Severidad:** Blocker · **Regla Sonar:** `secrets:S6703 / S2068`
- **Problema:** `API_HMAC_SECRET = "<REDACTADO>"` en `app/core/config.py:164`, en texto plano y versionado en git.
- **Solución aplicada:** se elimina el valor por defecto del código (`API_HMAC_SECRET: str = ""`), se carga exclusivamente desde el entorno. **Producción lo provee** vía `docker-compose.yml` (`API_HMAC_SECRET=${API_HMAC_SECRET}`, interpolado desde el `.env` no versionado), por lo que el proceso automático NO cambia. En `.env.example` queda como placeholder vacío. Validación de arranque que aborta si está vacío fuera de `DEBUG`.
- **Criterio de aceptación:** ningún secreto literal en el código fuente; la app arranca con la variable definida en entorno.
- **Acción manual requerida:** ⚠️ rotar el secreto en producción (ver [ACCIONES MANUALES](#acciones-manuales-requeridas-del-equipo)) porque ya quedó expuesto en el historial de git.
- **Archivos:** `app/core/config.py`, `.env`, `.env.example`

### SEC-2 · Credenciales de BD interna en texto plano — `✅ Hecho`
- **Tipo:** Security · **Prioridad:** P0 · **Severidad:** Blocker · **Regla Sonar:** `secrets:S2068`
- **Problema:** `INTERNAL_CONFIG_DATABASE_URL` con `internal_config_user:internal_config_pass` embebido en `app/core/config.py:161`.
- **Solución aplicada:** *default* vacío en el código; la URL se lee del entorno. **Producción ya la provee** en `docker-compose.yml:84`. Genéricado el *placeholder* de `.env.example`.
- **Criterio de aceptación:** sin credenciales literales en el código fuente.
- **Archivos:** `app/core/config.py`, `.env.example`

### SEC-3 · Contraseña Gmail versionada en `test_email.py` — `✅ Hecho`
- **Tipo:** Security · **Prioridad:** P0 · **Severidad:** Blocker · **Regla Sonar:** `secrets:S6290`
- **Problema:** `test_email.py:14` contenía la *app password* de `noreply@alocredit.co` en texto plano.
- **Solución aplicada:** se elimina el archivo (script de un solo uso, sin valor de prueba real).
- **Acción manual requerida:** ⚠️ **rotar de inmediato** la contraseña de aplicación de Gmail.
- **Archivos:** `test_email.py` (eliminado)

### SEC-4 · CORS totalmente permisivo — `✅ Hecho`
- **Tipo:** Security · **Prioridad:** P1 · **Severidad:** Critical · **Regla Sonar:** `python:S5122`
- **Problema:** `allow_origins=["*"]` + `allow_credentials=True` en `main.py:110` (configuración insegura e internamente contradictoria).
- **Solución aplicada:** orígenes y credenciales configurables vía `CORS_ALLOWED_ORIGINS` y `CORS_ALLOW_CREDENTIALS`. **El default preserva EXACTAMENTE el comportamiento actual de producción** (`*` + credenciales) para no romper el chatbot/clientes; el endurecimiento se hace definiendo orígenes explícitos por entorno (sin tocar código).
- **Criterio de aceptación:** producción puede restringir orígenes por entorno sin cambiar código; comportamiento por defecto preservado e idéntico.
- **Nota Sonar:** para cerrar el *hotspot* S5122 definir orígenes explícitos + `CORS_ALLOW_CREDENTIALS=False` en el entorno de producción.
- **Archivos:** `main.py`, `app/core/config.py`, `.env.example`

### SEC-5 · Fuga de detalles de error al cliente (`str(e)`) — `📋 Pendiente`
- **Tipo:** Security · **Prioridad:** P1 · **Severidad:** Major · **Regla Sonar:** `python:S5131 / information exposure`
- **Problema:** múltiples handlers devuelven `str(e)` (incl. health check exponiendo errores de conexión a BD): `app/api/routes/assignment.py:141,249,287,316,326`, `collection_agency.py:92,161,232`, `reports.py:76`.
- **Pendiente:** loggear el detalle internamente y responder un mensaje genérico al cliente, conservando los códigos HTTP. Se difiere para revisar contratos de respuesta con consumidores (chatbot) y no romper integraciones.
- **Criterio de aceptación:** el cuerpo de error público no expone trazas/credenciales/hosts.

### SEC-6 · HMAC sin protección anti-replay — `📋 Pendiente`
- **Tipo:** Security · **Prioridad:** P2 · **Severidad:** Major · **Regla Sonar:** `python:S5659 (firma sin nonce/ts)`
- **Problema:** `verify_hmac_signature` firma `method+path+body` sin *timestamp*/nonce (`app/api/dependencies/security.py:23`) → una petición capturada puede reenviarse.
- **Pendiente:** añadir header `X-Timestamp` + ventana de validez y rechazo de peticiones fuera de ventana. **No se aplica ahora** porque rompería los clientes existentes (incluido `hmac_client_example.py` y el endpoint del chatbot); requiere coordinación de despliegue cliente/servidor.
- **Criterio de aceptación:** *replay* fuera de ventana rechazado; clientes actualizados.

---

## EPIC QA — Pruebas automatizadas

> Estado inicial: **0% de cobertura automatizada**, sin `pytest`, sin carpeta `tests/`. Los `test_*.py` de la raíz son scripts manuales que corren contra producción real.

### QA-1 · Infraestructura de pruebas (pytest) — `✅ Hecho`
- **Tipo:** Story · **Prioridad:** P0 · **Severidad:** —
- **Solución aplicada:** `pyproject.toml` con configuración de `pytest` (testpaths, marcadores `unit`/`integration`/`e2e`, cobertura), `requirements-dev.txt` (`pytest`, `pytest-cov`, `pytest-mock`, `httpx`, `freezegun`), carpeta `tests/` con `conftest.py`.
- **Criterio de aceptación:** `pytest -m unit` ejecuta sin necesidad de BD ni red.
- **Archivos:** `pyproject.toml`, `requirements-dev.txt`, `tests/conftest.py`

### QA-2 · Pruebas unitarias del núcleo puro de negocio — `✅ Hecho`
- **Tipo:** Story · **Prioridad:** P0 · **Severidad:** —
- **Cobertura añadida (lógica pura, sin BD):**
  - `app/core/dpd.py` → `tests/unit/test_dpd.py` (fronteras 0/3/4/15/60/61/209/210, negativos, None, ambas funciones de rango).
  - `_compute_house_quotas` y `_build_alternating_user_sequence` (reparto 60/40 y configurable) → `tests/unit/test_assignment_quotas.py`.
  - Listas de contratos fijos (conteos 79/415/494, sin duplicados internos ni cruzados) → `tests/unit/test_manual_fixed_contracts.py`.
  - Firma HMAC (válida → 200/None, ausente → 401, inválida → 403) → `tests/unit/test_security_hmac.py`.
  - `file_lock.check_lock_status` → `tests/unit/test_file_lock.py`.
- **Criterio de aceptación:** todas verdes en CI sin infraestructura.
- **Archivos:** `tests/unit/*`

### QA-3 · Test de regresión de la query de reportes — `📋 Pendiente (depende de ARCH-1)`
- **Tipo:** Task · **Prioridad:** P1
- **Pendiente:** test *golden* que fije la query compartida extraída en ARCH-1. Se hará junto con ARCH-1 cuando se retome.

### QA-4 · Pruebas de integración con BD efímera — `📋 Pendiente`
- **Tipo:** Story · **Prioridad:** P1
- **Pendiente:** `testcontainers`/esquema dedicado + *fixtures* transaccionales con `rollback`. Cubrir `assignment_service` (`get_fixed_contracts`, `save_assignments`, `execute_assignment_process`), `division_service`, `history_service`, `contract_service`. Validar idempotencia y constraint UNIQUE. **Nunca contra producción.**

### QA-5 · Pruebas E2E de la API (TestClient) — `📋 Pendiente`
- **Tipo:** Story · **Prioridad:** P2
- **Pendiente:** reemplazar `test_api.py`/`test_complete.py` por `TestClient(app)` con `app.dependency_overrides`; probar HMAC, lock singleton (409) y endpoints sin disparar el proceso real (mock del servicio + SMTP).

---

## EPIC SONAR — Calidad de código (SonarQube)

### SONAR-1 · Encoding corrupto (mojibake) en `config.py` — `✅ Hecho`
- **Tipo:** Bug · **Prioridad:** P2 · **Severidad:** Minor
- **Problema:** docstrings/comentarios con UTF-8 mal decodificado (`ConfiguraciÃ³n`, `parÃ¡metros`, …).
- **Solución aplicada:** reescritura de comentarios/docstrings con acentos correctos en `app/core/config.py`.
- **Archivos:** `app/core/config.py`

### SONAR-2 · Uso de `any` (builtin) como anotación de tipo — `✅ Hecho`
- **Tipo:** Code Smell · **Prioridad:** P2 · **Severidad:** Minor · **Regla Sonar:** `python:S5886`
- **Problema:** `Dict[str, any]` en `assignment_service.py:698` y `manual_fixed_service.py:58` (debe ser `typing.Any`).
- **Solución aplicada:** `any` → `Any`; se añade `Any` a los imports de `manual_fixed_service.py`.
- **Archivos:** `app/services/assignment_service.py`, `app/services/manual_fixed_service.py`

### SONAR-3 · Variable de instancia muerta `_last_assigned_user` — `✅ Hecho`
- **Tipo:** Code Smell · **Prioridad:** P3 · **Severidad:** Minor · **Regla Sonar:** `python:S1854 (dead store)`
- **Problema:** `self._last_assigned_user` se asigna (`assignment_service.py:56`) y nunca se lee.
- **Solución aplicada:** eliminada.
- **Archivos:** `app/services/assignment_service.py`

### SONAR-4 · Método muerto `_build_weighted_sequence` — `✅ Hecho`
- **Tipo:** Code Smell · **Prioridad:** P3 · **Severidad:** Minor · **Regla Sonar:** `python:S1144 (unused private)`
- **Problema:** `@staticmethod _build_weighted_sequence` sin referencias en todo el repo.
- **Solución aplicada:** eliminado (su lógica fue reemplazada por `_compute_house_quotas` + `_build_alternating_user_sequence`).
- **Archivos:** `app/services/assignment_service.py`

### SONAR-5 · Configuración SonarQube del proyecto — `✅ Hecho`
- **Tipo:** Task · **Prioridad:** P2
- **Solución aplicada:** `sonar-project.properties` con `sources`, `tests`, exclusiones (`venv`, `docker-data`, `reports`, `scripts` desechables) y rutas de cobertura (`coverage.xml`).
- **Archivos:** `sonar-project.properties`

### SONAR-6 · Complejidad del *God Object* `assignment_service.py` — `📋 Pendiente`
- **Tipo:** Tech-Debt · **Prioridad:** P1 · **Severidad:** Critical · **Regla Sonar:** `python:S3776 (cognitive complexity)`
- **Problema:** `execute_assignment_process` (~228 líneas), `balance_assignments` (~155), `save_assignments` (~160) superan ampliamente el umbral de complejidad cognitiva.
- **Pendiente:** se aborda dentro de [ARCH-2/3/4](#epic-arch--arquitectura-y-refactor-estructural).

### SONAR-7 · Funciones `enforce_*` casi duplicadas — `📋 Pendiente`
- **Tipo:** Code Smell · **Prioridad:** P2 · **Severidad:** Major · **Regla Sonar:** `python:S4144 (duplicate methods)`
- **Problema:** `enforce_blacklist_on_active_assignments` y `enforce_promises_on_active_assignments` comparten estructura (solo cambia el tipo/estado).
- **Pendiente:** unificar en un método parametrizado por `tipo`/`estado`. Requiere pruebas de integración previas (QA-4) por tocar escritura en BD.

---

## EPIC ARCH — Arquitectura y refactor estructural

### ARCH-1 · Deduplicar la query SQL de reportes — `📋 Pendiente (diferido conscientemente)`
- **Tipo:** Tech-Debt · **Prioridad:** P1 · **Severidad:** Major · **Regla Sonar:** `python:S4144 / duplications`
- **Problema:** query SQL de ~274 líneas **byte-idéntica** (verificado con `diff -wB`: solo difieren firma y docstring) duplicada en `report_service_extended.generate_detailed_query` y `collection_agency_report_service._generar_query`.
- **Plan listo:** extraer a `app/services/house_report_query.py::build_detailed_report_query()`; ambos métodos como *wrappers* delgados (firmas intactas → cero impacto en *callers*).
- **Por qué se difiere (decisión senior):** (1) estos servicios generan informes que **se ejecutan automáticamente en producción**; (2) hay **trabajo concurrente** sobre la configuración/servicios de reportes (informe mensual `MONTHLY_REPORT_*`); (3) es un *code smell* de duplicación, no un fallo funcional. Tocar esos archivos ahora podría colisionar con cambios en curso. Se retoma cuando el trabajo de informe mensual esté estable y exista la red de pruebas de integración (QA-4).
- **Archivos (al retomar):** `app/services/house_report_query.py` (nuevo), `report_service_extended.py`, `collection_agency_report_service.py`

### ARCH-2 · Extraer capa de reglas puras de asignación — `📋 Pendiente`
- **Tipo:** Story · **Prioridad:** P1
- **Pendiente:** mover `_compute_house_quotas`, `_build_alternating_user_sequence` y la lógica de `dpd.py` a un módulo `app/core/assignment_rules.py`, ya cubierto por pruebas (QA-2), para aislar el corazón de negocio del I/O.

### ARCH-3 · Introducir `AssignmentRepository` (capa de persistencia) — `📋 Pendiente`
- **Tipo:** Story · **Prioridad:** P1
- **Pendiente:** encapsular consultas/escrituras de `contract_advisors`/historial; `execute_assignment_process` queda como orquestador delgado. Requiere QA-4 como red de seguridad.

### ARCH-4 · Inyección de dependencias en servicios — `📋 Pendiente`
- **Tipo:** Story · **Prioridad:** P2
- **Pendiente:** pasar `ContractService`, `HistoryService`, `RuntimeConfigService`, `email_service`, `report_service` por constructor (con *defaults*) en lugar de instanciarlos/importar singletons → testabilidad sin *monkeypatch*.

### ARCH-5 · Deduplicar el *fallback* a MySQL de reportes — `📋 Pendiente`
- **Tipo:** Tech-Debt · **Prioridad:** P2 · **Severidad:** Major
- **Problema:** `_fetch_missing_contracts_from_mysql` vs `_fetch_missing_from_mysql` (~250 líneas) comparten lógica pero **difieren** en adquisición de conexión y en una posible divergencia de comisión (`'30%'` string vs `30` int). **No se unifica ahora** por riesgo a la funcionalidad sin BD de pruebas.
- **Pendiente:** unificar tras QA-4, decidiendo la representación canónica de la comisión (ver [BUG-COMM](#bug-comm--comisión-divergente-entre-rutas-de-reporte--pendiente)).

### ARCH-6 · Sacar DDL de runtime a migraciones — `📋 Pendiente`
- **Tipo:** Tech-Debt · **Prioridad:** P2
- **Problema:** `_ensure_estado_actual_column` / `_ensure_history_dpd_actual_column` ejecutan `ALTER TABLE`/`CREATE INDEX` en cada corrida (`assignment_service.py:66,122`).
- **Pendiente:** migrar a Alembic; eliminar el DDL del *hot path* operativo.

---

## EPIC HYG — Higiene del repositorio

### HYG-1 · Eliminar scripts desechables `scratch*.py` — `✅ Hecho`
- **Tipo:** Task · **Prioridad:** P2 · **Regla Sonar:** `dead code`
- **Problema:** `scratch.py`, `scratch2.py`, `scratch3.py` eran *refactors* de un solo uso sobre `admin_panel.py`, archivo **ya eliminado** → código muerto al 100%.
- **Solución aplicada:** eliminados.

### HYG-2 · Salvaguardas en `reset_assignments.py` — `📋 Pendiente`
- **Tipo:** Task · **Prioridad:** P1 · **Severidad:** Critical (operacional)
- **Problema:** ejecuta cierre masivo de TODAS las asignaciones (vacía `contract_advisors`) sin confirmación ni `--dry-run`.
- **Pendiente:** añadir confirmación interactiva / flag `--force` y modo `--dry-run`.

### HYG-3 · Reubicar scripts operativos y de diagnóstico — `📋 Pendiente`
- **Tipo:** Task · **Prioridad:** P3
- **Pendiente:** mover `run_*.py`, `generate_and_send_reports.py`, `insert_fixed_contracts.py` a `scripts/`; `check_*`, `validate_contracts_in_db.py`, `verify_fixed_contracts.py` a `scripts/diagnostics/`; `hmac_client_example.py` a `examples/`.

### HYG-4 · Relocalizar `_run_v5.py` (producción mal ubicada) — `📋 Pendiente`
- **Tipo:** Tech-Debt · **Prioridad:** P3
- **Pendiente:** es código de producción con fechas hardcodeadas y nombre de borrador; convertir en módulo de `app/` con parámetros inyectables.

---

## EPIC DOC — Documentación y consistencia

### DOC-1 · `.env.example` completo y actualizado — `✅ Hecho`
- **Tipo:** Task · **Prioridad:** P1
- **Solución aplicada:** se añaden `API_HMAC_SECRET`, `INTERNAL_CONFIG_DATABASE_URL` y `CORS_ALLOWED_ORIGINS` con *placeholders*, documentando que son obligatorios.
- **Archivos:** `.env.example`

### DOC-2 · Documentar "8 vs 14 usuarios" en división — `📋 Pendiente`
- **Tipo:** Bug · **Prioridad:** P3
- **Problema:** comentarios/logs dicen "8 usuarios" pero `DIVISION_USER_IDS` tiene 14 (`config.py:63`); `DIVISION_CONTRATOS.md` desactualizado.
- **Pendiente:** sincronizar comentarios, logs y doc.

### DOC-3 · Aclarar semántica de "contratos fijos" — `📋 Pendiente`
- **Tipo:** Tech-Debt · **Prioridad:** P3
- **Problema:** en la división, los "fijos" terminan *excluidos* del reparto, contradiciendo `ensure_fixed_contracts_assigned` (no invocado en el flujo principal).
- **Pendiente:** decidir si `ensure_fixed_contracts_assigned` es código muerto y documentar la intención.

---

## BUG-COMM · Comisión divergente entre rutas de reporte — `📋 Pendiente`
- **Tipo:** Bug · **Prioridad:** P1 · **Severidad:** Major
- **Problema:** misma comisión calculada distinto: `'30%'` (string) en `report_service_extended.py:458` vs `30` (int) en `collection_agency_report_service.py:720`; además el `CASE` SQL tiene una rama inalcanzable (`BETWEEN 151 AND 210` seguido de `151 AND 211`).
- **Pendiente:** decidir representación canónica; se resolverá junto con [ARCH-5](#arch-5--deduplicar-el-fallback-a-mysql-de-reportes--pendiente). **No tocado** para preservar el comportamiento actual.

---

## Resumen de lo HECHO en esta sesión

1. **Seguridad:** secretos HMAC y de BD interna fuera del código (SEC-1, SEC-2), credencial Gmail eliminada (SEC-3), CORS configurable preservando comportamiento (SEC-4).
2. **Pruebas:** infraestructura `pytest` + suite unitaria del núcleo puro — **119 pruebas en verde** (QA-1, QA-2).
3. **SonarQube:** encoding corregido, `any→Any`, código muerto eliminado, configuración del proyecto Sonar (SONAR-1..5).
4. **Higiene:** `scratch*.py` y `test_email.py` eliminados (HYG-1, SEC-3).
5. **Docs:** `.env.example` completo y `jira.md` (este tablero) (DOC-1).

**Diferido conscientemente:** ARCH-1 (dedup de query de reportes) por seguridad de producción y trabajo concurrente — ver su ficha.

### Verificación ejecutada (evidencia)
- `python -m pytest tests/` → **119 passed**.
- `python -m compileall app main.py` → sin errores de sintaxis.
- Import de los 12 módulos principales → todos OK.
- `settings` resuelve `API_HMAC_SECRET` e `INTERNAL_CONFIG_DATABASE_URL` desde entorno; CORS = `['*']` + credenciales `True` (idéntico a hoy).

**Garantía de funcionalidad:** no se modificó ninguna regla de negocio ni firma pública; los cambios son aditivos (pruebas/config), eliminación de código sin referencias, o ajustes que preservan el comportamiento (verificado). Producción inalterada: los secretos se inyectan por `docker-compose.yml`.

### Cómo ejecutar las pruebas
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest                       # 119 unitarias, sin BD ni red
pytest --cov=app --cov-report=xml   # genera coverage.xml para SonarQube
```

---

## ACCIONES MANUALES REQUERIDAS DEL EQUIPO

> Estas no las puede hacer el repositorio; requieren acceso a sistemas externos / despliegue.

1. 🚫 **Rotar la contraseña de aplicación de Gmail** de `noreply@alocredit.co` (estaba expuesta en `test_email.py`). Actualizar `SMTP_PASSWORD` en el `.env` de cada entorno.
2. 🚫 **Rotar `API_HMAC_SECRET`** (quedó expuesto en el historial de git) y definirlo como variable de entorno en todos los despliegues. Coordinar con los clientes/chatbot que firman peticiones.
3. 🚫 **Definir en el `.env` de producción** las claves obligatorias: `API_HMAC_SECRET`, `INTERNAL_CONFIG_DATABASE_URL`, credenciales MySQL/PostgreSQL/reportes. Sin ellas la app no arranca (comportamiento deseado tras SEC-1/SEC-2).
4. 🚫 **Decidir orígenes CORS** reales de producción y fijarlos en `CORS_ALLOWED_ORIGINS`.
5. 🚫 **Confirmar despliegue de la capa de seguridad:** `app/api/dependencies/` y `app/api/routes/reports.py` están *untracked* en git — commitearlos o el despliegue queda sin autenticación.
