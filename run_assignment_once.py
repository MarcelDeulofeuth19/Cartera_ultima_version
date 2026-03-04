import json
import time
from app.database.connections import db_manager
from app.services.assignment_service import AssignmentService

out = {}
start = time.time()
with db_manager.get_mysql_session() as mysql_session, db_manager.get_postgres_session() as postgres_session:
    service = AssignmentService(mysql_session, postgres_session)
    result = service.execute_assignment_process()
out['duration_wall'] = round(time.time() - start, 3)
out['success'] = bool(result.get('success'))
out['error'] = result.get('error')
out['runtime_config'] = result.get('runtime_config')
out['fixed_contracts_count'] = result.get('fixed_contracts_count')
out['fixed_insert_stats'] = result.get('fixed_insert_stats')
out['insert_stats'] = result.get('insert_stats')
out['estado_actual_update_stats'] = result.get('estado_actual_update_stats')
out['report_sent'] = result.get('report_sent')
out['completion_notification_sent'] = result.get('completion_notification_sent')
out['duration_seconds'] = result.get('duration_seconds')
print(json.dumps(out, ensure_ascii=False))
