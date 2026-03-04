import time
from app.database.connections import db_manager
from app.services.assignment_service import AssignmentService

print('START', flush=True)
with db_manager.get_mysql_session() as mysql_session, db_manager.get_postgres_session() as postgres_session:
    service = AssignmentService(mysql_session, postgres_session)
    print('SERVICE_OK', flush=True)

    t=time.time(); runtime_config = service._load_runtime_assignment_config(); print('runtime_config', round(time.time()-t,3), flush=True)
    effective_min_days = max(int(runtime_config.min_days), 61)
    effective_max_days = max(int(runtime_config.max_days), effective_min_days)

    t=time.time(); blocked = service._load_contract_blacklist(); print('blacklist_loaded', len(blocked), round(time.time()-t,3), flush=True)
    t=time.time(); bl_stats = service.enforce_blacklist_on_active_assignments(blocked); print('blacklist_enforced', bl_stats, round(time.time()-t,3), flush=True)

    t=time.time(); fixed = service.get_fixed_contracts(); print('fixed_loaded', {k:len(v) for k,v in fixed.items()}, round(time.time()-t,3), flush=True)
    t=time.time(); fixed_stats = service.ensure_fixed_contracts_assigned(fixed, effective_max_days); print('fixed_ensured', fixed_stats, round(time.time()-t,3), flush=True)

    t=time.time(); current = service.get_current_assignments(); print('current_loaded', {k:len(v) for k,v in current.items()}, round(time.time()-t,3), flush=True)

    t=time.time(); contracts = service._require_contract_service().get_contracts_with_arrears(min_days=effective_min_days, max_days=effective_max_days, excluded_contract_ids=None); print('contracts_loaded', len(contracts), round(time.time()-t,3), flush=True)
    t=time.time(); new_assign, days_map = service.balance_assignments(contracts, current, runtime_config.serlefin_ratio, blocked); print('balanced', {k:len(v) for k,v in new_assign.items()}, len(days_map), round(time.time()-t,3), flush=True)

    t=time.time(); insert_stats = service.save_assignments(new_assign, contracts_days_map=days_map, excluded_contract_ids=blocked); print('saved', insert_stats, round(time.time()-t,3), flush=True)

    t=time.time(); current_after = service.get_current_assignments(); print('current_after', {k:len(v) for k,v in current_after.items()}, round(time.time()-t,3), flush=True)
    t=time.time(); estado_stats = service.refresh_estado_actual_for_assignments(current_after); print('estado_refreshed', estado_stats, round(time.time()-t,3), flush=True)

    t=time.time(); report_ok = service.generate_and_send_reports(); print('report_sent', report_ok, round(time.time()-t,3), flush=True)

print('DONE', flush=True)
