from __future__ import annotations
import json, os, sys, time
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple
import psycopg

# --- ENV (stałe z Twojego .env) ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "energia")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")
DB_TIMEOUT_SEC = int(os.getenv("HC_DB_TIMEOUT_SEC", "3"))

SCHEMA_OUTPUT = os.getenv("SCHEMA_OUTPUT", "output")
TBL_OZE = os.getenv("TBL_OZE", f"{SCHEMA_OUTPUT}.energy_oze_detail")
TBL_ARBI = os.getenv("TBL_ARBI", f"{SCHEMA_OUTPUT}.energy_arbi_detail")
TBL_BROKER = os.getenv("TBL_BROKER", f"{SCHEMA_OUTPUT}.energy_broker_detail")
VIEW_DELTA = os.getenv("VIEW_DELTA_BRUTTO", f"{SCHEMA_OUTPUT}.delta_brutto")
VIEW_SUMMARY = os.getenv("VIEW_SUMMARY", f"{SCHEMA_OUTPUT}.energy_store_summary")

# progi
MIN_ROWS_DELTA = int(os.getenv("HC_MIN_ROWS_DELTA", "1"))

# exit codes
EXIT_HEALTHY=0; EXIT_DEGRADED=8; EXIT_STARTING=10; EXIT_DB_FAIL=12
EXIT_MISSING=14; EXIT_INCONSISTENT=18; EXIT_PERMISSION=20; EXIT_UNKNOWN=30

def dsn()->str:
    parts=[f"host={DB_HOST}", f"port={DB_PORT}", f"dbname={DB_NAME}", f"user={DB_USER}", f"sslmode={DB_SSLMODE}"]
    if DB_PASSWORD: parts.append(f"password={DB_PASSWORD}")
    return " ".join(parts)

@dataclass
class Check: name:str; ok:bool; message:str; elapsed_ms:int

def run_check(fn,name)->Check:
    t0=time.perf_counter()
    try:
        ok,msg=fn()
        return Check(name,ok,msg,int((time.perf_counter()-t0)*1000))
    except psycopg.errors.InsufficientPrivilege as e:
        return Check(name,False,f"permission: {e}",int((time.perf_counter()-t0)*1000))
    except Exception as e:
        return Check(name,False,f"error: {e}",int((time.perf_counter()-t0)*1000))

def check_env()->Tuple[bool,str]:
    miss=[k for k in ("DB_HOST","DB_PORT","DB_NAME","DB_USER","SCHEMA_OUTPUT","TBL_OZE","TBL_ARBI","TBL_BROKER","VIEW_DELTA_BRUTTO","VIEW_SUMMARY") if not os.getenv(k)]
    return (len(miss)==0, "ENV OK" if not miss else "Brak zmiennych: "+",".join(miss))

def check_db()->Tuple[bool,str]:
    with psycopg.connect(dsn(), connect_timeout=DB_TIMEOUT_SEC) as c, c.cursor() as cur:
        cur.execute("select current_database(), inet_server_addr()::text, inet_server_port()")
        db,addr,port=cur.fetchone()
    return True, f"{db}@{addr}:{port}"

def check_schema_output()->Tuple[bool,str]:
    with psycopg.connect(dsn(), connect_timeout=DB_TIMEOUT_SEC) as c, c.cursor() as cur:
        cur.execute("select 1 from information_schema.schemata where schema_name=%s", (SCHEMA_OUTPUT,))
        ok = cur.fetchone() is not None
    return ok, ("OK" if ok else f"Brak schematu {SCHEMA_OUTPUT}")

def check_view_delta()->Tuple[bool,str]:
    with psycopg.connect(dsn(), connect_timeout=DB_TIMEOUT_SEC) as c, c.cursor() as cur:
        cur.execute("select to_regclass(%s)", (VIEW_DELTA,))
        if cur.fetchone()[0] is None:
            return False, f"Brak widoku {VIEW_DELTA}"
        cur.execute(f"select count(*) from {VIEW_DELTA}")
        cnt = cur.fetchone()[0]
        if cnt < MIN_ROWS_DELTA:
            return False, f"Za mało wierszy w {VIEW_DELTA}: {cnt} < {MIN_ROWS_DELTA}"
    return True, f"{VIEW_DELTA} rows={cnt}"

def check_detail_counts()->Tuple[bool,str]:
    with psycopg.connect(dsn(), connect_timeout=DB_TIMEOUT_SEC) as c, c.cursor() as cur:
        for tbl in (TBL_OZE,TBL_ARBI,TBL_BROKER):
            cur.execute("select to_regclass(%s)", (tbl,))
            if cur.fetchone()[0] is None:
                return False, f"Brak tabeli {tbl}"
        cur.execute(f"select count(*) from {VIEW_DELTA}"); rows_delta=cur.fetchone()[0]
        cur.execute(f"select count(*) from {TBL_OZE}"); rows_oze=cur.fetchone()[0]
        cur.execute(f"select count(*) from {TBL_ARBI}"); rows_arbi=cur.fetchone()[0]
        cur.execute(f"select count(*) from {TBL_BROKER}"); rows_broker=cur.fetchone()[0]
        problems=[]
        if rows_oze   < rows_delta: problems.append(f"OZE {rows_oze} < delta {rows_delta}")
        if rows_arbi  < rows_delta: problems.append(f"ARBI {rows_arbi} < delta {rows_delta}")
        if rows_broker< rows_delta: problems.append(f"BROKER {rows_broker} < delta {rows_delta}")
        if problems:  return False, "; ".join(problems)
    return True, f"delta={rows_delta}, oze={rows_oze}, arbi={rows_arbi}, broker={rows_broker}"

def check_view_summary()->Tuple[bool,str]:
    with psycopg.connect(dsn(), connect_timeout=DB_TIMEOUT_SEC) as c, c.cursor() as cur:
        cur.execute("select to_regclass(%s)", (VIEW_SUMMARY,))
        ok = cur.fetchone()[0] is not None
    return ok, ("OK" if ok else f"Brak widoku {VIEW_SUMMARY}")

def main(argv: Optional[List[str]]=None)->int:
    t0=time.perf_counter()
    checks:List[Check]=[
        run_check(check_env,"ENV"),
        run_check(check_db,"DB connect"),
        run_check(check_schema_output,"Schema output"),
        run_check(check_view_delta,"Delta exists & count"),
        run_check(check_detail_counts,"Detail counts vs delta"),
        run_check(check_view_summary,"Summary view exists"),
    ]
    ok_all=all(c.ok for c in checks)
    status="HEALTHY" if ok_all else "UNHEALTHY"
    code=EXIT_HEALTHY if ok_all else EXIT_UNKNOWN

    msg_all=" | ".join(c.message.lower() for c in checks if not c.ok)
    if "brak widoku" in msg_all or "brak tabeli" in msg_all or "brak schematu" in msg_all:
        code=EXIT_MISSING
    if " < delta " in msg_all:
        status="DEGRADED"; code=EXIT_INCONSISTENT
    if "permission" in msg_all:
        code=EXIT_PERMISSION
    result={
        "status":status,
        "code":code,
        "elapsed_ms":int((time.perf_counter()-t0)*1000),
        "checks":[asdict(c) for c in checks],
        "objects":{
            "schema_output":SCHEMA_OUTPUT,
            "tables":{"oze":TBL_OZE,"arbi":TBL_ARBI,"broker":TBL_BROKER},
            "views":{"delta":VIEW_DELTA,"summary":VIEW_SUMMARY},
        }
    }
    print(json.dumps(result, ensure_ascii=False))
    return code

if __name__=="__main__":
    try:
        sys.exit(main())
    except psycopg.OperationalError as e:
        print(json.dumps({"status":"UNHEALTHY","code":EXIT_DB_FAIL,"error":f"DB connection failed: {e}"})); sys.exit(EXIT_DB_FAIL)
    except Exception as e:
        print(json.dumps({"status":"UNHEALTHY","code":EXIT_UNKNOWN,"error":f"{e.__class__.__name__}: {e}"})); sys.exit(EXIT_UNKNOWN)
