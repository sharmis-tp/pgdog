from globals import direct_sync, normal_sync, no_out_of_sync, admin
from multiprocessing import Pool
from psycopg.pq import DiagnosticField, ExecStatus, TransactionStatus
import pytest
import time


def run_prepare_execute(worker_id):
    conn = normal_sync()
    conn.autocommit = True
    cur = conn.cursor()

    stmt_name = f"stmt_{worker_id}"
    cur.execute(f"PREPARE {stmt_name} AS SELECT $1::bigint * 2")
    time.sleep(0.01)

    for i in range(100):
        cur.execute(f"EXECUTE {stmt_name}({i})")
        result = cur.fetchone()
        assert result[0] == i * 2
        time.sleep(0.01)

    cur.execute(f"DEALLOCATE {stmt_name}")
    conn.close()
    return True


def test_prepare_execute_parallel():
    admin().execute("SET prepared_statements TO 'full'")

    with Pool(5) as pool:
        results = pool.map(run_prepare_execute, range(5))
    assert all(results)
    no_out_of_sync()
    admin().execute("RELOAD")


@pytest.mark.parametrize("connect", [direct_sync, normal_sync])
def test_parse_cached_statement_in_aborted_transaction(connect):
    conn = connect()
    conn.autocommit = True
    pgconn = conn.pgconn
    ok = ExecStatus.COMMAND_OK

    def sqlstate(result):
        return result.error_field(DiagnosticField.SQLSTATE)

    assert pgconn.exec_(b"BEGIN").status == ok
    # Each query is parsed again under a new name below, which
    # PgDog maps to the statement already prepared on the server.
    query = b"SELECT 'aborted_transaction_parse'"
    assert pgconn.prepare(b"aborted_tx_1", query).status == ok
    assert pgconn.prepare(b"aborted_tx_rb_1", b"ROLLBACK").status == ok
    assert sqlstate(pgconn.exec_(b"SELECT 1/0")) == b"22012"

    assert sqlstate(pgconn.prepare(b"aborted_tx_2", query)) == b"25P02"

    # Statements that end the transaction can still be parsed.
    assert pgconn.prepare(b"aborted_tx_rb_2", b"ROLLBACK").status == ok
    assert pgconn.exec_prepared(b"aborted_tx_rb_2", None).status == ok
    assert pgconn.transaction_status == TransactionStatus.IDLE

    conn.close()
    no_out_of_sync()
