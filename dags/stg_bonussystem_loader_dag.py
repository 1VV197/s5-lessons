import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

DWH_CONN_ID = 'PG_WAREHOUSE_CONNECTION'
SRC_CONN_ID = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'

# --- helpers ---
def get_last_loaded_id(cursor, workflow_key):
    cursor.execute("""
        SELECT workflow_settings->>'last_loaded_id'
        FROM stg.srv_wf_settings
        WHERE workflow_key = %s
    """, (workflow_key,))
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] else -1

def save_last_loaded_id(cursor, workflow_key, max_id):
    settings = json.dumps({"last_loaded_id": max_id})
    cursor.execute("""
        INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
        VALUES (%s, %s::json)
        ON CONFLICT (workflow_key) DO UPDATE
        SET workflow_settings = EXCLUDED.workflow_settings
    """, (workflow_key, settings))

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'bonussystem'],
    is_paused_upon_creation=False,
)
def stg_bonussystem_loader_dag():

    @task()
    def load_ranks():
        workflow_key = 'stg_ranks_loader'
        batch_limit = 1000

        src_hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)
        dwh_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

        with dwh_hook.get_conn() as dwh_conn, src_hook.get_conn() as src_conn:
            dwh_cur = dwh_conn.cursor()
            src_cur = src_conn.cursor()

            last_loaded = get_last_loaded_id(dwh_cur, workflow_key)

            src_cur.execute("""
                SELECT id, name, bonus_percent, min_payment_threshold
                FROM ranks
                WHERE id > %s
                ORDER BY id ASC
                LIMIT %s
            """, (last_loaded, batch_limit))
            rows = src_cur.fetchall()

            for row in rows:
                dwh_cur.execute("""
                    INSERT INTO stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        bonus_percent = EXCLUDED.bonus_percent,
                        min_payment_threshold = EXCLUDED.min_payment_threshold
                """, row)

            if rows:
                max_id = max([r[0] for r in rows])
                save_last_loaded_id(dwh_cur, workflow_key, max_id)
            dwh_conn.commit()

    @task()
    def load_users():
        workflow_key = 'stg_users_loader'
        batch_limit = 1000

        src_hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)
        dwh_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

        with dwh_hook.get_conn() as dwh_conn, src_hook.get_conn() as src_conn:
            dwh_cur = dwh_conn.cursor()
            src_cur = src_conn.cursor()

            last_loaded = get_last_loaded_id(dwh_cur, workflow_key)

            src_cur.execute("""
                SELECT id, order_user_id
                FROM users
                WHERE id > %s
                ORDER BY id ASC
                LIMIT %s
            """, (last_loaded, batch_limit))
            rows = src_cur.fetchall()

            for row in rows:
                dwh_cur.execute("""
                    INSERT INTO stg.bonussystem_users (id, order_user_id)
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        order_user_id = EXCLUDED.order_user_id
                """, row)

            if rows:
                max_id = max([r[0] for r in rows])
                save_last_loaded_id(dwh_cur, workflow_key, max_id)
            dwh_conn.commit()

    @task()
    def load_events():
        workflow_key = 'stg_events_loader'
        batch_limit = 10000

        src_hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)
        dwh_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

        with dwh_hook.get_conn() as dwh_conn, src_hook.get_conn() as src_conn:
            dwh_cur = dwh_conn.cursor()
            src_cur = src_conn.cursor()

            last_loaded = get_last_loaded_id(dwh_cur, workflow_key)

            src_cur.execute("""
                SELECT id, event_ts, event_type, event_value
                FROM outbox
                WHERE id > %s
                ORDER BY id ASC
                LIMIT %s
            """, (last_loaded, batch_limit))
            rows = src_cur.fetchall()

            for row in rows:
                dwh_cur.execute("""
                    INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value
                """, row)

            if rows:
                max_id = max([r[0] for r in rows])
                save_last_loaded_id(dwh_cur, workflow_key, max_id)
            dwh_conn.commit()

    # Зависимости: можно запускать параллельно
    [load_ranks(), load_users(), load_events()]

dag = stg_bonussystem_loader_dag()
