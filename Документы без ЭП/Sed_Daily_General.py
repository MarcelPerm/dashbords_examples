from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

import psycopg2
from psycopg2 import sql, Error

CONN_ID = 'bd_pk_data_warehouse'
con = BaseHook.get_connection(CONN_ID)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 5),
    'max_active_runs': 1
}


def daily_raw():
    connection = psycopg2.connect(
        user=con.login,
        password=con.password,
        host=con.host,
        port=con.port,
        database=con.schema
    )

    cursor = connection.cursor()

    cursor.execute(
        sql.SQL('''

INSERT INTO raw.sed_document (id, cdate)
SELECT id, cdate
FROM dblink('???',
    'SELECT id, cdate
     FROM document
     WHERE document.cdate BETWEEN (current_timestamp - INTERVAL ''2 DAY'') AND current_timestamp')
     AS document(id BIGINT,
    cdate TIMESTAMP)
    WHERE NOT EXISTS 
    (SELECT 1 
     FROM raw.sed_document
     WHERE id = data.id);




INSERT INTO raw.sed_document_n (id, document_id, num, category)
SELECT data.id, data.document_id, data.num, data.category
FROM dblink('???',
    'SELECT dn.id ,dn.document_id, dn.num, category
     FROM document_n dn ')
 AS data(id BIGINT ,document_id BIGINT, num VARCHAR, category VARCHAR);
     WHERE NOT EXISTS 
    (SELECT 1 
     FROM raw.sed_document_n
     WHERE id = data.id);




INSERT INTO raw.sed_document_a (id, document_id, author)
SELECT data.id, data.document_id, data.author
FROM dblink('???',
    'SELECT da.id, da.document_id, da.author
     FROM document_a da '
) AS data(id BIGINT, document_id BIGINT, author VARCHAR);
WHERE NOT EXISTS 
    (SELECT 1 
     FROM raw.sed_document_a
     WHERE id = data.id);




INSERT INTO raw.sed_document_a_sign (id, document_a_id)
SELECT data.id, data.document_a_id
FROM dblink('???',
    'SELECT da.id, da.document_a_id
     FROM document_a_sign da
     JOIN document d ON da.document_a_id = d.id
     WHERE d.cdate BETWEEN (current_timestamp - INTERVAL ''2 DAY'') AND current_timestamp')
AS data(id BIGINT, document_a_id BIGINT)
WHERE NOT EXISTS 
    (SELECT 1 
     FROM raw.sed_document_a_sign
     WHERE id = data.id);
     
     
delete from raw.sed_document_a_sign;
INSERT INTO raw.sed_document_a_sign (id, document_a_id)
SELECT data.id, data.document_a_id
FROM dblink('???',
    'SELECT da.id, da.document_a_id
     FROM document_a_sign da '
) AS data(id BIGINT,document_a_id BIGINT);
        '''))


    connection.commit()
    
    
    
def daily_marts():
    
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine
    from sqlalchemy import create_engine
    from datetime import datetime

    conn = psycopg2.connect(
        user=con.login,
        password=con.password,
        host=con.host,
        port=con.port,
        database=con.schema
    )
    conn.autocommit = True
    cur = conn.cursor()

    sql = '''
    SELECT
        user_group.id as "ID организации"
    FROM
        dblink('???', '
            SELECT
                user_group.id,
                user_group.short_name,
                user_group.name,
                user_group.tax_number
            FROM
                user_group, c_org
            WHERE
                c_org.org_id = user_group.id
                AND c_org.dis_date IS NULL
            ORDER BY
                user_group.id
        ') AS user_group(id INT, short_name VARCHAR, name VARCHAR, tax_number VARCHAR)
    '''

    df = pd.read_sql_query(sql, conn)
    id_list = df['ID организации'].unique().tolist()
    id_list_str = ', '.join(str(id) for id in id_list)

    sql_query = f"""
    select
        d.id as "ID документа",
        n.num as "Рег номер",
        d.cdate as "Дата документа",
        usr.name as "От кого",
        u_p.post as "Должность",
        case usr.vip_type
            when 1 then 'Руководитель подразделения'
            when 2 then 'Руководство'
            else 'Руководитель'
        end as "Категория должности",
        u_g.name as "Организация"
    from
        raw.sed_document as d,
        raw.sed_document_n as n,
        raw.sed_usr as usr,
        raw.sed_document_a as a,
        raw.sed_user_post as u_p,
        raw.sed_user_group as u_g
    where NOT EXISTS
        (SELECT *
        FROM raw.sed_document_a_sign as sign
        JOIN raw.sed_document_a as a ON a.id = sign.document_a_id
        WHERE a.document_id = d.id)
        and n.document_id = d.id
        and n.num not like 'согл%'
        and n.category:: int in (1, 4)
        and a.document_id = d.id
        and a.author :: BIGINT = usr.id
        and usr.fired = 0
        and u_p.user_id = usr.id
        and u_g.id = usr.group_id
        and u_g.id IN ({id_list_str})
        and usr.vip_type in (1, 2, 3)
    """

    cur.execute(sql_query)
    conn.commit()
    rows = cur.fetchall()
    df_documents_without_ep = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
    current_datetime = datetime.now()
    df_documents_without_ep['dt_load'] = current_datetime

    engine = create_engine('postgresql+psycopg2://???')
    df_documents_without_ep.to_sql('sed_documents_without_ep', engine, schema='data_marts', if_exists='replace', index = 1)
    
with DAG('Sed_Daily_General',
         default_args=default_args, 
         schedule_interval='@daily',
         catchup = False,
         tags = ['sed']
        ) as dag:
    
    daily_raw_op = PythonOperator(task_id="daily_raw", python_callable=daily_raw)
    
    daily_marts_op = PythonOperator(task_id="daily_marts", python_callable=daily_marts)
    
    daily_raw_op >> daily_marts_op
    
