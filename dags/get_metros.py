import requests
import pandas as pd
import boto3
from io import StringIO

@dag(
    dag_id='get_metros',
    schedule='@once',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)

def get_metro():
    import requests
    import pandas as pd
    import boto3
    from io import StringIO
    @task()
    def create_metro_table()-> None:
        import pandas as pd
        import numpy as np
        import sqlalchemy
        from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime,UniqueConstraint, Numeric
        from sqlalchemy.dialects import mysql
        # дополните импорты необходимых типов колонок
        from sqlalchemy import inspect
        hook = PostgresHook('destination_db')
        conn = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        metros_db = Table(
            'metro_stations',
            metadata,
            Column('id', Numeric, primary_key=True, autoincrement=False),
            Column('name', String),
            Column('lat', Float),
            Column('lng', Float),
            Column('order', Integer),
            Column('line', String)
        )
        if not inspect(conn).has_table(metros_db.name): 
            metadata.create_all(conn)
    # 1. Загрузка файла CSV из GitHub
    @task()
    def get_csv_from_github(url):
        import requests
        import pandas as pd
        import boto3
        from io import StringIO
        github_raw_url = url
        response = requests.get(github_raw_url)
        csv_content = response.content.decode('utf-8')
        return csv_content
    @task()
    def load(csv_content):
        data = pd.read_scv(csv_content)
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="metro_stations",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['id'],
            rows=data.values.tolist()
    )
    create_metro_table()
    csv_content = get_csv_from_github("https://github.com/AlexKretov/yn/blob/main/metro_stations.csv")
    load(csv_content)
get_metro()
