import pendulum
from airflow.decorators import dag, task
import pandas as pd
import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime, UniqueConstraint # дополните импорты необходимых типов колонок
from sqlalchemy import inspect
#from steps.messages import send_telegram_success_message, send_telegram_failure_message
# Строка сверху хронически выдаёт ошибку, что модель steps не найден
import sys
import os

# Добавляем путь к папке plugin\steps в sys.path
try:
    from steps.messages import send_telegram_success_message, send_telegram_failure_message, deal_outlier, manhattan_distance, find_nearest_metro
except:
    sys.path.append('/home/mle-user/mle_projects/yn/plugins/steps')
    from messages import send_telegram_success_message, send_telegram_failure_message, deal_outlier, manhattan_distance, find_nearest_metro
@dag(
    dag_id='flats_etl',
    schedule='@once',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def prepare_flat_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import sqlalchemy
    from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime, UniqueConstraint # дополните импорты необходимых типов колонок
    from sqlalchemy import inspect
    @task()
    def create_table()-> None:
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
        flat_dataset = Table(
            'flat_dataset',
            metadata,
            Column('id', mysql.BIGINT, primary_key=True, autoincrement=True),
            Column('flat_id', mysql.BIGINT),
            Column('floor', mysql.BIGINT),
            Column('is_apartment', String),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', mysql.BIGINT),
            Column('studio', String),
            Column('total_area', Float),
            Column('price', Numeric),
            Column('build_year', mysql.BIGINT),
            Column('building_type_int', mysql.BIGINT),
            Column('ceiling_height', Float),
            Column('flats_count', mysql.BIGINT),
            Column('floors_total', mysql.BIGINT),
            Column('has_elevator', String),
            Column('nearest_metro', String),
            Column('distance_to_metro', Float),
            UniqueConstraint('flat_id', name='unique_flat_id')
        )
        if not inspect(conn).has_table(flat_dataset.name): 
            metadata.create_all(conn)

    @task()
    def extract(**kwargs):
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql ='''
        SELECT
            f.id as flat_id,
            floor,
            is_apartment,
            kitchen_area,
            living_area,
            rooms,
            studio,
            total_area,
            price,
            build_year,
            building_type_int,
            latitude,
            longitude,
            ceiling_height,
            flats_count,
            floors_total,
            has_elevator

        FROM
            flats f
            left join buildings b on b.id = f.building_id 
            
        '''
        data = pd.read_sql(sql, conn)
        conn.close()
        return data
    
    @task()
    def get_stations(**kwargs):
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql2 = '''
            SELECT * FROM metro_stations
            '''
        all_stations = pd.read_sql(sql2, conn)
        conn.close()
        return all_stations
    

    @task()
    def transform(data: pd.DataFrame, all_stations: pd.DataFrame):
        from sklearn.cluster import KMeans
        import numpy as np
        from scipy.spatial.distance import cdist
        from tqdm import tqdm
        data = data.dropna(subset=['price'])
        data = deal_outlier(data)
        data = data.drop_duplicates()
        tqdm.pandas()
        data[['nearest_metro', 'distance_to_metro']] = data.progress_apply(
                lambda row: pd.Series(find_nearest_metro(row['latitude'], row['longitude'], all_stations)), 
                axis=1
            )
        data = data.drop(['latitude', 'longitude'], axis=1)

        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="flat_dataset",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
    )

        # ваш код здесь #
    create_table()
    data = extract()
    all_stations = get_stations()
    transformed_data = transform(data, all_stations)
    load(transformed_data)
prepare_flat_dataset()