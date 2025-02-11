import pendulum
from airflow.decorators import dag, task
import pandas as pd
import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime,UniqueConstraint # дополните импорты необходимых типов колонок
from sqlalchemy import inspect
#from steps.messages import send_telegram_success_message, send_telegram_failure_message
# Строка сверху хронически выдаёт ошибку, что модель steps не найден
import sys
import os

# Добавляем путь к папке plugin\steps в sys.path
sys.path.append('/home/mle-user/mle_projects/yn/plugins/steps')
from messages import send_telegram_success_message, send_telegram_failure_message
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
    from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime,UniqueConstraint # дополните импорты необходимых типов колонок
    from sqlalchemy import inspect
    @task()
    def create_table()-> None:
        import pandas as pd
        import numpy as np
        import sqlalchemy
        from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime,UniqueConstrain
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
            Column('price', mysql.BIGINT),
            Column('build_year', mysql.BIGINT),
            Column('building_type_int', mysql.BIGINT),
            Column('living_cluster', mysql.BIGINT),
            Column('ceiling_height', Float),
            Column('flats_count', mysql.BIGINT),
            Column('floors_total', mysql.BIGINT),
            Column('has_elevator', String),
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
    def transform(data: pd.DataFrame):
        from sklearn.cluster import KMeans
        coordinates = np.array(data[['latitude', 'longitude']])
        kmeans = KMeans(n_clusters=6, init='k-means++', max_iter=300, n_init=10, random_state=0)
        array = kmeans.fit_predict(coordinates)
        data['living_cluster'] = array
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
    transformed_data = transform(data)
    load(transformed_data)
prepare_flat_dataset()