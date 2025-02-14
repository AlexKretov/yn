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
    def csv_to_s3(csv_content):
        import pandas as pd
        from dotenv import load_dotenv
        from io import StringIO
        import boto3
        # 2. Преобразование CSV в DataFrame
        df = pd.read_csv(StringIO(csv_content))
        # 3. Настройка клиента AWS S3
        load_dotenv()
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
            #region_name='YOUR_REGION'
        )

        # 4. Запись DataFrame в CSV в памяти
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # 5. Загрузка CSV в S3
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        s3_file_name = 'metro_stations.csv'

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_file_name,
            Body=csv_buffer.getvalue()
        )
    csv_content = get_csv_from_github("https://github.com/AlexKretov/yn/blob/main/metro_stations.csv")
    csv_to_s3(csv_content)
get_metro()
