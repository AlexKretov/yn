def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма
    hook = TelegramHook(token='7917536946:AAFP5M6JBkvFRiCGzRbnGaebb9yhesUaYrE', chat_id='-1002302177831')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '{-1002302177831}',
        'text': message
    }) # отправление сообщения 

def send_telegram_failure_message(context):
    from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма
    hook = TelegramHook(token='7917536946:AAFP5M6JBkvFRiCGzRbnGaebb9yhesUaYrE', chat_id='-1002302177831')
    message = "DAG failed: \nRun ID: {0}\nTask Instance Key: {1}".format(
                #context['dag'].dag_id,
                context['run_id'],
                context['task_instance_key_str']
            )   
    hook.send_message({
        'chat_id': '-1002302177831',
        'text': message
    }) # отправление сообщения 

def deal_outlier(data):
    Q1 = data['price'].quantile(0.25)
    Q3 = data['price'].quantile(0.75)
    IQR = Q3 - Q1

    filter = (data['price'] >= Q1 - 1.5 * IQR) & (data['price']<= Q3 + 1.5 *IQR)
    train2 = data.loc[filter]  
    return train2

def manhattan_distance(a, b):
    import numpy as np
    lat1, lon1 = a
    lat2, lon2 = b
    # Приблизительное преобразование градусов в километры
    lat_km = 111  # 1 градус широты примерно равен 111 км
    lon_km = 111 * np.cos(np.radians((lat1 + lat2) / 2))  # Учитываем сужение меридианов
    
    dlat = np.abs(lat2 - lat1) * lat_km
    dlon = np.abs(lon2 - lon1) * lon_km
    
    return dlat + dlon

def find_nearest_metro(lat, lon, all_stations):
    import numpy as np
    from scipy.spatial.distance import cdist
    from tqdm import tqdm
    metro_coords = all_stations[['lat', 'lng']].values
    flat_coords = np.array([[lat, lon]])
    distances = cdist(flat_coords, metro_coords, metric=manhattan_distance)[0]
    nearest_index = np.argmin(distances)
    return all_stations.iloc[nearest_index]['name'], distances[nearest_index]