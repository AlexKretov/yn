def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма
    hook = TelegramHook(token='##################', chat_id='##################')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '##################',
        'text': message
    }) # отправление сообщения 

def send_telegram_failure_message(context):
    from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма
    hook = TelegramHook(token='##################', chat_id='##################')
    message = "DAG failed: \nRun ID: {0}\nTask Instance Key: {1}".format(
                #context['dag'].dag_id,
                context['run_id'],
                context['task_instance_key_str']
            )   
    hook.send_message({
        'chat_id': '##################',
        'text': message
    }) # отправление сообщения 

def deal_outlier(data):
    Q1 = data['price'].quantile(0.25)
    Q3 = data['price'].quantile(0.75)
    IQR = Q3 - Q1

    filter = (data['price'] >= Q1 - 1.5 * IQR) & (data['price']<= Q3 + 1.5 *IQR)
    train2 = data.loc[filter]  
    return train2