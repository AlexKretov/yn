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