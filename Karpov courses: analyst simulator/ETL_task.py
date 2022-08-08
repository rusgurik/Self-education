from datetime import datetime, timedelta

import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

'''
Решим вариант ETL задачи. Ожидается, что на выходе будет DAG в airflow, который будет считаться каждый день за вчера. 

1. Параллельно будем обрабатывать две таблицы. В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка должна быть в отдельном таске.
2. Далее объединяем две таблицы в одну.
3. Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.
4. И финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse.
5. Каждый день таблица должна дополняться новыми данными. 

Структура финальной таблицы должна быть такая:
Дата - event_date
Название среза - dimension
Значение среза - dimension_value
Число просмотров - views
Числой лайков - likes
Число полученных сообщений - messages_received
Число отправленных сообщений - messages_sent
От скольких пользователей получили сообщения - users_received
Скольким пользователям отправили сообщение - users_sent
Срез - это os, gender и age
'''

# Параметры подключения к базам данных
connection_simulator = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20220620',
    'user':'student',
    'password':'***'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'test',
    'user':'student-rw',
    'password':'***'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'r.huretski-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 6),
}

# Интервал запуска DAG'а каждый день в 11:00
schedule_interval = '0 12 * * *'

# Запросы для выгрузки данных из базы данных
query_feed = """
select 
    toDate(time) AS event_date, 
    user_id,
    gender, age, os,
    countIf(action='view') AS views,
    countIf(action='like') AS likes
from {db}.feed_actions 
where event_date = yesterday()
group by event_date, user, gender, age, os
"""

query_messenger = """
with q1 as (
select 
    toDate(time) as event_date, 
    reciever_id user_id,
    gender, age, os,
    count(user_id) as messages_received, 
    count(distinct user_id) as users_sent
from simulator_20220620.message_actions
where toDate(time) = yesterday()
group by event_date, user_id, gender, age, os
),
q2 AS 
(select
    toDate(time) as event_date,
    user_id,
    gender, age, os,
    count(reciever_id) as messages_sent, 
    count(distinct reciever_id) as users_received
from simulator_20220620.message_actions
    where toDate(time) = yesterday()
group by event_date, user_id, gender, age, os
)

select 
    event_date, user_id, 
    gender, age, os,
    messages_received, 
    messages_sent, 
    users_received, 
    users_sent
from 
    q1 full outer join q2 using (user_id, event_date)
"""

# Запросы для создания таблиц
query_test = """
CREATE TABLE IF NOT EXISTS test.r_huretski_etl_table (
    event_date Date,
    dimension String,
    dimension_value String,
    views UInt64,
    likes UInt64,
    messages_received UInt64,
    messages_sent UInt64,
    users_received UInt64,
    users_sent UInt64
)
ENGINE = MergeTree()
ORDER BY event_date
"""


# Функция для получения датафрейма из базы данных Clickhouse
def ch_get_df(query='SELECT 1', connection=connection_simulator):
    df = ph.read_clickhouse(query, connection=connection)
    return df


# Функция для загрузки датафрейма в базу данных Clickhouse
def ch_load_df(df, query='SELECT 1', connection=connection_test):
    ph.execute(query=query, connection=connection)
    ph.to_clickhouse(
        df, 'r_huretski_etl_table',
        connection=connection, index=False
    )


# Функция для DAG'а
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_r_huretski_etl():

    # Функция для извлечения данных
    @task
    def extract(query):
        df = ch_get_df(query=query)
        return df

    # Функция для объединения двух таблиц в одну
    @task
    def merge(df_feed, df_messenger):
        df_total = df_feed.merge(
            df_messenger,
            how='outer',
            on=['event_date', 'user', 'gender', 'age', 'os']
        ).dropna()
        df_total[metrics] = df_total[metrics].astype(int)
        return df_total
    
    # Функция для преобразования данных
    @task
    def transform(df, metric):
        gpoup_by = ['event_date', metric]
        columns = gpoup_by + metrics
        df_transform = df[columns] \
            .groupby(gpoup_by) \
            .sum() \
            .reset_index()
        return df_transform
    
    # Функция для сохранения данных в таблицу
    @task
    def load(df_gender, df_age, df_os):
        df_gender.rename(columns={'gender': 'dimension_value'}, inplace = True)
        df_gender.insert(1, 'dimension', 'gender')

        df_age.rename(columns={'age': 'dimension_value'}, inplace = True)
        df_age.insert(1, 'dimension', 'age')

        df_os.rename(columns={'os': 'dimension_value'}, inplace = True)
        df_os.insert(1, 'dimension', 'os')
        
        df_final = pd.concat([df_gender, df_age, df_os])
        
        ch_load_df(df_final, query_test)
        
    # Последовательный вызов задачей DAG'a
    df_feed = extract(query_feed)
    df_messenger = extract(query_messenger)
    
    df_total = merge(df_feed, df_messenger)
    
    df_gender = transform(df_total, 'gender')
    df_age = transform(df_total, 'age')
    df_os = transform(df_total, 'os')
    
    load(df_gender, df_age, df_os)

dag_r_huretski_etl = dag_r_huretski_etl()