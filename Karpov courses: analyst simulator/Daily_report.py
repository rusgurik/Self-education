import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta

'''
Отчет по ленте

Напишите скрипт для сборки отчета по ленте новостей, который отправляется в телеграм каждое утро. Отчет должен состоять из двух частей:
1) текст с информацией о значениях ключевых метрик за предыдущий день
2) график с значениями метрик за предыдущие 7 дней

Отобразите в отчете следующие ключевые метрики: 
- DAU 
- Просмотры
- Лайки
- CTR
'''

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'r.huretski-16',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 6),
}

# Интервал запуска DAG'а каждый день в 11:00
schedule_interval = '0 11 * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220620',
                      'user':'student', 
                      'password':'***'
                     }

q_1 = """
        SELECT 
            toDate(time) as date,
            COUNT(DISTINCT user_id) as DAU,
            sum(action = 'view') as views,
            sum(action = 'like') as likes,
            100 * likes / views as CTR,
            views + likes as events,
            COUNT(DISTINCT post_id) as posts,
            likes / DAU as LPU
        FROM simulator_20220620.feed_actions 
        WHERE toDate(time) between today() - 8 and today() - 1
        GROUP BY date
        ORDER BY date
        """

# Функция для создания визуализации
def get_plot(data):
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    
    fig.suptitle('Статистика по ЛЕНТЕ за предыдущие 7 дней')
    
    plot_dict = {(0, 0): {'y': 'DAU', 'title': 'Уникальные пользователи'},
                {(0, 1): {'y': 'likes', 'title': 'likes'},
                {(1, 0): {'y': 'views', 'title': 'views'},
                {(1, 1): {'y': 'CTR', 'title': 'CTR'}}
    
    for i in range(2):
        for j in range(2):
            sns.lineplot(ax=axes[i, j], data=data, x='date', y=plot_dict[(i, j)]['y'])
            axes[i, j].set_title(plot_dict[(i, j)]['title'])
            axes[i, j].set(xlabel=None)
            axes[i, j].set(ylabel=None)
            for ind, label in enumerate(axes[i, j].get_xticklabels()):
                 if ind % 3 == 0:
                     label.set_visible(True)
                 else:
                     label.set_visible(False)
                 
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'feed_stat.png'
    plot_object.seek(0)
    plt.close()
    return plot_object

# Функция для DAG'а
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_r_huretski_report():
   
    @task   
    def feed_report(chat=None):
        chat_id = chat or 454623234
        bot = telegram.Bot(token=os.environ.get('metrics_report_bot_token'))         
        msg = '''🗓Отчет по ЛЕНТЕ за {date}🗓
    Events: {events}
    🎴DAU: {users} ({to_users_day_ago:+.2%} к дню назад, {to_users_week_ago:+.2%} к недели назад)
    ❤️Likes: {likes} ({to_likes_day_ago:+.2%} к дню назад, {to_likes_week_ago:+.2%} к недели назад)
    👀Views: {views} ({to_views_day_ago:+.2%} к дню назад, {to_views_week_ago:+.2%} к недели назад)
    🎯CTR: {ctr:.2f}% ({to_ctr_day_ago:+.2%} к дню назад, {to_ctr_week_ago:+.2%} к недели назад)
    🗒Posts: {posts} ({to_posts_day_ago:+.2%} к дню назад, {to_posts_week_ago:+.2%} к недели назад)
    ❣️Likes per user: {lpu} ({to_lpu_day_ago:+.2%} к дню назад, {to_lpu_week_ago:+.2%} к недели назад)
        '''

        data = ph.read_clickhouse(q_1, connection=connection)

        today = pd.Timestamp('now') - pd.Date0ffset(days=1)
        day_ago = today - pd.Date0ffset(days=1)
        week_ago = today - pd.Date0ffset(days=7)

        data['date'] = pd.to_datetime(data['date']).dt.date
        data = data.astype({'DAU': int, 'views': int, 'likes': int, 'events': int, 'posts': int})

        report = msg.format(date = today.date(),                       
                            events = data[data['date'] == today.date()]['events'].iloc[0],                 
                            users = data[data['date'] == today.date()]['DAU'].iloc[0],
                            to_users_day_ago = data[data['date'] == today.date()]['DAU'].iloc[0]
                                             - data[data['date'] == day_ago.date()]['DAU'].iloc[0]
                                             / data[data['date'] == day_ago.date()]['DAU'].iloc[0],
                            to_users_week_ago = data[data['date'] == today.date()]['DAU'].iloc[0]
                                             - data[data['date'] == week_ago.date()]['DAU'].iloc[0]
                                             / data[data['date'] == week_ago.date()]['DAU'].iloc[0],

                            likes = data[data['date'] == today.date()]['likes'].iloc[0],
                            to_likes_day_ago = data[data['date'] == today.date()]['likes'].iloc[0]
                                             - data[data['date'] == day_ago.date()]['likes'].iloc[0]
                                             / data[data['date'] == day_ago.date()]['likes'].iloc[0],
                            to_likes_week_ago = data[data['date'] == today.date()]['likes'].iloc[0]
                                             - data[data['date'] == week_ago.date()]['likes'].iloc[0]
                                             / data[data['date'] == week_ago.date()]['likes'].iloc[0],

                            views = data[data['date'] == today.date()]['views'].iloc[0],
                            to_views_day_ago = data[data['date'] == today.date()]['views'].iloc[0]
                                             - data[data['date'] == day_ago.date()]['views'].iloc[0]
                                             / data[data['date'] == day_ago.date()]['views'].iloc[0],
                            to_views_week_ago = data[data['date'] == today.date()]['views'].iloc[0]
                                             - data[data['date'] == week_ago.date()]['views'].iloc[0]
                                             / data[data['date'] == week_ago.date()]['views'].iloc[0],

                            ctr = data[data['date'] == today.date()]['CTR'].iloc[0],
                            to_ctr_day_ago = data[data['date'] == today.date()]['CTR'].iloc[0]
                                             - data[data['date'] == day_ago.date()]['CTR'].iloc[0]
                                             / data[data['date'] == day_ago.date()]['CTR'].iloc[0],
                            to_ctr_week_ago = data[data['date'] == today.date()]['CTR'].iloc[0]
                                             - data[data['date'] == week_ago.date()]['CTR'].iloc[0]
                                             / data[data['date'] == week_ago.date()]['CTR'].iloc[0],

                            posts = data[data['date'] == today.date()]['posts'].iloc[0],
                            to_posts_day_ago = data[data['date'] == today.date()]['posts'].iloc[0]
                                             - data[data['date'] == day_ago.date()]['posts'].iloc[0]
                                             / data[data['date'] == day_ago.date()]['posts'].iloc[0],
                            to_posts_week_ago = data[data['date'] == today.date()]['posts'].iloc[0]
                                             - data[data['date'] == week_ago.date()]['posts'].iloc[0]
                                             / data[data['date'] == week_ago.date()]['posts'].iloc[0],

                            lpu = data[data['date'] == today.date()]['LPU'].iloc[0],
                            to_lpu_day_ago = data[data['date'] == today.date()]['LPU'].iloc[0]
                                             - data[data['date'] == day_ago.date()]['LPU'].iloc[0]
                                             / data[data['date'] == day_ago.date()]['LPU'].iloc[0],
                            to_lpu_week_ago = data[data['date'] == today.date()]['LPU'].iloc[0]
                                             - data[data['date'] == week_ago.date()]['LPU'].iloc[0]
                                             / data[data['date'] == week_ago.date()]['LPU'].iloc[0]
                           )
        
        plot_object = get_plot(data)
        bot.sendMessage(chat_id=chat_id, text=report)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    try:
        feed_report(chat=None)
    except Exception as e:
        print(e)
        
dag_r_huretski_report = dag_r_huretski_report()