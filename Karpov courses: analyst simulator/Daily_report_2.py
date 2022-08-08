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
Отчет по приложению
Соберите единый отчет по работе всего приложения. В отчете должна быть информация и по ленте новостей, и по сервису отправки сообщений. 

Продумайте, какие метрики необходимо отобразить в этом отчете? Как можно показать их динамику?  Приложите к отчету графики или файлы, чтобы сделать его более наглядным и информативным. Отчет должен быть не просто набором графиков или текста, а помогать отвечать бизнесу на вопросы о работе всего приложения совокупно. 

Автоматизируйте отправку отчета с помощью Airflow
'''

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'r.huretski-16',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 18),
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
            COUNT(DISTINCT user_id) as users_feed,
            sum(action = 'view') as views,
            sum(action = 'like') as likes,
            100 * likes / views as CTR,
            views + likes as events,
            COUNT(DISTINCT post_id) as posts,
            likes / users_feed as LPU
        FROM simulator_20220620.feed_actions 
        WHERE toDate(time) between today() - 8 and today() - 1
        GROUP BY date
        ORDER BY date
        """

q_2 = """
        SELECT 
            toDate(time) as date,
            COUNT(DISTINCT user_id) as users_msg,
            count(user_id) as msgs,
            msgs / users_msg as MPU
        FROM simulator_20220620.message_actions 
        WHERE toDate(time) between today() - 8 and today() - 1
        GROUP BY date
        ORDER BY date
        """

q_3 = """
        SELECT
            date,
            COUNT(DISTINCT user_id) as users,
            uniqExactIf(user_id, os='iOS') as users_ios,
            uniqExactIf(user_id, os='Android') as users_android
        FROM (
                SELECT 
                    distinct toDate(time) as date,
                    user_id,
                    os
                FROM simulator_20220620.feed_actions 
                WHERE toDate(time) between today() - 8 and today() - 1
                UNION ALL
                SELECT 
                    distinct toDate(time) as date,
                    user_id,
                    os
                FROM simulator_20220620.message_actions 
                WHERE toDate(time) between today() - 8 and today() - 1
        ) as t
        GROUP BY date
        ORDER BY date
        """

q_4 = """
        SELECT
            date,
            COUNT(DISTINCT user_id) as new_users
            uniqExactIf(user_id, source='ads') as new_users_ads
            uniqExactIf(user_id, source='organic') as new_users_organic
        FROM (
                SELECT
                    user_id,
                    min(min_dt) as date,
                    source
                FROM (
                        SELECT 
                            user_id,
                            min(toDate(time)) as min_dt,
                            source
                        FROM simulator_20220620.feed_actions 
                        WHERE toDate(time) between today() - 90 and today() - 1
                        GROUP BY user_id, source
                        UNION ALL
                        SELECT 
                            user_id,
                            min(toDate(time)) as min_dt,
                            source
                        FROM simulator_20220620.message_actions 
                        WHERE toDate(time) between today() - 90 and today() - 1
                        GROUP BY user_id, source
                ) as t
                GROUP BY user_id, source
        ) as tab
        WHERE toDate(time) between today() - 8 and today() - 1
        GROUP BY date
        """

# Функция для создания визуализации
def get_plot(data_feed, data_msg, data_new_users, data_DAU_all):
    data = pd.merge(data_feed, data_msg, on='date')
    data = pd.merge(data, data_new_users, on='date')
    data = pd.merge(data, data_DAU_all, on='date')
    
    data['events_app'] = data['events'] + data['msgs']
    
    plot_objects = []
    
    fig, axes = plt.subplots(3, figsize=(10, 14))
    fig.suptitle('Статистика по всему приложению за 7 дней')    
    app_dict = {0: {'y': ['events_app'], 'title': 'Events'},
                1: {'y': ['users', 'users_ios', 'users_ios'], 'title': 'DAU'},
                2: {'y': ['new_users', 'new_users_ads', 'new_users_organic'], 'title': 'New users'}}
    
    for i in range(3):
        for y in app_dict[i]['y']:
            sns.lineplot(ax=axes[i], data=data, x='date', y=y)
        axes[i].set_title(app_dict[(i)]['title'])
        axes[i].set(xlabel=None)
        axes[i].set(ylabel=None)
        axes[i].legend(app_dict[i]['y'])
        for ind, label in enumerate(axes[i].get_xticklabels()):
            if ind % 3 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)    
                 
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'app_stat.png'
    plot_object.seek(0)
    plt.close()
    plot_objects.append(plot_object)
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 14))
    fig.suptitle('Статистика по ленте приложения за 7 дней')    
    plot_dict = {(0, 0): {'y': 'users_feed', 'title': 'Уникальные пользователи'},
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
    plot_objects.append(plot_object)
                 
    fig, axes = plt.subplots(3, figsize=(10, 14))
    fig.suptitle('Статистика по мессенджеру приложения за 7 дней')    
    msg_dict = {0: {'y': 'users_msg', 'title': 'DAU'},
                1: {'y': 'msgs', 'title': 'Messages'},
                2: {'y': 'MPU', 'title': 'Messages per user'}}
    
    for i in range(3):
        sns.lineplot(ax=axes[i], data=data, x='date', y=msg_dict[i]['y'])
        axes[i].set_title(msg_dict[(i)]['title'])
        axes[i].set(xlabel=None)
        axes[i].set(ylabel=None)
        for ind, label in enumerate(axes[i].get_xticklabels()):
            if ind % 3 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)    
                 
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'msg_stat.png'
    plot_object.seek(0)
    plt.close()
    plot_objects.append(plot_object)
    
    return plot_objects

# Функция для DAG'а
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_r_huretski_report_2():
   
    @task   
    def app_report(chat=None):
        chat_id = chat or 454623234
        bot = telegram.Bot(token=os.environ.get('metrics_report_bot_token'))  
        
        msg = '''🗓Отчет по всему приложению за {date}🗓
    Всего событий: {events}
    🎴DAU: {users} ({to_users_day_ago:+.2%} к дню назад, {to_users_week_ago:+.2%} к недели назад)
    🎴DAU by platform:
        iOS: {users_ios} ({to_users_ios_day_ago:+.2%} к дню назад, {to_users_ios_week_ago:+.2%} к недели назад)
        Android: {users_android} ({to_users_android_day_ago:+.2%} к дню назад, {to_users_android_week_ago:+.2%} к недели назад)
    👫New users: {new_users} ({to_new_users_day_ago:+.2%} к дню назад, {to_new_users_week_ago:+.2%} к недели назад)
    👫New users by source:
        ads: {new_users_ads} ({to_new_users_ads_day_ago:+.2%} к дню назад, {to_new_users_ads_week_ago:+.2%} к недели назад)
        organic: {new_users_organic} ({to_new_users_organic_day_ago:+.2%} к дню назад, {to_new_users_organic_week_ago:+.2%} к недели назад)
        
    ЛЕНТА:
    🎴DAU: {users_feed} ({to_users_feed_day_ago:+.2%} к дню назад, {to_users_feed_week_ago:+.2%} к недели назад)
    ❤️Likes: {likes} ({to_likes_day_ago:+.2%} к дню назад, {to_likes_week_ago:+.2%} к недели назад)
    👀Views: {views} ({to_views_day_ago:+.2%} к дню назад, {to_views_week_ago:+.2%} к недели назад)
    🎯CTR: {ctr:.2f}% ({to_ctr_day_ago:+.2%} к дню назад, {to_ctr_week_ago:+.2%} к недели назад)
    🗒Posts: {posts} ({to_posts_day_ago:+.2%} к дню назад, {to_posts_week_ago:+.2%} к недели назад)
    ❣️Likes per user: {lpu:.2f} ({to_lpu_day_ago:+.2%} к дню назад, {to_lpu_week_ago:+.2%} к недели назад)
    
    МЕССЕНДЖЕР:
    🎴DAU: {users_msg} ({to_users_msg_day_ago:+.2%} к дню назад, {to_users_msg_week_ago:+.2%} к недели назад)
    🗒Messages: {msgs} ({to_msgs_day_ago:+.2%} к дню назад, {to_msgs_week_ago:+.2%} к недели назад)
    👀Messages per user: {mpu:.2f} ({to_mpu_day_ago:+.2%} к дню назад, {to_mpu_week_ago:+.2%} к недели назад)
        '''

        data_feed = ph.read_clickhouse(q_1, connection=connection)
        data_msg = ph.read_clickhouse(q_2, connection=connection)
        data_DAU_all = ph.read_clickhouse(q_3, connection=connection)
        data_new_users = ph.read_clickhouse(q_4, connection=connection)
        
        today = pd.Timestamp('now') - pd.Date0ffset(days=1)
        day_ago = today - pd.Date0ffset(days=1)
        week_ago = today - pd.Date0ffset(days=7)

        data_feed['date'] = pd.to_datetime(data_feed['date']).dt.date
        data_msg['date'] = pd.to_datetime(data_msg['date']).dt.date
        data_DAU_all['date'] = pd.to_datetime(data_DAU_all['date']).dt.date
        data_new_users['date'] = pd.to_datetime(data_new_users['date']).dt.date
        data_feed = data_feed.astype({'users_feed': int, 'views': int, 'likes': int, 'events': int, 'posts': int})
        data_msg = data_msg.astype({'users_msg': int, 'msgs': int})
        data_DAU_all = data_DAU_all.astype({'users': int, 'users_ios': int, 'users_android': int})
        data_new_users = data_new_users.astype({'new_users': int, 'new_users_ads': int, 'new_users_organic': int})

        report = msg.format(date = today.date(),                       
                            events = data_msg[data_msg['date'] == today.date()]['msgs'].iloc[0] +
                                     data_feed[data_feed['date'] == today.date()]['events'].iloc[0], 
                            
                            users = data_DAU_all[data_DAU_all['date'] == today.date()]['users'].iloc[0],
                            to_users_day_ago = data_DAU_all[data_DAU_all['date'] == today.date()]['users'].iloc[0]
                                             - data_DAU_all[data_DAU_all['date'] == day_ago.date()]['users'].iloc[0]
                                             / data_DAU_all[data_DAU_all['date'] == day_ago.date()]['users'].iloc[0],
                            to_users_week_ago = data_DAU_all[data_DAU_all['date'] == today.date()]['users'].iloc[0]
                                             - data_DAU_all[data_DAU_all['date'] == week_ago.date()]['users'].iloc[0]
                                             / data_DAU_all[data_DAU_all['date'] == week_ago.date()]['users'].iloc[0],
                            
                            users_ios = data_DAU_all[data_DAU_all['date'] == today.date()]['users_ios'].iloc[0],
                            to_users_ios_day_ago = data_DAU_all[data_DAU_all['date'] == today.date()]['users_ios'].iloc[0]
                                             - data_DAU_all[data_DAU_all['date'] == day_ago.date()]['users_ios'].iloc[0]
                                             / data_DAU_all[data_DAU_all['date'] == day_ago.date()]['users_ios'].iloc[0],
                            to_users_ios_week_ago = data_DAU_all[data_DAU_all['date'] == today.date()]['users_ios'].iloc[0]
                                             - data_DAU_all[data_DAU_all['date'] == week_ago.date()]['users_ios'].iloc[0]
                                             / data_DAU_all[data_DAU_all['date'] == week_ago.date()]['users_ios'].iloc[0],
                            
                            users_android = data_DAU_all[data_DAU_all['date'] == today.date()]['users_android'].iloc[0],
                            to_users_android_day_ago = data_DAU_all[data_DAU_all['date'] == today.date()]['users_android'].iloc[0]
                                             - data_DAU_all[data_DAU_all['date'] == day_ago.date()]['users_android'].iloc[0]
                                             / data_DAU_all[data_DAU_all['date'] == day_ago.date()]['users_android'].iloc[0],
                            to_users_android_week_ago = data_DAU_all[data_DAU_all['date'] == today.date()]['users_android'].iloc[0]
                                             - data_DAU_all[data_DAU_all['date'] == week_ago.date()]['users_android'].iloc[0]
                                             / data_DAU_all[data_DAU_all['date'] == week_ago.date()]['users_android'].iloc[0],
                            
                            new_users = data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0],
                            to_new_users_day_ago = data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0]
                                             - data_new_users[data_new_users['date'] == day_ago.date()]['new_users'].iloc[0]
                                             / data_new_users[data_new_users['date'] == day_ago.date()]['new_users'].iloc[0],
                            to_new_users_week_ago = data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0]
                                             - data_new_users[data_new_users['date'] == week_ago.date()]['new_users'].iloc[0]
                                             / data_new_users[data_new_users['date'] == week_ago.date()]['new_users'].iloc[0],
                            
                            new_users_ads = data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0],
                            to_new_users_ads_day_ago=data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0]
                                             - data_new_users[data_new_users['date'] == day_ago.date()]['new_users_ads'].iloc[0]
                                             / data_new_users[data_new_users['date'] == day_ago.date()]['new_users_ads'].iloc[0],
                            to_new_users_ads_week_ago = data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0]
                                             - data_new_users[data_new_users['date'] == week_ago.date()]['new_users_ads'].iloc[0]
                                             / data_new_users[data_new_users['date'] == week_ago.date()]['new_users_ads'].iloc[0],
                            
                            new_users_organic = data_new_users[data_new_users['date'] == today.date()]['new_users_organic'].iloc[0],
                            to_new_users_organic_day_ago = data_new_users[data_new_users['date'] == today.date()]['users_android'].iloc[0]
                                             - data_new_users[data_new_users['date'] == day_ago.date()]['new_users_organic'].iloc[0]
                                             / data_new_users[data_new_users['date'] == day_ago.date()]['new_users_organic'].iloc[0],
                            to_new_users_organic_week_ago = data_new_users[data_new_users['date'] ==today.date()]['users_android'].iloc[0]
                                             - data_new_users[data_new_users['date'] == week_ago.date()]['new_users_organic'].iloc[0]
                                             / data_new_users[data_new_users['date'] == week_ago.date()]['new_users_organic'].iloc[0],
                            
                            users_feed = data_feed[data_feed['date'] == today.date()]['users_feed'].iloc[0],
                            to_users_feed_day_ago = data_feed[data_feed['date'] == today.date()]['users_feed'].iloc[0]
                                             - data_feed[data_feed['date'] == day_ago.date()]['users_feed'].iloc[0]
                                             / data_feed[data_feed['date'] == day_ago.date()]['users_feed'].iloc[0],
                            to_users_feed_week_ago = data_feed[data_feed['date'] == today.date()]['users_feed'].iloc[0]
                                             - data_feed[data_feed['date'] == week_ago.date()]['users_feed'].iloc[0]
                                             / data_feed[data_feed['date'] == week_ago.date()]['users_feed'].iloc[0],

                            likes = data_feed[data_feed['date'] == today.date()]['likes'].iloc[0],
                            to_likes_day_ago = data_feed[data_feed['date'] == today.date()]['likes'].iloc[0]
                                             - data_feed[data_feed['date'] == day_ago.date()]['likes'].iloc[0]
                                             / data_feed[data_feed['date'] == day_ago.date()]['likes'].iloc[0],
                            to_likes_week_ago = data_feed[data_feed['date'] == today.date()]['likes'].iloc[0]
                                             - data_feed[data_feed['date'] == week_ago.date()]['likes'].iloc[0]
                                             / data_feed[data_feed['date'] == week_ago.date()]['likes'].iloc[0],

                            views = data_feed[data_feed['date'] == today.date()]['views'].iloc[0],
                            to_views_day_ago = data_feed[data_feed['date'] == today.date()]['views'].iloc[0]
                                             - data_feed[data_feed['date'] == day_ago.date()]['views'].iloc[0]
                                             / data_feed[data_feed['date'] == day_ago.date()]['views'].iloc[0],
                            to_views_week_ago = data_feed[data_feed['date'] == today.date()]['views'].iloc[0]
                                             - data_feed[data_feed['date'] == week_ago.date()]['views'].iloc[0]
                                             / data_feed[data_feed['date'] == week_ago.date()]['views'].iloc[0],

                            ctr = data_feed[data_feed['date'] == today.date()]['CTR'].iloc[0],
                            to_ctr_day_ago = data_feed[data_feed['date'] == today.date()]['CTR'].iloc[0]
                                             - data_feed[data_feed['date'] == day_ago.date()]['CTR'].iloc[0]
                                             / data_feed[data_feed['date'] == day_ago.date()]['CTR'].iloc[0],
                            to_ctr_week_ago = data_feed[data_feed['date'] == today.date()]['CTR'].iloc[0]
                                             - data_feed[data_feed['date'] == week_ago.date()]['CTR'].iloc[0]
                                             / data_feed[data_feed['date'] == week_ago.date()]['CTR'].iloc[0],

                            posts = data_feed[data_feed['date'] == today.date()]['posts'].iloc[0],
                            to_posts_day_ago = data_feed[data_feed['date'] == today.date()]['posts'].iloc[0]
                                             - data_feed[data_feed['date'] == day_ago.date()]['posts'].iloc[0]
                                             / data_feed[data_feed['date'] == day_ago.date()]['posts'].iloc[0],
                            to_posts_week_ago = data_feed[data_feed['date'] == today.date()]['posts'].iloc[0]
                                             - data_feed[data_feed['date'] == week_ago.date()]['posts'].iloc[0]
                                             / data_feed[data_feed['date'] == week_ago.date()]['posts'].iloc[0],

                            lpu = data_feed[data_feed['date'] == today.date()]['LPU'].iloc[0],
                            to_lpu_day_ago = data_feed[data_feed['date'] == today.date()]['LPU'].iloc[0]
                                             - data_feed[data_feed['date'] == day_ago.date()]['LPU'].iloc[0]
                                             / data_feed[data_feed['date'] == day_ago.date()]['LPU'].iloc[0],
                            to_lpu_week_ago = data_feed[data_feed['date'] == today.date()]['LPU'].iloc[0]
                                             - data_feed[data_feed['date'] == week_ago.date()]['LPU'].iloc[0]
                                             / data_feed[data_feed['date'] == week_ago.date()]['LPU'].iloc[0],
                            
                            users_msg = data_msg[data_msg['date'] == today.date()]['users_msg'].iloc[0],
                            to_users_msg_day_ago = data_msg[data_msg['date'] == today.date()]['users_msg'].iloc[0]
                                             - data_msg[data_msg['date'] == day_ago.date()]['users_msg'].iloc[0]
                                             / data_msg[data_msg['date'] == day_ago.date()]['users_msg'].iloc[0],
                            to_users_msg_week_ago = data_msg[data_msg['date'] == today.date()]['users_msg'].iloc[0]
                                             - data_msg[data_msg['date'] == week_ago.date()]['users_msg'].iloc[0]
                                             / data_msg[data_msg['date'] == week_ago.date()]['users_msg'].iloc[0],

                            msgs = data_msg[data_msg['date'] == today.date()]['msgs'].iloc[0],
                            to_msgs_day_ago = data_msg[data_msg['date'] == today.date()]['msgs'].iloc[0]
                                             - data_msg[data_msg['date'] == day_ago.date()]['msgs'].iloc[0]
                                             / data_msg[data_msg['date'] == day_ago.date()]['msgs'].iloc[0],
                            to_msgs_week_ago = data_msg[data_msg['date'] == today.date()]['msgs'].iloc[0]
                                             - data_msg[data_msg['date'] == week_ago.date()]['msgs'].iloc[0]
                                             / data_msg[data_msg['date'] == week_ago.date()]['msgs'].iloc[0],

                            mpu = data_msg[data_msg['date'] == today.date()]['MPU'].iloc[0],
                            to_mpu_day_ago = data_msg[data_msg['date'] == today.date()]['MPU'].iloc[0]
                                             - data_msg[data_msg['date'] == day_ago.date()]['MPU'].iloc[0]
                                             / data_msg[data_msg['date'] == day_ago.date()]['MPU'].iloc[0],
                            to_mpu_week_ago = data_msg[data_msg['date'] == today.date()]['MPU'].iloc[0]
                                             - data_msg[data_msg['date'] == week_ago.date()]['MPU'].iloc[0]
                                             / data_msg[data_msg['date'] == week_ago.date()]['MPU'].iloc[0],
                           )
                 
        plot_objects = get_plot(data_feed, data_msg, data_new_users, data_DAU_all)
        bot.sendMessage(chat_id=chat_id, text=report)
        for plot_object in plot_objects:
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    try:
        app_report(chat=None)
    except Exception as e:
        print(e)
        
dag_r_huretski_report_2 = dag_r_huretski_report_2()