import telegram
import matplotlib.pyplot as plt
import io
from datetime import datetime, timedelta
import pandahouse
import seaborn as sns
import pandas as pd
from airflow.decorators import dag, task

'''
Напишите систему алертов для нашего приложения.
Система должна с периодичностью каждые 15 минут проверять ключевые метрики, такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений. 

Изучите поведение метрик и подберите наиболее подходящий метод для детектирования аномалий. На практике как правило применяются статистические методы. 
В самом простом случае можно, например, проверять отклонение значения метрики в текущую 15-минутку от значения в такую же 15-минутку день назад. 

В случае обнаружения аномального значения, в чат должен отправиться алерт - сообщение со следующей информацией: метрика, ее значение, величина отклонения.
В сообщение можно добавить дополнительную информацию, которая поможет при исследовании причин возникновения аномалии, это может быть, например,  график, ссылки на дашборд/чарт в BI системе. 

Пример шаблона алерта: 

Метрика {metric_name} в срезе {group}. 
Текущее значение {current_x}. Отклонение более {x}%.
[опционально: ссылка на риалтайм чарт этой метрики в BI для более гибкого просмотра]
[опционально: ссылка на риалтайм дашборд в BI для исследования ситуации в целом]
@[опционально: тегаем ответственного/наиболее заинтересованного человека в случае отклонения конкретно 
  этой метрики в этом срезе (если такой человек есть)]
   
[график]

Автоматизируйте систему алертов с помощью Airflow:
'''

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'r.huretski-16',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 18),
}

# Интервал запуска DAG'а 
schedule_interval = '0 * * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220620',
                      'user':'student', 
                      'password':'***'
                     }

q_1 = """
        SELECT 
            toStartOfFifteenMinutes(time) as ts, 
            toDate(time) as date, 
            formatDateTime(ts, '%R') as hm, 
            count(distinct user_id) as users_feed, 
            sum(action = 'view') as views,
            sum(action = 'like') as likes,
        FROM simulator_20220620.feed_actions
        WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        """

# Функция для создания визуализации
def get_plot(df):
    plt.tight_layout()
    
    ax=sns.lineplot(x=df['ts'], y= df[metric], label='metric')
    ax=sns.lineplot(x=df['ts'], y= df['up'], label='up')
    ax=sns.lineplot(x=df['ts'], y= df['low'], label='low')

    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 2 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)

    ax.set(xlabel='time')
    ax.set(ylabel=metric)

    ax.set_title(metric)
    ax.set(ylim=(0, None))

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = '{0}.png'.format(metric)
    plt.close()             
                                  
    return plot_object

# функция предлагает алгоритм поиска аномалий в данных (межквартильный размах)
def search_anomaly(df, metric, a=4, n=5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']

    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    if df[metric].iloc[-1] > df['up'].iloc[-1] or df[metric].iloc[-1] < df['low'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df
        
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_r_huretski_report_3():
    
    @task
    def trigger_alerts(chat=None):
        #непосредственно сама система алертов
        chat_id = chat or -714461138
        my_token = '***'
        bot = telegram.Bot(token=my_token)

        msg = '''📢Алерт📢
    Метрика {metric}:
    - текущее значение {current_val:.2f}
    - отклонение от предыдущего значения {last_val_diff:.2%}
        '''
        
        data = ph.read_clickhouse(q_1, connection=connection)

        metrics_list = ['users_feed', 'views', 'likes']
        for metric in metrics_list:
            print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = search_anomaly(df, metric)

            if is_alert == 1:
                alert = msg.format(metric=metric, 
                                   current_val = df[metric].iloc[-1], 
                                   last_val_diff = abs(1 - (df[metric].iloc[-1] / df[metric].iloc[-2])))

            plot_object = get_plot(df)
            bot.sendMessage(chat_id=chat_id, text=alert)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
              
    try:
        trigger_alerts(chat=None)
    except Exception as e:
        print(e)   
    
dag_r_huretski_report_3 = dag_r_huretski_report_3()