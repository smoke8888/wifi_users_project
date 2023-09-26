import random as rn
import datetime
import pandas as pd

global session_start
session_start = datetime.datetime.now() - datetime.timedelta(days=2000)
wifi_users = []

try: 
  #читаем файла wifi_users и смотрим на datetime в последней строке
  with open('wifi_users.csv', encoding='utf-8-sig') as csvfile0:
    df_wifi_users = pd.read_csv(csvfile0, header=None)
    last_row = len(df_wifi_users.index)-1
    session_start = datetime.datetime.strptime(df_wifi_users.loc[last_row,1], '%Y-%m-%d %H:%M:%S.%f')

except:
    print("file wifi_users.csv not exist!!")\

finally:
  #открываем rtk_users и по случайному юзеру генерируем трафик, длительность сессии и 
  #место его подключения
  with open('rtk_users.csv', newline='', encoding='utf-8-sig') as csvfile:
    df_rtk_users = pd.read_csv(csvfile, header=None)
    
    for i in range(1000):

        row = rn.randint(0,len(df_rtk_users.index))
        phone_number = df_rtk_users.loc[row,0]
        age = df_rtk_users.loc[row,2]

        # генератор даты и времени
        if 0 < session_start.hour < 5:
            session_start += datetime.timedelta(hours=5)
        session_start += datetime.timedelta(minutes=rn.randint(1,10))

        # генератор выбора места от его веса для разных возрастов
        # генератор интервала времени по нормальному распределению
        # генератор количества трафика

        if age < 21:
            priority_list = rn.choices(['Супермаркет', 'Торговый центр', 'Бизнес центр', 'Кинотеатр', 'Спортзал', 'Кафе', 'Парк', 'Вокзал', 'Гостиница', 'Поликлиника', 'Общественный транспорт', 'Автозаправка', 'Автомойка'],
                                              weights=[0.1,0.2,0,0.15,0.05,0.2,0.2,0,0,0,0.1,0,0], k=1)
            session_duration = rn.randint(1, 360)
            traffic = rn.randint(100, 1000)

        if 21 <= age < 31:
            priority_list = rn.choices(['Супермаркет', 'Торговый центр', 'Бизнес центр', 'Кинотеатр', 'Спортзал', 'Кафе', 'Парк', 'Вокзал', 'Гостиница', 'Поликлиника', 'Общественный транспорт', 'Автозаправка', 'Автомойка'],
                                              weights=[0.1,0.05,0.12,0.05,0.1,0.15,0.1,0.02,0.02,0.01,0.13,0.05,0.1], k=1)
            session_duration = rn.randint(1, 300)
            traffic = rn.randint(200, 700)

        if 31 <= age < 41:
            priority_list = rn.choices(['Супермаркет', 'Торговый центр', 'Бизнес центр', 'Кинотеатр', 'Спортзал', 'Кафе', 'Парк', 'Вокзал', 'Гостиница', 'Поликлиника', 'Общественный транспорт', 'Автозаправка', 'Автомойка'],
                                              weights=[0.1,0.05,0.1,0.05,0.12,0.1,0.12,0.04,0.05,0.04,0.15,0.03,0.05], k=1)
            session_duration = rn.randint(1, 240)
            traffic = rn.randint(100, 300)

        if 41 <= age < 51:
            priority_list = rn.choices(['Супермаркет', 'Торговый центр', 'Бизнес центр', 'Кинотеатр', 'Спортзал', 'Кафе', 'Парк', 'Вокзал', 'Гостиница', 'Поликлиника', 'Общественный транспорт', 'Автозаправка', 'Автомойка'],
                                              weights=[0.12,0.02,0.08,0.08,0.05,0.07,0.14,0.07,0.07,0.1,0.12,0.03,0.05], k=1)
            session_duration = rn.randint(1, 120)
            traffic = rn.randint(50, 100)

        if 51 <= age < 61:
            priority_list = rn.choices(['Супермаркет', 'Торговый центр', 'Бизнес центр', 'Кинотеатр', 'Спортзал', 'Кафе', 'Парк', 'Вокзал', 'Гостиница', 'Поликлиника', 'Общественный транспорт', 'Автозаправка', 'Автомойка'],
                                              weights=[0.12,0.01,0.08,0.08,0.05,0.04,0.18,0.07,0.07,0.1,0.12,0.03,0.05], k=1)
            session_duration = rn.randint(1, 60)
            traffic = rn.randint(10, 100)

        if age > 60:
            priority_list = rn.choices(['Супермаркет', 'Торговый центр', 'Бизнес центр', 'Кинотеатр', 'Спортзал', 'Кафе', 'Парк', 'Вокзал', 'Гостиница', 'Поликлиника', 'Общественный транспорт', 'Автозаправка', 'Автомойка'],
                                              weights=[0.12,0.01,0.08,0.08,0.05,0.04,0.18,0.07,0.07,0.1,0.12,0.03,0.05], k=1)
            session_duration = rn.randint(1, 30)
            traffic = rn.randint(1, 10)

        place_type = priority_list[0]

        # генератор компании, где подключился пользователь
        def company(type):
                company_name_dict = {'Супермаркет':(1,2,3,4,5,6,7,8,9),
                                  'Торговый центр':(10,11,12,13,14,15,16),
                                  'Бизнес центр':(17,18,19,20,21,22,23,24,25,26,27,28),
                                  'Кинотеатр':(29,30,31,32,33,34,35,36,37),
                                  'Спортзал':(38,39,40,41,42,43,44,45,46,47),
                                  'Кафе':(48,49,50,51,52,53,54,55,56,57,58,59,60,61),
                                  'Парк':(62,63,64,65,66,67,68,69),
                                  'Вокзал':(70,71,72,73,74,75,76),
                                  'Гостиница':(77,78,79,80,81,82,83,84,85,86,87,88,89,90),
                                  'Поликлиника':(91,92,93,94,95,96),
                                  'Общественный транспорт':(97,98,99,100),
                                  'Автозаправка':(101,102,103,104,105),
                                  'Автомойка':(106,107,108,109)}
                return company_name_dict[type][rn.randint(0, len(company_name_dict[type])-1)]
        company_id = company(place_type)

        wifi_users.append([phone_number, session_start, session_duration, traffic, company_id])
        df_wifi_users = pd.DataFrame(wifi_users)

  with open('wifi_users.csv', 'a', newline='', encoding='utf-8-sig') as csvfile2:
    df_wifi_users.to_csv(csvfile2, index=False, header=False)

  print("End")