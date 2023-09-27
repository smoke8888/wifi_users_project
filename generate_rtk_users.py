from russian_names import RussianNames
import random
from scipy.stats import norm
import numpy as np
import csv

with open('rtk_users.csv', 'a', newline='', encoding='utf-8-sig') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    
    number_rtk = 9911000000
    
    for i in range(100000):
        # генератор номера телефона
        number_rtk += random.randint(1, 5)
        def_number = number_rtk

        # генератор возраста по нормальному распределению с медианой 25 лет в диапазоне от 14 до 70 лет
        def old():
            ages = np.arange(14,70,1)
            prob_list_add = norm(loc = 25 , scale = 12).cdf(ages)
            prob_list = np.subtract(prob_list_add[1:], prob_list_add[0:-1])*100
            return random.choices(ages[:-1], weights=prob_list, k=1)[0]
        age = old()

        # генератор фио
        name, gender = random.choices([[RussianNames(gender = 1).get_person(),'1'], [RussianNames(gender = 0).get_person(),'0']],
                                          weights=[0.4,0.6], k=1)[0]

        spamwriter.writerow([def_number, name, age, gender])

print("End")
