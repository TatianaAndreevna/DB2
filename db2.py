import csv
import re
from pymongo import MongoClient
import pymongo
from pprint import pprint
from datetime import datetime


client = MongoClient()
concert_tickets_db = client.concert_tickets


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        concert_tickets_data = list()
        for cell in reader:
            cell['Цена'] = int(cell['Цена'])
            date_str = cell['Дата'] + '.{}'.format(datetime.now().year)
            concert_tickets_data.append(cell)
        concert_tickets_list = db.concert_tickets_list
        concert_tickets_list.insert_many(concert_tickets_data)


def find_cheapest(db):
    """
    Найти самые дешевые билеты
    Документация: https://docs.mongodb.com/manual/reference/operator/aggregation/sort/
    """
    cheapest_tickets = list(db.concert_tickets_list.find().sort('Цена', pymongo.ASCENDING))
    print('Самый дешевый билет на концерт {} стоит {}'.format(cheapest_tickets[0]['Исполнитель'],
                                                              cheapest_tickets[0]['Цена']))


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке),
    и выведите их по возрастанию цены
    """
    regex = re.compile(r'\w*{}\w*'.format(name))
    tickets = list(db.concert_tickets_list.find({'Исполнитель': regex}).sort('Цена', pymongo.ASCENDING))
    for ticket in tickets:
        print('{} {} {}'.format(ticket['Дата'], ticket['Исполнитель'], ticket['Цена']))


if __name__ == '__main__':
    read_data('artists.csv', concert_tickets_db)
    find_cheapest(concert_tickets_db)
    find_by_name('L', concert_tickets_db)
