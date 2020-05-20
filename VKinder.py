import datetime
import json
import re
import ssl
import time
from pprint import pprint

import psycopg2
import pymorphy2
import requests
import vk_api
from nltk.corpus import stopwords
import psycopg2._psycopg
from tqdm import tqdm


class DBVKinder:  # Класс для работы с БД PostgreSQL

    def __init__(self, dbname, user, password):
        self.dbname = dbname
        self.user = user
        self.password = password

    def db_connection(self):
        return self.dbname, self.user, self.password

    @staticmethod
    def create_db(cursor):  # создает таблицы
        cursor.execute("DROP TABLE IF EXISTS USER_LIST CASCADE;"
                       "CREATE TABLE USER_LIST "
                       "(ID serial PRIMARY KEY not null, "
                       "USER_ID integer, "
                       "DATA jsonb, "
                       "IS_FAVORITE boolean DEFAULT False, "
                       "IS_BLOCKED boolean DEFAULT False);")

    @staticmethod
    def insert_to_db(cursor, to_db):  # Записьм данных по ТОП списку пользователей
        for item in to_db:
            from psycopg2._json import Json
            cursor.execute("INSERT INTO USER_LIST(USER_ID, DATA) values(%s, %s);", (item['id'], Json(item)))
        print(f'\t - данные записаны в БД')

    @staticmethod
    def db_update(cursor, is_value, user_id):  # Обновление статусов IS_BLOCKED и IS_FAVORITE
        if is_value == "IS_BLOCKED":
            cursor.execute("UPDATE USER_LIST SET IS_BLOCKED = True WHERE USER_ID = %s;", (user_id,))
        elif is_value == "IS_FAVORITE":
            cursor.execute("UPDATE USER_LIST SET IS_FAVORITE = True WHERE USER_ID = %s;", (user_id,))
        print(f'\t - данные в БД обновлены')

    @staticmethod
    def db_show(cursor):  # Просмотр содержимого таблицы в БД
        cursor.execute("SELECT * FROM USER_LIST;")
        return cursor.fetchall()

    @staticmethod
    def db_show_user(cursor):  # Просмот списка пользоватлей и статусов IS_BLOCKED, IS_FAVORITE
        cursor.execute("SELECT USER_ID, IS_FAVORITE, IS_BLOCKED FROM USER_LIST;")
        return cursor.fetchall()

    @staticmethod
    def get_blocked_list(cursor): # Получение списка только блокированных пользователей
        blocked_list = []
        cursor.execute("SELECT USER_ID FROM USER_LIST WHERE IS_BLOCKED = True;")
        for item in cursor.fetchall():
            blocked_list.extend(item)
        return blocked_list


class VKinder:

    @staticmethod
    def default_value(item, value):  # Запись пустого значения в случае отсуствия данных в запросе
        try:
            return item[f'{value}']
        except KeyError:
            return ""

    @staticmethod
    def default_value_for_birth_year(item, value):  # Запись значения 1900 в случае отсуствия доступа к данным ДР
        try:
            return datetime.datetime.strptime(item[f'{value}'], '%d.%m.%Y').date().year
        except (ValueError, KeyError):
            return 1900

    @staticmethod
    def is_digit(string):  # Проверка значения на число
        if string.isdigit():
            return True
        else:
            try:
                float(string)
                return True
            except (ValueError, AttributeError):
                return False

    @staticmethod
    def words_normalization(list):  # Нормализация текстовой информации для поиска (сравнения с данными пользователя)
        try:
            _create_unverified_https_context = ssl._create_unverified_context  # Получение сертификата
        except AttributeError:
            pass
        else:
            ssl._create_default_https_context = _create_unverified_https_context
        morph = pymorphy2.MorphAnalyzer()  # Необходимо для приведения слов в родительный падеж
        for item in list:
            for key, value in item.items():
                if isinstance(value, str) and len(value) > 0 and key != 'domain':
                    value = re.sub(re.compile(r'[\W_()]+'), ' ', value.lower()).split()  # Разделение тестка на слова

                    # Морфолгическая обработка и удаление СТОП-слов
                    value = [morph.parse(word)[0][2] for word in value if
                             not word in (stopwords.words('russian') or stopwords.words('english'))]
                    item[key] = value
        return list

    @staticmethod
    def get_mutual_interests_score(person_list, users_list, field, coef=0):  # Вычисление совпадений в интересах
        for person_item in person_list:
            for pesron_key, person_value in person_item.items():
                if pesron_key == field:
                    for person_word in person_value:
                        for user_item in users_list:
                            interests_score = int()
                            for user_key, user_value in user_item.items():
                                if user_key == pesron_key:
                                    for user_word in user_value:
                                        if person_word == user_word:
                                            interests_score += 1
                            if interests_score:
                                interests_score += coef
                            user_item.update({f'{pesron_key}_score': interests_score})

    @staticmethod
    def set_coef(score, coef=0, type=True):  # Установка поэффициента влияния разных интересов
        if score:
            if type:
                score += coef
            else:
                score = coef - score
        return score

    @staticmethod
    def is_value(item, value):  # Проверка доступности значения в ответе
        try:
            return item[f'{value}']
        except KeyError:
            return 0

    def __init__(self, login, user_password, vk_session=None, vk=None, vk_tools=None, vk_request_pool=None,
                 vk_request_result=None, vk_request_one_param_pool=None, **params):
        self.vk_session = vk_session
        self.user_id = int()
        self.login = login
        self.user_password = user_password
        self.vk = vk
        self.vk_tools = vk_tools
        self.access_token = str()
        self.vk_request_pool = vk_request_pool
        self.vk_request_result = vk_request_result
        self.vk_request_one_param_pool = vk_request_one_param_pool
        self.params = params
        self.user_search_list = []
        self.user_search_list_of_friends = []
        self.mutual_friends_list = []
        self.result_list_of_candidates = []
        self.birth_year_person = int()
        self.person_groups = []
        self.fields = str()
        self.interim_list_of_top_users = []
        self.response_list_of_top_users = []
        self.RPS_DELAY = 0.4
        self.last_request = 0.0
        self.photo_tags = {}
        self.session = requests.Session()

    def close(self):
        self.session.close()

    def vk_session_init(self):  # Инициализация методов vk_api
        self.vk_session = vk_api.VkApi(self.login, self.user_password)
        self.vk_session.auth()
        self.vk = self.vk_session.get_api()
        self.vk_tools = vk_api.VkTools(self.vk_session)
        self.vk_request_pool = vk_api.VkRequestsPool(self.vk_session)
        self.vk_request_result = vk_api.requests_pool.RequestResult
        self.vk_request_one_param_pool = vk_api.requests_pool
        self.birth_year_person = datetime.datetime.strptime(self.vk.account.getProfileInfo()['bdate'],
                                                            '%d.%m.%Y').date().year
        self.person_groups = self.vk.groups.get(user_id=self.user_id)
        print('\t - инициализация vk api сессии')

    def get_access_token(self):  # Получение токена
        with open('vk_config.v2.json', encoding='UTF-8') as vk_config:
            data = json.load(vk_config)
            self.access_token = (data[self.login]['token']
            [list(data[self.login]['token'].keys())[0]]
            [list(data[self.login]['token'][list(data[self.login]['token'].keys())[0]].keys())[0]]
            ['access_token'])
        print('\t - получен access token')
        return self.access_token

    def get_user_id(self):  # Получение id пользователя
        with open('vk_config.v2.json', encoding='UTF-8') as vk_config:
            data = json.load(vk_config)
            self.user_id = (data[self.login]['token']
            [list(data[self.login]['token'].keys())[0]]
            [list(data[self.login]['token'][list(data[self.login]['token'].keys())[0]].keys())[0]]
            ['user_id'])
        print(f'\t - получен id ({self.user_id}) пользователя')
        return self.user_id

    def get_tags_photos(self, owner_id):  # Получение всех фото пользователя с атрибутом tags
        delay = self.RPS_DELAY - (time.time() - self.last_request)
        if delay > 0:
            time.sleep(delay)
        self.photo_tags = requests.post(url="https://api.vk.com/method/execute?",
                                        data={"code": "return [API.photos.get({'owner_id': %d, 'album_id': 'wall', "
                                                      "'extended': '1'}).items, "
                                                      "API.photos.get({'owner_id': %d, 'album_id': 'profile', "
                                                      "'extended': '1'}).items, "
                                                      "API.photos.get({'owner_id': %d, 'album_id': 'saved', "
                                                      "'extended': '1'}).items];" % (owner_id, owner_id, owner_id),
                                              "access_token": self.access_token,
                                              "v": "5.61"}).json()
        self.last_request = time.time()
        return self.photo_tags

    def vk_execute_init(self, code_request):  # Выполнение метода execute VK API
        self.RPS_DELAY = 0
        delay = self.RPS_DELAY - (time.time() - self.last_request)
        if delay > 0:
            time.sleep(delay)
        response = requests.post(url="https://api.vk.com/method/execute?",
                                 data={"code": "return [%s];" % code_request, "access_token": self.access_token,
                                       "v": "5.61"}).json()
        self.last_request = time.time()
        return response

    def user_search(self, blocked_list):  # Поиск кандидатов с учетом критериев поиска
        self.fields = 'common_count, activities, bdate, music, movies, tv, books, games, domain, interests, ' \
                      'has_photo, occupation '
        self.params.update(dict(fields=self.fields))
        for item in self.vk_tools.get_all('users.search', 1000, self.params)['items']:
            if item['id'] in blocked_list:  # Исключение блокированных пользователей
                pass
            else:
                self.user_search_list.append(item['id'])
                self.result_list_of_candidates.append({'id': item['id'],
                                                       'mutual_friends_score': self.set_coef(
                                                           self.default_value(item, 'common_count'), 500),
                                                       'activities': self.default_value(item, 'activities'),
                                                       'bdate_difference_score': self.set_coef(abs(
                                                           self.default_value_for_birth_year(item, 'bdate') -
                                                           self.birth_year_person), 400, False),
                                                       'music': self.default_value(item, 'music'),
                                                       'movies': self.default_value(item, 'movies'),
                                                       'tv': self.default_value(item, 'tv'),
                                                       'books': self.default_value(item, 'books'),
                                                       'games': self.default_value(item, 'games'),
                                                       'domain': self.default_value(item, 'domain'),
                                                       'interests': self.default_value(item, 'interests'),
                                                       'has_photo': self.default_value(item, 'has_photo'),
                                                       'occupation': self.default_value(item, 'occupation')})
        print(f'\t - получены результаты поиска')
        return self.result_list_of_candidates

    def get_score_for_interests(self):  # Расчет очков для интересов (с учетом приоритетов по некоторым интересам)
        print(f'\t - запускаем поиск совпадений по интересам')
        person_list = self.words_normalization(self.vk.users.get(fields=self.fields))
        users_list = self.words_normalization(self.result_list_of_candidates)
        self.get_mutual_interests_score(person_list, users_list, 'activities', 0)
        self.get_mutual_interests_score(person_list, users_list, 'music', 200)
        self.get_mutual_interests_score(person_list, users_list, 'movies', 0)
        self.get_mutual_interests_score(person_list, users_list, 'tv', 0)
        self.get_mutual_interests_score(person_list, users_list, 'books', 100)
        self.get_mutual_interests_score(person_list, users_list, 'games', 0)
        self.get_mutual_interests_score(person_list, users_list, 'interests', 0)
        print(f'\t - завершен поиск совпадений по интересам')

    def get_mutual_groups(self):  # Получение и вычисление общих групп
        print(f'\t - запрос информации по группам для {len(self.result_list_of_candidates)} пользоватей')
        list_of_users_groups = []
        code_request = str()
        groups_count = int()
        # Получение списка всех групп
        for i, id in tqdm(enumerate(self.user_search_list)):
            code_pattern = "{'%d': API.groups.get({'user_id': %d, 'count': 1000}).items}" % (id, id)
            if code_request:
                code_request = code_request + ', ' + code_pattern
            else:
                code_request = code_pattern
            # Формирование запрос не более 25 штук в execute
            if ((i % 25) == 0) or (i == (len(self.user_search_list) - 1)):
                try:
                    list_of_users_groups.extend(self.vk_execute_init(code_request)['response'])
                except KeyError:
                    print('Too many requests! Sleeping 0.5 sec...')
                    time.sleep(0.5)
                    list_of_users_groups.extend(self.vk_execute_init(code_request)['response'])
                code_request = str()
        # Вычисление общих групп
        for item in self.result_list_of_candidates:
            for groups in list_of_users_groups:
                for key, value in groups.items():
                    if int(key) == int(item['id']):
                        try:
                            item['mutual_groups'] = len(set(self.person_groups['items']).intersection(set(value)))
                            groups_count += len(value)
                        except TypeError:
                            item['mutual_groups'] = 0
                        if item['mutual_groups'] > 0:
                            item['mutual_groups'] += 300
        print(f'\t - завершен анализ {groups_count} групп')

    def final_score(self):  # Рассчет суммарного количество очков для пользователей
        for item in self.result_list_of_candidates:
            item['final_score'] = item['bdate_difference_score'] + \
                                  item['mutual_friends_score'] + \
                                  self.is_value(item, 'activities_score') + \
                                  self.is_value(item, 'music_score') + \
                                  self.is_value(item, 'movies_score') + \
                                  self.is_value(item, 'tv_score') + \
                                  self.is_value(item, 'books_score') + \
                                  self.is_value(item, 'games_score') + \
                                  self.is_value(item, 'interests_score')
        self.result_list_of_candidates = sorted(self.result_list_of_candidates, key=lambda i: i['final_score'],
                                                reverse=True)
        print(f'\t - рассчитан итоговый score, кандидаты отсортированы')

    def get_top_list(self):  # Получения ТОП списка по итогам поиска и вычислений
        self.response_list_of_top_users = []
        for i, item in enumerate(self.result_list_of_candidates):
            if item['id'] in self.interim_list_of_top_users:
                pass
            else:
                # Получение фото профиля и сортировка по количеству лайков
                top_photos = []
                try:
                    for photo in sorted(
                            self.vk.photos.get(owner_id=item['id'], album_id='profile', extended='1')['items'],
                            key=lambda i: i['likes']['count'], reverse=True):
                        if len(top_photos) < 3:
                            top_photos.append({'photo_id': photo['id'], 'photo': photo['sizes'][-1]['url'],
                                               'likes': photo['likes']['count']})
                except vk_api.exceptions.ApiError:
                    top_photos.append('This profile is private')
                # Получение всех отмеченных фото
                tags_photos = []
                self.get_tags_photos(item['id'])
                try:
                    self.photo_tags = self.photo_tags['response']
                except KeyError:
                    print('Too many requests! Sleeping 0.5 sec...')
                    time.sleep(0.5)
                    self.get_tags_photos(item['id'])
                for tags in self.photo_tags:
                    try:
                        for tag in tags:
                            if tag['tags']['count'] > 0:
                                tags_photos.append({'photo_id': tag['id'],
                                                    'photo': tag['photo_604'],
                                                    'tags': tag['tags']['count']})
                    except TypeError:
                        continue
                # Добавление пользователя в ТОП список
                self.response_list_of_top_users.append({'id': item['id'],
                                                        'account_link': ('https://vk.com/id' + str(item['id'])),
                                                        'top-photos': top_photos, 'tags_photos': tags_photos})
            # ТОП список не более 10 человек
            if (len(self.response_list_of_top_users) == 10) or (i == (len(self.result_list_of_candidates) - 1)):
                break
        # Запись результатов поисков для исключения дублирования в следующих поисках
        for item in self.response_list_of_top_users:
            self.interim_list_of_top_users.extend([item['id']])
        # Запись ТОП листа в JSON файл
        with open('VKinder.json', 'w') as file:
            file.write(json.dumps(self.response_list_of_top_users))
        print(f'\t - ТОП-10 записаны JSON файл')
        return self.response_list_of_top_users

    def set_like_unlike_photo(self, photo_id, action_type):  # Like и Unlike фото
        if action_type == 'like':
            self.vk.likes.add(type='photo', item_id=photo_id)
            print(f'\t - установлен like для фото {photo_id}')
        elif action_type == 'unlike':
            self.vk.likes.delete(type='photo', item_id=photo_id)
            print(f'\t - установлен unlike для фото {photo_id}')


def main():
    HELP = "HELP TUTORIAL: " \
           "\nsearch      - поиск с помощью VKinder \n" \
           "like        - поставить like для фото \n" \
           "unlike      - поставить unlike для фото \n" \
           "show        - показать результаты поиска из БД \n" \
           "block       - поставить блок на пользователя в БД\n" \
           "favorite    - указать избранного пользователя в БД\n" \
           "q           - выйти\n"
    db_name = "VKinder"
    command = str()
    sub_command = str()
    kwargs = {}
    db = DBVKinder(db_name, 'test', 'test')
    with psycopg2.connect(dbname=db.dbname, user=db.user, password=db.password) as connection:
        print(f"Подключились к БД {db_name}")
        with connection.cursor() as cursor:
            db.create_db(cursor)
            login = input('Введите логин: ')
            password = input('Введите пароль: ')
            while command != "q":
                command = input('Введите команду (help): ')
                if command == 'search':
                    print('Введите остальные параметры поиска:')
                    while sub_command != "q":
                        sub_command = input('\t- введите ключ: ')
                        key = sub_command
                        sub_command = input('\t- введите значение: ')
                        value = sub_command
                        if (key and value) and (key != 'q' or value != 'q'):
                            kwargs.update({key: value})
                    sub_command = str()
                    # kwargs = {'sex': '1', 'hometown': 'Череповец', 'age_from': '25', 'age_to': '35'}  # delete
                    users = VKinder(login, password, kwargs)
                    print('Запуск VKinder:')
                    start_time = time.time()
                    users.vk_session_init()
                    users.get_access_token()
                    users.get_user_id()
                    users.user_search(
                        db.get_blocked_list(cursor))  # Результаты поиска с учетом блокированных пользователей
                    users.get_score_for_interests()
                    users.get_mutual_groups()
                    users.final_score()
                    to_db = users.get_top_list()
                    db.insert_to_db(cursor, to_db)
                    print(f'Время выполнения: {round((time.time() - start_time), 0)} сек.')
                elif command == 'like' or command == 'unlike':
                    photo_id = input('Укажите id фото: ')
                    users.set_like_unlike_photo(photo_id, 'command')
                elif command == 'block' or command == 'favorite':
                    user_id = input('Укажите id пользователя: ')
                    if command == 'block':
                        db.db_update(cursor, 'IS_BLOCKED', user_id)
                    elif command == 'favorite':
                        db.db_update(cursor, 'IS_FAVORITE', user_id)
                elif command == 'show':
                    pprint(db.db_show(cursor))
                elif command == 'help':
                    print(HELP)
        cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
