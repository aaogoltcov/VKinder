import unittest
import psycopg2
import VKinder


class UnitTest(unittest.TestCase):
    def setUp(self):
        self.response = VKinder.VKinder(login, password)
        self.response.vk_session_init()
        self.db = VKinder.DBVKinder('VKinder', 'test', 'test')
        self.to_db = [{"id": 2135293, "account_link": "https://vk.com/id2135293",
                       "top-photos": [{"photo_id": 456241485,
                                       "photo": "https://sun9-39.userapi.com/c836339/v836339293/42944/f90BslXR4c8.jpg",
                                       "likes": 220},
                                      {"photo_id": 411494515,
                                       "photo": "https://sun9-28.userapi.com/c630227/v630227293/2a8ee/tyCgju9Ulpg.jpg",
                                       "likes": 214},
                                      {"photo_id": 457252355,
                                       "photo": "https://sun9-19.userapi.com/c857424/v857424435/f10c0/N9Il2vKFiDY.jpg",
                                       "likes": 167}],
                       "tags_photos": [{"photo_id": 280136998,
                                        "photo": "https://sun9-8.userapi.com/c5338/u2135293/-14/x_9af031fb.jpg",
                                        "tags": 1},
                                       {"photo_id": 280137077,
                                        "photo": "https://sun9-8.userapi.com/c5338/u2135293/-14/x_88b271a1.jpg",
                                        "tags": 1},
                                       {"photo_id": 280173092,
                                        "photo": "https://sun9-8.userapi.com/c5338/u2135293/-14/x_2a243ba6.jpg",
                                        "tags": 2},
                                       {"photo_id": 457252355,
                                        "photo": "https://sun9-39.userapi.com/c857424/v857424435/f10be/ZnbUqshOh48.jpg",
                                        "tags": 1}]}]

    def test_get_access_token(self):
        self.assertTrue(self.response.get_access_token())

    def test_execute_method_check(self):
        self.access_token = self.response.get_access_token()
        self.assertEqual(int(self.response.get_user_id()),
                         list(self.response.vk_execute_init("API.users.get()")['response'][0])[0]['id'])

    def test_db(self):
        db_name = "VKinder"
        db = VKinder.DBVKinder(db_name, 'test', 'test')
        with psycopg2.connect(dbname=db.dbname, user='test', password='test') as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                db.create_db(cursor)
                db.insert_to_db(cursor, self.to_db)
                response = db.db_show(cursor)
                self.assertEqual(response[0][2], self.to_db[0])
            cursor.close()
        connection.close()


if __name__ == '__main__':
    login = input('Логин: ')
    password = input('Пароль: ')
    unittest.main()
