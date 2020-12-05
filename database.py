import sqlite3

import common

class Db:
    def __init__(self, filename):
        self.__conn = sqlite3.connect(filename)

    def close(self):
        self.__conn.close()

    def init(self):
        with self.__conn:
            self.__conn.execute('create table users(id integer primary key, gl_name text unique)')

    def __insert_user(self, id):
        with self.__conn:
            self.__conn.execute('insert into users(id) values(?) on conflict(id) do nothing', (id,))

    def delete_user(self, id):
        with self.__conn:
            self.__conn.execute('delete from users where id = ?', (id,))

    def set_user_gl_name(self, id, gl_name):
        self.__insert_user(id)

        try:
            with self.__conn:
                self.__conn.execute('update users set gl_name = ? where id = ?', (gl_name, id))
        except sqlite3.IntegrityError:
            # Assume duplicate gl_name
            conflicting_id = self.try_user_id_from_gl_name(gl_name)
            if conflicting_id is None:
                raise common.Error('Someone else has taken that name already.')
            else:
                raise common.Error(f'That name is already taken by <@{conflicting_id}>.')

    def get_user_gl_name(self, id):
        for row in self.__conn.execute('select gl_name from users where id = ?', (id,)):
            return row[0]
        raise common.Error(f'I don\'t know what name <@{id}> goes by in the GL spreadsheet.')

    def try_user_id_from_gl_name(self, gl_name):
        for row in self.__conn.execute('select id from users where gl_name = ?', (gl_name,)):
            return row[0]
        return None
