import mariadb

def load_set_id(cur, table, column, value):
    try:
        cur.execute('SELECT id FROM {} WHERE {}=?'.format(table, column), (value,))
    except mariadb.Error as e:
        print('DB Error: {}'.format(e))

        return None

    ids = cur.fetchall()
    if len(ids) == 0:
        try:
            cur.execute('INSERT INTO {} (id, {}) VALUES(0, ?)'.format(table, column), (value,))
        except mariadb.Error as e:
            print('DB Error: {}'.format(e))

    try:
        cur.execute('SELECT id FROM {} WHERE {}=?'.format(table, column), (value,))
    except mariadb.Error as e:
        print('DB Error: {}'.format(e))

        return None

    ids = cur.fetchall()
    if len(ids) == 0:
        return None

    return ids[0][0]
