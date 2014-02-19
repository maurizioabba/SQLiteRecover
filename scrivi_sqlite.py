import sqlite3

conn = sqlite3.connect('two_pagest_test.db')
c = conn.cursor()
# c.execute("DROP TABLE FIRST")
# c.execute("DROP TABLE SECOND")
# c.execute("DROP TABLE THIRD")

# c.execute("CREATE TABLE FIRST (s varchar(128), i smallint)")
# c.execute("CREATE TABLE SECOND (s varchar(128), i smallint)")
# c.execute("CREATE TABLE THIRD (s varchar(128), i smallint)")

# sql = "INSERT INTO FIRST (s,i) VALUES (?,?)"
# s = "A"*128
# l = []
# for el in xrange(1, 500):
#     l.append((s, el))
# c.executemany(sql, l)
# s = "B"*128
# sql = "INSERT INTO SECOND (s, i) VALUES (?, ?)"
# l = []
# for el in xrange(1, 500):
#     l.append((s, el))
# c.executemany(sql, l)
# s = "C"*128
# t=1
# sql = "INSERT INTO SECOND (s, i) VALUES (?, ?)"
# l = []
# for el in xrange(1, 500):
#     l.append((s, 1))
# c.executemany(sql, l)
c.execute("DROP TABLE FIRST")
c.execute("CREATE TABLE FIRST (s varchar(64))")
data = "A" * 3000
c.execute("INSERT INTO FIRST (s) VALUES (?)", [(data)])
c.execute("INSERT INTO FIRST (s) VALUES (?)", [(data)])

conn.commit()
conn.close()
