import psycopg2

class databaseConnection():
    def __init__(self,dbname,user,password):
        self.dbname=dbname
        self.user=user
        self.password=password

    def __del__(self):
        if self.conn is not None:
            self.conn.close()
    
    def connect(self):
        self.conn = psycopg2.connect("dbname="+self.dbname+" user="+self.user+" password="+self.password)

    def insert(self,statment,values,needFeach):
        if self.conn is None:
            raise MyConnectionError('Нет подключения к базе')
            return
        cur = self.conn.cursor()
        cur.execute(statment,values)
        self.conn.commit()
        if needFeach:
            result=cur.fetchall()
            return result[0][0]
        
    def select(self,statment):
        if self.conn is None:
            raise MyConnectionError('Нет подключения к базе')
            return
        cur = self.conn.cursor()
        cur.execute(statment)
        result=cur.fetchall()
        cur.close()
        return result
        
        
