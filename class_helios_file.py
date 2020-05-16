# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 18:15:16 2020

@author: GuillermoMatas
"""
import pytz, datetime as dt
import os
from psycopg2 import sql



class HeliosFile():
    
    def __init__(self,filename,path):
        line_count=0
        #nombre del archivo
        self.name=filename
        self.date=[]
        self.time=[]
        self.g_0=[]
        self.g_41=[]
        self.d_0=[]
        self.b=[]
        self.w_vel=[]
        self.w_dir=[]   
        self.t_amb=[]
        
        #extracción de los datos 
        helios_file=open(os.path.join(path,filename))
        for line in helios_file.readlines(): 
            if (line.startswith('yy')):
                continue
            line=line.strip('\n') 
            line=line.split('\t')
            
            #Conversión del tiempo a tiempo UTC
            
            local_tz=pytz.timezone("Europe/Madrid")
            naive_dt=dt.datetime.strptime("{} {}".format(line[0],line[1]), "%Y/%m/%d %H:%M")
            local_dt=local_tz.localize(naive_dt, is_dst=None)
            utc_dt=local_dt.astimezone(pytz.utc)

            self.date.append(dt.datetime.date(utc_dt))
            
            self.time.append(dt.datetime.time(utc_dt))
            
            self.g_0.append(None if line[2].isalpha() else float(line[2]))
            
            self.g_41.append(None if line[3].isalpha() else float(line[3]))
            
            self.d_0.append(None if line[4].isalpha() else float(line[4]))
            
            self.b.append(None if line[5].isalpha() else float(line[5]))
            
            self.w_vel.append(None if line[6].isalpha() else float(line[6]))
            
            self.w_dir.append(None if line[7].isalpha() else float(line[7]))
            
            self.t_amb.append(None if line[8].isalpha() else float(line[8]))
            
            line_count+=1
    
    

class HeliosFileList():
    def __init__(self, path, table_name, db_cursor):
        self.path=path
        self.files=[]
        
        
        #Creación de la tabla en la DB
        query = sql.SQL('''CREATE TABLE IF NOT EXISTS {} (
                        ID SERIAL PRIMARY KEY,
                        FILE_NAME VARCHAR(100) NOT NULL,
                        MEASURE_DATE DATE NOT NULL,
                        MEASURE_UTC_TIME TIMETZ NOT NULL,
                        G_0 NUMERIC,
                        G_41 NUMERIC,
                        D_0 NUMERIC,
                        B NUMERIC,
                        W_VEL NUMERIC,
                        W_DIR NUMERIC,
                        T_AMB NUMERIC
                );''')
        
        db_cursor.execute(query.format(sql.Identifier(table_name)))
        
        #Al usar el tipo de datos TIMESTAMPZ introducimos la zona horaria a la que se refiere, de manera que los datos se guardan en la DB como GMT
        db_cursor.execute("SET timezone = 'Europe/Madrid';")


        #Comprobación de los archivos ya importados a la base de datos. 
        #De esta manera, únicamente se importan los archivos que no se encuentren           
        #ya en la DB
        query = sql.SQL('''SELECT
                                  DISTINCT FILE_NAME
                             FROM
                                 {};
                          ''')
        db_cursor.execute(query.format(sql.Identifier(table_name)))
        db_filenames_data=db_cursor.fetchall()
        db_filenames = [item for db_tuple in db_filenames_data for item in db_tuple] 
        
        for filename in os.listdir(path): 
            if (filename.startswith('data') and filename.endswith('.txt') and filename not in db_filenames):
                self.files.append(HeliosFile(filename,path))

    def sendToDataBase(self, table_name, db_cursor):
        for file in self.files:
            sql_command='''INSERT INTO {}
                            (
                            FILE_NAME, MEASURE_DATE, MEASURE_UTC_TIME,
                            G_0, G_41, D_0, B,
                            W_VEL, W_DIR, T_AMB
                            )
                            VALUES
                            (
                            %s, %s, %s, 
                            %s, %s, %s, %s,
                            %s, %s, %s
                            );
                        '''
            for i in range(len(file.time)):
                db_cursor.execute(sql_command.format(table_name), (file.name, file.date[i], file.time[i], file.g_0[i], file.g_41[i], file.d_0[i], file.b[i], file.w_vel[i], file.w_dir[i], file.t_amb[i]))
            
    


    def deleteTable(self, table_name, db_cursor):
        query = sql.SQL("DROP TABLE IF EXISTS {};")
        db_cursor.execute(query.format(sql.Identifier(table_name)))
