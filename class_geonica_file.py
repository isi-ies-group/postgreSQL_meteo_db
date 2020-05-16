# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 18:15:16 2020

@author: GuillermoMatas
"""
import datetime as dt
import os
from psycopg2 import sql



class GeonicaFile():
    
    def __init__(self,filename,path):
        line_count=0
        #nombre del archivo
        self.name=filename
        self.date=[]
        self.time=[]
        self.v_viento=[]
        self.d_viento=[]
        self.temp_air=[]
        self.rad_dir=[]
        self.ele_sol=[]
        self.ori_sol=[]   
        self.top=[]
        self.mid=[]
        self.bot=[]
        self.cal_top=[]
        self.cal_mid=[]
        self.cal_bot=[]
        self.pres_aire=[]
        
        #extracción de los datos 
        geonica_file=open(os.path.join(path,filename))
        for line in geonica_file.readlines(): 
            if (line.startswith('yy')):
                continue
            line=line.strip('\n') 
            line=line.split('\t')
            
            
            
            #De esta manera se reduce la cantidad de espacio necesario para almacenar los datos del archivo
            
            if (line_count==0):
                self.date=dt.datetime.date(dt.datetime.strptime(line[0],'%Y/%m/%d'))
            # self.date.append(dt.datetime.date(dt.datetime.strptime(line[0],'%Y/%m/%d')))
            
            self.time.append(dt.datetime.time(dt.datetime.strptime(line[1],'%H:%M')))
            
            self.v_viento.append(None if line[2].isalpha() else float(line[2]))
            
            self.d_viento.append(None if line[3].isalpha() else float(line[3]))
            
            self.temp_air.append(None if line[4].isalpha() else float(line[4]))
            
            self.rad_dir.append(None if line[5].isalpha() else float(line[5]))
            
            self.ele_sol.append(None if line[6].isalpha() else float(line[6]))
            
            self.ori_sol.append(None if line[7].isalpha() else float(line[7]))
            
            self.top.append(None if line[8].isalpha() else float(line[8]))
            
            self.mid.append(None if line[9].isalpha() else float(line[9]))
            
            self.bot.append(None if line[10].isalpha() else float(line[10]))
        
            self.cal_top.append(None if line[11].isalpha() else float(line[11]))
            
            self.cal_mid.append(None if line[12].isalpha() else float(line[12]))
    
            self.cal_bot.append(None if line[13].isalpha() else float(line[13]))
            
            self.pres_aire.append(None if line[14].isalpha() else float(line[14]))
            
            line_count+=1
    
    
    
    
    
class GeonicaFileList():
    def __init__(self,path, table_name, db_cursor):
        self.path=path
        self.files=[]
        
        #Creación de la tabla en la DB
        query = sql.SQL('''CREATE TABLE IF NOT EXISTS {} (
                        ID SERIAL PRIMARY KEY,
                        FILE_NAME VARCHAR(100) NOT NULL,
                        MEASURE_DATE DATE NOT NULL,
                        MEASURE_UTC_TIME TIME NOT NULL,
                        V_VIENTO NUMERIC,
                        D_VIENTO NUMERIC,
                        TEMP_AIR NUMERIC,
                        RAD_DIR NUMERIC,
                        ELE_SOL NUMERIC,
                        ORI_SOL NUMERIC,
                        TOP NUMERIC,
                        MID NUMERIC,
                        BOT NUMERIC,
                        CAL_TOP NUMERIC,
                        CAL_MID NUMERIC,
                        CAL_BOT NUMERIC,
                        PRES_AIRE NUMERIC
                );''')
        
        db_cursor.execute(query.format(sql.Identifier(table_name)))
        
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
            if (filename.startswith('geonica') and filename.endswith('.txt') and filename not in db_filenames):
                self.files.append(GeonicaFile(filename,path))

    def sendToDataBase(self, table_name, db_cursor):
        for file in self.files:
            sql_command='''INSERT INTO {}
                            (
                            FILE_NAME,MEASURE_DATE,MEASURE_UTC_TIME,
                            V_VIENTO,D_VIENTO,TEMP_AIR,RAD_DIR,
                            ELE_SOL,ORI_SOL,TOP,MID,BOT,CAL_TOP,
                            CAL_MID,CAL_BOT, PRES_AIRE 
                            )
                            VALUES
                            (
                            %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s
                            );
                        '''
            for i in range(len(file.time)):
                db_cursor.execute(sql_command.format(table_name), (file.name, file.date, file.time[i], file.v_viento[i], file.d_viento[i], file.temp_air[i], file.rad_dir[i], file.ele_sol[i], file.ori_sol[i], file.top[i], file.mid[i], file.bot[i], file.cal_top[i], file.cal_mid[i], file.cal_bot[i], file.pres_aire[i]))
            
    


    def deleteTable(self, table_name, db_cursor):
        query = sql.SQL("DROP TABLE IF EXISTS {};")
        db_cursor.execute(query.format(sql.Identifier(table_name)))

        