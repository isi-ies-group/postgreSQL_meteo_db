# -*- coding: utf-8 -*-

import class_geonica_file as geonica
import class_helios_file as helios
import os
import psycopg2
from config import config

#Acceso a base de datos
conn = None
try:
    # read connection parameters
    params = config()
 
    # connect to the PostgreSQL server
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)
  
    # create a cursor
    cur = conn.cursor()
    
   # execute a statement
    print('PostgreSQL database version:')
    cur.execute('SELECT version()')
 
    # display the PostgreSQL database server version
    db_version = cur.fetchone()
    print(db_version)
    
    #Definición de los directorios de almacenamiento de los datos
    geonica_path=os.path.join(os.getcwd(),'geonica_data')
    helios_path=os.path.join(os.getcwd(),'helios_data')
    
    #Obtiene los datos de los ficheros y crea la tabla en caso de que no exista
    lista_geonica=geonica.GeonicaFileList(geonica_path,'geonica',cur)
    lista_helios=helios.HeliosFileList(helios_path,'helios',cur)

    #Escribe/Actualiza la tabla
    lista_geonica.sendToDataBase('geonica',cur)
    lista_helios.sendToDataBase('helios',cur)
    
    #Full Join de ambas tablas
    # cur.execute('SELECT * FROM helios FULL JOIN geonica ON helios.measure_date=geonica.measure_date and helios.measure_utc_time=geonica.measure_utc_time')
   
   # close the communication with the PostgreSQL
    cur.close()
    
   # commit the changes
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
        print('Database connection closed.')