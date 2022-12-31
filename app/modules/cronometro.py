# nymeliapp v 2021.06.09
# Cronometro -------
# Modulo propio de python que se encarga de 
# pasar al script que lo requiera el valor
# del acces_token de mercadolibre.
# Su objetivo es ahorrar recursos, realizando
# una sola llamada a la api de google sheets cada
# vez que se expire los tokens, estos tokens son
# almacenandos en un archivo local txt
# Proxima Actualizad: Usar un archivo CSV para 
# almacenar la data de los tokens y no generar
# tantos archivos.

# CONFIGURACION
# ingrese el lapso o diferencia de tiempo 
# para actualizar el access_token, leerlo de google sheets
# recomendado 4 a 5 horas, (14400 - 18000) segundos
lapse_time = 18000 # segundos

import time
import os
from modules.google_sheets import faccess_log
# tokenUpdater: Modulo que actualiza tokens de mercadolibre
from modules.tokenUpdater import tokenUpdater  

#import gspread
#from oauth2client.service_account import ServiceAccountCredentials
path = os.getcwd() #+ "/data/txt/"  #19/07/2021
path = path.split('/modules')  #19/07/2021 
path = path[0] + "/data/txt/"       #19/07/2021

# path_old_access_token0 = path + "old_access_token0.txt"
# path_old_access_token1 = path + "old_access_token1.txt"
# path_old_access_token2 = path + "old_access_token2.txt"

def access_token_memory(ACCOUNT):
    path_old_access_token = path + f"old_access_token{ACCOUNT}.txt"
    path_old_date = path + f"old_date{ACCOUNT}.txt"
    # Verificamos si existe tiempo anterior
    try:

        with open(path_old_date, 'r') as file:
            
            linea = file.readline()
            #print(f'Si existe tiempo anterior, valor: {linea}')
            
    except FileNotFoundError as e:

        #print('No Existe tiempo anterior, lo definimos!')
        old_date = time.time()
        old_date = str(old_date)
        # Escribimos en el archivo old_date
        file = open(path_old_date, "w")
        file. write(old_date + os.linesep)
        #print('Se creo exitosamente el archivo old_date.txt')
 
    # Leemos el tiempo anterior
    file = open(path_old_date, 'r')
    tiempo_anterior = float(file.readline())
    # Tiempo actual
    tiempo_actual = time.time()
    # Calculamos la difencia en segundos
    dif = tiempo_actual - tiempo_anterior
    #print(f'dif: {dif}')

    # Decision - Actualizar o no el access token
    if dif > lapse_time: # segundos
        # Si han transcurrido mas de 5 horas, se debe actualizar
        # LLamo al modulo tokenUpdater y actualizo tokens
        # Llamo al modulo google_sheets - funcion: faccess_log
        # me comunico con la sheet que contiene los access token
        access_token= tokenUpdater(ACCOUNT) 
        # access_log = faccess_log()
        # access_token = (access_log.cell(2+ACCOUNT,3)).value
                                        # 2 + 0  =  Miguel
                                        # 2 + 1  = Natalia
        # Actualizamos el archivo access_token

        # if ACCOUNT == 0: #Miguel
        file = open(path_old_access_token, "w")
        file. write(access_token + os.linesep)
        #print('Actualizamos el archivo old_access_token0.txt')

        # elif ACCOUNT == 1: #Natalia
        #     file = open(path_old_access_token1, "w")
        #     file. write(access_token + os.linesep)
        #     #print('Actualizamos el archivo old_access_token1.txt')

        # Actualizamos el archivo old_date
        old_date = str(time.time())
        #old_date = str(old_date)
        file = open(path_old_date, "w")
        file. write(old_date + os.linesep)
        #print('Actualizamos el archivo old_date.txt')


    else: 
        # menor a lapse_time 
        #print('usamos el access_token anterior')
        
        # Verificamos si existe token anterior
        try:
            # Intentamos leer los archivos txt, si da error, significa
            # No existen, debemos crearlos.
            # if ACCOUNT == 0: #Miguel
            with open(path_old_access_token, 'r') as file:
            
                linea = file.readline()
                #print(f'Si existe access_token anterior, valor: {linea}')

            # elif ACCOUNT == 1: #Natalia
            #     with open(path_old_access_token1, 'r') as file:
                
            #         linea = file.readline()
            #         #print(f'Si existe access_token anterior, valor: {linea}')

        # creamos los access token txt.        
        except FileNotFoundError as e:
            # Llamo al modulo google_sheets - funcion: faccess_log
            # me comunico con la sheet que contiene los access token
            access_token= tokenUpdater(ACCOUNT) 
            # access_log = faccess_log()
            # access_token = (access_log.cell(2+ACCOUNT,3)).value
            # Escribimos en el archivo old_access_token

            # if ACCOUNT == 0: #Miguel
            file = open(path_old_access_token, "w")
            file. write(access_token + os.linesep)
            #print('Se creo exitosamente el archivo old_access_token0.txt')

            # elif ACCOUNT == 1: #Natalia
            #     file = open(path_old_access_token1, "w")
            #     file. write(access_token + os.linesep)
            #     #print('Se creo exitosamente el archivo old_access_token1.txt')


    # Retornamos access_token al script que lo requiere
    # if ACCOUNT == 0: #Miguel
    file = open(path_old_access_token, 'r')
    access_token = str(file.readline())

    # elif ACCOUNT == 1: #Natalia 
    #     file = open(path_old_access_token1, 'r')
    #     access_token = str(file.readline())  

    # elif ACCOUNT == 2: # 
    #     file = open(path_old_access_token2, 'r')
    #     access_token = str(file.readline()) 

    access_token = access_token.rstrip('\n') #Eliminamos \n
    #print(f"Enviamos access token al script que lo requiere: {access_token}")
    
    return access_token


# NOTAS, El script se puede mejorar, por ahora se dejara asi
# por motivo de prisa, se espera realizar el lanzamiento del 
# programa el 1 de Junio ya con Scraper API pago 
# Google Sheets Pago y VM Instance Google Pago
# Alternativamente puedo usar Amazon Web Services Free por 1 a;o.

# DEBILIDADES DEL MODULO:
# ACCOUNTs, si se requiere hacer uso de mas cuentas de mercadolibre
# se deben crear mas if else, cosa que haria el codigo poco eficiente
# tiempo de respuesta promedio del modulo 0.0015 segundos.
# se podria mejorar aun m[as el performance 