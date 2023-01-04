# nymeliapp v 2021.06.09
# Fwecha: 2/06/2021
# Miguel Sanchez
# Modulo que maneja los errores al hacer llamados a la api de Mercadolibre.
# Falta agregar el error de limite maximo de piblicaciones diarias
# maximum daily quote. algo asi.
# import os
import json
from os import remove
import time
from modules.cronometro import access_token_memory
from modules.mysqlCRUD import DataLogManager
from datetime import datetime

# path = '/tmp/' # ruta path carpeta actual
# path_old_access_token0 = path + "old_access_token0.txt"
# path_old_access_token1 = path + "old_access_token1.txt"
# path_old_access_token2 = path + "old_access_token2.txt"

def fixerr_item(rta,seller_id,access_token,databaseName,id):

    # a es una bandera que indica si se debe repetir el ciclo o no
    print(f"Ups! Hubo un error, lee el body: {rta['message']}")   
    date = str(datetime.now()).split(".")[0]

    if rta['status'] == 400:
        print('\nError en la data - ... Revisar rta ->\n')
        print('rta: ',rta)
        item_closed_msg =  '[status:closed, has_bids:false]'
        item_inactive_msg = '[status:under_review, has_bids:false]'
        # Imprimo el mensaje para pruebas
        try:
            #print(f'rta: {rta}')
            e_body = rta['message'] #mesnaje response mercadolibre
            print(f'e_body: {e_body}')
        except:
            print('no mensaje Error Mercadolibre')
            e_body = ''
        # Pregunto si el mensaje del error es igual a item_closed_msg
        #print('body ',e_body)
        if item_closed_msg in e_body:
            # 400 - Bad Request
            # Item eliminado de la cuenta  - closed
            print('\n @@@@  Item closed @@@\n' )
            status = 'closed'
            # IMPORTANTE ACTUALIZAR BASE DE DATOS
            objectConn = DataLogManager("ecommerce_prueba")
            objectConn.updateProducts("products_info_customers", id,"id_meli",  **{"meli_status": status, "date_updated":date})
        elif item_inactive_msg in e_body:
            print('\n @@@@  Item inactive @@@\n' )
            status = 'under_review'
            # IMPORTANTE ACTUALIZAR BASE DE DATOS
            objectConn = DataLogManager("ecommerce_prueba")
            objectConn.updateProducts("products_info_customers", id,"id_meli",  **{"meli_status": status, "date_updated":date})


        a = 2 # Fail, Saltar al siguiente articulo
        return a,access_token
        
    elif rta['status'] == 500:
        print('\nError del Servidor 500... reintentar request...\n')
        a = 0 # Reintentar
        return a,access_token

    elif rta['status'] == 409:
        print('\nError Conflicto Mshops 409... reintentar request...\n')
        a = 0 # Reintentar
        return a,access_token
    
    elif rta['status'] == 403:
        print(f'\nError 403 Item Existe pero Inaccesible\n')
        status = 'forbidden'
        # IMPORTANTE ACTUALIZAR BASE DE DATOS
        objectConn = DataLogManager("ecommerce_prueba")
        objectConn.updateProducts("products_info_customers", id,"id_meli",  **{"meli_status": status, "date_updated":date})
        a = 2 # NO Reintentar
        return a,access_token
    
    elif rta['status'] == 404:
        print(f'\nError 404 Item No encontrado en Meli\n')
        status = 'not_found'
        # IMPORTANTE ACTUALIZAR BASE DE DATOS
        objectConn = DataLogManager("ecommerce_prueba")
        objectConn.updateProducts("products_info_customers", id,"id_meli",  **{"meli_status": status, "date_updated":date})
        a = 2 # NO Reintentar        a = 2 # NO Reintentar
        return a,access_token

    elif rta['status'] == 429:
        print('\nError Too Many Request... reintentar request...\n')
        a = 0 # Reintentar
        time.sleep(7)
        return a,access_token

    elif rta['status'] == 401: #Error en el token - actualizar token

        #POSIBLE ERROR DE TOKEN
        #ELIMINAR old_access_token
        path = '/tmp/'
        path_old_access_token = path + f"old_access_token{seller_id}.txt"
        
        try:
            # seller_id = 0
            remove(path_old_access_token)
            print(f'\nSe removió el archivo old_access_token{seller_id}.txt\n')
        except:
            pass
     

        a = 0 # Reintentar
        access_token = access_token_memory(seller_id,databaseName)  #cronometro modulo
        #print('\nYa se actualizo el token, reintentando request...\n')
        return a,access_token

    else:

        print('\nNuevo Error Meli')
        #access_token = access_token_memory(seller_id,databaseName) 
        a = 2 # Pasar al siguiente articulo
        return a,access_token