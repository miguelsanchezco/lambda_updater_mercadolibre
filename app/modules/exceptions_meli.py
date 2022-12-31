# nymeliapp v 2021.06.09
# Fwecha: 2/06/2021
# Miguel Sanchez
# Modulo que maneja los errores al hacer llamados a la api de Mercadolibre.
# Falta agregar el error de limite maximo de piblicaciones diarias
# maximum daily quote. algo asi.
import os
from os import remove
import time
from modules.cronometro import access_token_memory

path = os.getcwd() + "/data/txt/"# ruta path carpeta actual
path_old_access_token0 = path + "old_access_token0.txt"
path_old_access_token1 = path + "old_access_token1.txt"
path_old_access_token2 = path + "old_access_token2.txt"

def fixerr_item(e,ACCOUNT,access_token):

    # a es una bandera que indica si se debe repetir el ciclo o no
    print(f"Ups! Hubo un error, lee el body: {e.body}")   

    if e.status == 400:
        print('\nError en la data - Nada que hacer ... Next Item ->\n')
        a = 2 # Fail, Saltar al siguiente articulo
        return a,access_token
        
    elif e.status == 500:
        print('\nError del Servidor 500... reintentar request...\n')
        a = 0 # Reintentar
        return a,access_token

    elif e.status == 409:
        print('\nError Conflicto Mshops 409... reintentar request...\n')
        a = 0 # Reintentar
        return a,access_token

    elif e.status == 429:
        print('\nError Too Many Request... reintentar request...\n')
        a = 0 # Reintentar
        time.sleep(7)
        return a,access_token

    elif e.status == 401: #Error en el token - actualizar token

        #POSIBLE ERROR DE TOKEN
        #ELIMINAR old_access_token
        
        try:
            # ACCOUNT = 0
            remove(path_old_access_token0)
            print('\nSe removió el archivo old_access_token0.txt\n')
        except:
            pass
        try:
            # ACCOUNT = 1
            remove(path_old_access_token1)
            print('\nSe removió el archivo old_access_token1.txt\n')
        except:
            pass
        try:
            # ACCOUNT = 2
            remove(path_old_access_token2)
            print('\nSe removió el archivo old_access_token2.txt\n')
        except:
            pass

        a = 0 # Reintentar
        access_token = access_token_memory(ACCOUNT)  #cronometro modulo
        print('\nYa se actualizo el token, reintentando request...\n')
        return a,access_token

    else:
        print('\nNuevo Error Meli o Error de Google Api')
        a = 2 # Pasar al siguiente articulo
        return a,access_token