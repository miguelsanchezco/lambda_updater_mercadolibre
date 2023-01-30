''' Librerias '''
import json
import math
import sys
import time
from datetime import datetime, timedelta
from modules.cronometro import access_token_memory
from modules.mysqlCRUD import DataLogManager
from modules.exceptions_meli import fixerr_item
from modules.apiMeliController import apiMeliController

''' Mercadolibre '''
# import meli
# from meli.rest import ApiException

""" 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

MEJORA A FUTURO:
CREAR UN DATAFRAME CON LA INFO QUE SE VA A ENVIAR A MYSQL
AL FINAL SE HACE UNA SOLA QUERY A MYSQL ENVIANDO EL DATAFRAME
SE AHORRA TIEMPO Y RECUERSOS

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
"""

# # # # # ACCOUNT = int(sys.argv[1])  # pruebas: Account 2


# # # # # print('Actualizando MELI ACCOUNT: ',ACCOUNT)

# # # # # ''' API Mercadolibre cliente '''
# # # # # api_client = meli.ApiClient()
# # # # # api_instance2 = meli.RestClientApi(api_client)

# # # # # ''' Conexion Base de Datos '''
# # # # # objectDataLog = DataLogManager("ecommerce")
# # # # # #Solo Datos con cambios => hubo_cambio = 'SI'
# # # # # df_meli = objectDataLog.extractAllDataByMeliAccount(ACCOUNT)
# # # # # df_meli.fillna("null", inplace=True)
# # # # # print(f'# ITEMS A ACTUALIZAR: {df_meli.shape[0]}\n')

# # # # # ''' Conexion Base de Datos para extraer Factores'''
# # # # # objectDataLog = DataLogManager("ecommerce")
# # # # # factors = objectDataLog.downloadFactorMeli(ACCOUNT)
# # # # # print(f'FACTORES: {factors}')

# # # # # ''' Conexion Base de Datos para extraer seller_id'''
# # # # # objectDataLog = DataLogManager("ecommerce")
# # # # # # PARA PROMOCIONES MERCADO SHOPS
# # # # # access_token = access_token_memory(ACCOUNT)
# # # # # rta = api_instance2.resource_get('/users/me',access_token)
# # # # # idUser = rta['id']

#CONSULTANDO PROMOS VIGENTES
#rta = api_instance2.resource_get(f'seller-promotions/users/{idUser}',access_token)
#print('promotions: ',rta)
#ciyberLunes 31jul - 8ago
#idCyberLunes = 'MCO5719'

# # # # ''' Estas Promotion ID en una proxima actualizacion se debe extraer de base de datos !!!!'''
# # # # if ACCOUNT == 0:
# # # #     #Promocion Mshops Miguel
# # # #     PROMOTION_ID = 'TRM-116499542-202207090922102210'
# # # # elif ACCOUNT == 1:
# # # #     #Promocion Mshops Nata
# # # #     PROMOTION_ID = 'TRM-492989005-202207012111121112'

# # # # # rutaPromoMshops = f'/seller-promotions/users/{idUser}/promotion/{PROMOTION_ID}/items?channel=mshops'
#rutaPromoMarketplace = f'/seller-promotions/users/{idUser}/promotion/{idCyberLunes}/items?channel=marketplace'
#rutaConsultarPromoEstado  = f'/seller-promotions/offers/OFFER-{id}-{idCyberLunes}'
#rutaDeletePromoMeli = f'/seller-promotions/items/{id}?promotion_type=DEAL&deal_id={idCyberLunes}'


# NOS TRAEMOS LOS PRODUCTOS DEL USUARIO.
# REQUERIMOS SELLER_ID Y PACK1000, quantity_pack1000

'''
    SE DEBE CREAR LOGICA DE MSHOPS.
    ESTA LOGICA APLICA PARA USUARIOS CON MSHOPS ACTIVO.
'''

def updater_meli(seller_id, pack1000, quantity_pack1000) :
    # seller_id = 116499542
    # pack1000 = 1
    # quantity_pack1000 = 1

    # CONSULTAMOS EN MYSQL LOS PRODUCTOS DE ESTE USUARIO CON APP_STATUS = 1 Y CON CAMBIO = SI
    #Traemos los productos correspondientes a actualizar
    ''' Creo un Objeto para conexión con BaseDeDatos'''
    databaseName = 'ecommerce_prueba'
    objectDataLog = DataLogManager(databaseName)
    df = objectDataLog.productsToUpdate(seller_id)  #Trae todo products_info_customers
    print(f'df.shape: {df.shape}')

    inicio = 0
    fin = 0

    if pack1000 <= quantity_pack1000:
        inicio = (pack1000-1)*1000
        fin =  (pack1000)*1000
    else:
        print(f'WARNING: No tienes habilitado esa cantidad de productos a actualizar. quantity_pack1000 = {quantity_pack1000}')

    cantidad_real_productos = df.shape[0]  #cantidad real de productos listos a actualizar

    if(cantidad_real_productos<=quantity_pack1000*1000):
        if (pack1000 == quantity_pack1000): #si es el ultimo pack
            fin = cantidad_real_productos


    print(f'inicio: {inicio}')
    print(f'fin: {fin}')


    try:
        df = df.iloc[inicio: fin, :]
        df.reset_index(drop=True, inplace=True) #Para que los indices empiecen en 0
        print(df.head(10))
    except:
        print('no hay esa cantidad de productos')


    # ACA SACAMOS SOLO PRODUCTOS CON CAMBIOS
    # df_meli = df
    df_meli = df[df['changed'] != '--']
    df_meli.reset_index(drop=True, inplace=True) #Para que los indices empiecen en 0
    print(f'new_df.shape: {df_meli.shape}')
    print(df_meli.head(10))


    ##################################################################
    # RECORREMOS TODAS LAS PUBLICACIONES DE MERCADOLIBRE CON CAMBIOS #
    ##################################################################

    for i in range(df_meli.shape[0]):
    # for i in range(3):
        # si meli_regular_price = 0, siguiente producto
        print('\n`````````````````````````````````````````````````\n')
        
        meli_regular_price = int(df_meli.loc[i, "meli_regular_price"])
        if meli_regular_price <= 0: 
            print(f'SKIPPED: {i}')
            continue
        meli_sale_price = int(df_meli.loc[i, "meli_sale_price"])
        mshops_regular_price = meli_regular_price
        mshops_sale_price = meli_sale_price
        sku = df_meli.loc[i, "sku"]
        id = df_meli.loc[i, "id_meli"]
        date = str(datetime.now()).split(".")[0]
        MANUFACTURING_TIME = f"{df_meli.loc[i, 'manufacturing_time']}" if df_meli.loc[i, 'manufacturing_time'] != "null" else "null"
        available_quantity = int(df_meli.loc[i, "stock_quantity"])
        total_price = float(df_meli.loc[i, "total_price"])
        

        #GTIN = df_meli.loc[i, "GTIN"]
        print('ITEM: ',i)
        print(f'FECHA: {date}')
        print(f'SELLER_ID: {seller_id}')
        print("ID_MELI: ", id)
        print("SKU: ", sku)
        print("TOTAL PRICE: ", total_price)
        print("MELI REGULAR PRICE: ", meli_regular_price)
        print("MELI SALE PRICE: ", meli_sale_price)
        print("MSHOPS REGULAR PRICE: ", mshops_regular_price)
        print("MSHOPS SALE PRICE: ", mshops_sale_price)
        print("AVAILABLE QUANTITY: ", available_quantity)
        print('MANUFACTURING TIME: ',MANUFACTURING_TIME)
        #print('GTIN: ',GTIN)

        access_token = access_token_memory(seller_id, databaseName)
        #print(access_token)
        ruta = '/items/' + id  # Actualizar Precio y Stock MercadoLibre
        # rutaPromoMeli = f'/seller-promotions/items/{id}' # Crear Promo MercadoLibre
        # rutaMshops = '/items/' + id + '/prices/types/standard/channels/mshops' #Actualizar precio Mshops
        # rutaFreeShippingMshops = f'/items/{id}/shipping' #Ruta Activar envio gratis Mshops

        ''' SI UN PRODUCTO NO ESTA DISPONIBLE EN AMAZON, LE PONEMOS STOCK=0 EN MELI'''
        if df_meli.loc[i, "amz_status"] == "Eliminado":

            print('PRODUCTO CON ERROR 404 AMAZON')
            body = {
                    # 'price': 10000000, 
                    #'status':'active',
                    'available_quantity': 0 
                    } #Activamos el item y stock = 0
            secure = 0
            while secure == 0:
                # rta = api_instance2.resource_put(ruta, access_token, body) #async_req=True
                response = apiMeliController('PUT',ruta,access_token,body)
                rta = json.loads(response.text)
                #print(rta)
                #VALIDADMOS SI FUE EXITOSO O NO
                print(f'response.status_code: {response.status_code}')
                if response.status_code > 299:
                    #INVOCAR MANEJADOR DE ERRORES 
                    secure,access_token = fixerr_item(rta,seller_id,access_token,databaseName,id)
                else:
                    status = rta['status']  
                    print(f'\n*** 404 status item: {status} ***\n') 
                    # IMPORTANTE ACTUALIZAR BASE DE DATOS
                    objectConn = DataLogManager("ecommerce_prueba")
                    objectConn.updateProducts("products_info_customers", id,"id_meli",  **{"meli_status": status, "date_updated":date})
                    secure = 2
            
            continue  
                    
        
        # # #PRECIO PARA MERCADO SHOPS
        # # if sale_price_mshops != 0:
        # #     meli_regular_priceMshops = sale_price_mshops
        # #     percent = round(((regular_price_mshops - sale_price_mshops)/regular_price_mshops)*100)
        # #     print(f'DESCUENTO Mshops: {percent}%')
        # # else:
        # #     print('NO DESCUENTO EN Mshops')
        # #     meli_regular_priceMshops = regular_price_mshops
        # #     percent = 0

        # # #PRECIO PARA MERCADOLIBRE
        # # percentMeli = round(((meli_regular_price - meli_sale_price)/meli_regular_price)*100)
        # # print(f'DESCUENTO Meli: {percentMeli}%')

        # # #COMENTADO 5/11/2022
        # # # # # if percentMeli == 100 or percentMeli < 5:
        # # # # #     print('PONEMOS DESCUENTO FAKE 10%')
        # # # # #     #Inflamos meli_regular_price y meli_sale_price
        # # # # #     percentMeli = 10
        # # # # #     if meli_sale_price == 0:  
        # # # # #         meli_sale_price = meli_regular_price 
        # # # # #         meli_regular_price = meli_regular_price/0.9
        # # # # #     else:
        # # # # #         meli_regular_price = meli_sale_price/0.9
        # # # # #     #REDONDEOS
        # # # # #     meli_regular_price = (math.ceil(meli_regular_price/1000) * 1000) - 10 if meli_regular_price != 0 else meli_regular_price
        # # # # #     meli_sale_price = (math.ceil(meli_sale_price/1000) * 1000) - 10 if meli_sale_price != 0 else meli_sale_price
    

        #CREACION BODY PARA ACTUALIZAR PRECIO MERCADOLIBRE
        diccionario = {}
        diccionario['id'] = "MANUFACTURING_TIME"
        diccionario['value_name'] = MANUFACTURING_TIME
        if MANUFACTURING_TIME == "null":
            diccionario['value_id'] = "null"
        # Creamos el body actualizar precio Meli
        body = {}
            
        if meli_regular_price > 0:    
            body['price'] = meli_regular_price 
            if meli_sale_price != 0: #AGREGADO 5/11/2022 
                body['price'] = meli_sale_price 
            body['available_quantity'] = available_quantity
            body['sale_terms'] = [diccionario]
            #body["attributes"] = [{ 'id': 'GTIN', 'value_name':str(GTIN) }]
        else:
            body['available_quantity'] = available_quantity
            body['sale_terms'] = [diccionario]
            #body["attributes"] = [{ 'id': 'GTIN', 'value_name':str(GTIN) }]

        if available_quantity > 0 :
            body['status'] = 'active'

        print('body: ',body)
        # # #COMENTADO 5/11/2022
        # # # # # # #CREAMOS EL BODY DEL DESCUENTO MERCADOLIBRE
        # # # # # # if percentMeli >= 5 and percentMeli <= 75:
        # # # # # #     # bodyCyberLunes = {
        # # # # # #     #             "deal_id": idCyberLunes,
        # # # # # #     #             "regular_price":meli_regular_price,
        # # # # # #     #             "deal_price":meli_sale_price,
        # # # # # #     #             "promotion_type":"DEAL"
        # # # # # #     # }
        # # # # # #     initDate = datetime.now()
        # # # # # #     finishDate = str((initDate + timedelta(days=6))).split(' ')[0]
        # # # # # #     initDate = str(initDate).split(' ')[0]
        # # # # # #     bodyPromoMeli = {
        # # # # # #                     "discount_percent": percentMeli,
        # # # # # #                     #"top_discount_percent": 10,
        # # # # # #                     "start_date": f"{initDate}T01:00:00",
        # # # # # #                     "finish_date": f"{finishDate}T23:59:59",
        # # # # # #                     "promotion_type": "PRICE_DISCOUNT"
        # # # # # #     }
        
        # # #print('BODY PROMO MELI: ',bodyPromoMeli)
        # # secure = 0 # Variable auxiliar para publicar en mercadolibre

        # statusError = 0  #Para Evitar Borrar las PROMOS de Meli
    

        # PASO 1. ACTUALIZAR PRECIO MERCADOLIBRE 
        # ELIMINO PROMO ANTERIOR SI EXISTE
        # rutaDeletePromoMeli = rutaPromoMeli + '?promotion_type=PRICE_DISCOUNT'
        # try:
        #     print('ELIMINADO ANTERIOR PROMOCION MELI SI EXISTE')
        #     rta2 = api_instance2.resource_delete(rutaDeletePromoMeli,access_token)#,async_req=True)
        #     print('rta: ',rta2)
        # except Exception as error:
        #     print(f'Error Eliminado Promo Ant Meli: {error.body}')
        #ERROR CREANDO PROMO MELI, B0046ZQRNO: {"message":"The promo to update must have been started or pending.","error":"conflict_error","status":409,"cause":[]}
        
        # if statusError != 409:
        print(f"ACTUALIZANDO PRECIO MELI: {body['price']}")
        ###############################################################
        # REVISANDO VARIANTES
        ###############################################################
        rutaConsultarVariantes =  f'items/{id}/variations'
        print('rutaConsultarVariantes: ',rutaConsultarVariantes)
        secure = 0
        while secure == 0:
            response = apiMeliController('GET',rutaConsultarVariantes,'','')
            rta = json.loads(response.text)
            print('1rta: ',rta)
            #VALIDADMOS SI FUE EXITOSO O NO
            print(f'1. response.status_code: {response.status_code}')
            if response.status_code > 299:
                #INVOCAR MANEJADOR DE ERRORES 
                continue
            else:
                if rta != []:
                    idVariante = rta[0]['id']
                    #ACTUALIZAR PRECIO DE VARIANTE
                    bodyVariante = {
                        "variations": [{
                                "id": idVariante,
                                "price": body['price'],
                                "available_quantity":body['available_quantity'] ,
                                # "sale_terms": body['sale_terms'] 
                            }
                        ]
                    }
                    print('bodyVariante: ',bodyVariante)
                    while secure == 0:
                        response = apiMeliController('PUT',ruta,access_token,bodyVariante)
                        rta = json.loads(response.text)
                        print(f'2. response.status_code: {response.status_code}')
                        if response.status_code > 299:
                            #INVOCAR MANEJADOR DE ERRORES 
                            secure,access_token = fixerr_item(rta,seller_id,access_token,databaseName,id)
                        else:
                            # print('2rta: ',rta)
                            body = { "sale_terms": [diccionario]}
                            secure = 1
                else:
                    #No tiene variantes
                    secure = 1
        ###############################################################
        # if percentMeli >= 5 and percentMeli <= 75:
        #COMENTADO 5/11/2022
        # # # # body['price'] = meli_regular_price
        secure = 0
        while secure == 0:
            #rta = api_instance2.resource_put(ruta,access_token,body) #async_req=True   
            response = apiMeliController('PUT',ruta,access_token,body)
            rta = json.loads(response.text)
            #VALIDADMOS SI FUE EXITOSO O NO
            print(f'3. response.status_code: {response.status_code}')
            if response.status_code > 299:
                #INVOCAR MANEJADOR DE ERRORES 
                secure,access_token = fixerr_item(rta,seller_id,access_token,databaseName,id)

            else:
                status = rta['status']
                rtaPrice = rta['price']
                print(f'\n*** PRODUCTO STATUS: {status} priceResponse {rtaPrice} ***\n')
                #GUARDO EN BASE DE DATOS EL STATUS MELI
                objectConn = DataLogManager("ecommerce_prueba")
                objectConn.updateProducts("products_info_customers", id,"id_meli",  **{"meli_status": status, "date_updated":date})
                secure = 2  
        
        # else:
        #     status = 'active'




        # # # PASO 2. SI LA PUBLICACION ESTA ACTIVA
        # # if status == 'active':
        # #     #COMENTADO 5/11/2022
        # #     # # # # # PASO 3. CREAMOS UNA PROMOCION PARA MELI
        # #     # # # # try:
        # #     # # # #     if statusError != 409 and percentMeli >= 5 and percentMeli <= 75:
        # #     # # # #         print('CREANDO PROMO MELI')
        # #     # # # #         rta = api_instance2.resource_post(rutaPromoMeli,access_token,bodyPromoMeli)#,async_req=True)
        # #     # # # #         #rta = api_instance2.resource_post(rutaPromoMeli,access_token,bodyCyberLunes)#,async_req=True)
        # #     # # # #         print('Response: ',rta)
        # #     # # # #         if rta['price'] >= meli_sale_price*0.985: #Le damos una tolerancia de -1.5%
        # #     # # # #             print('Promo Price OK!')
        # #     # # # #         else:
        # #     # # # #             print('BUG DETECTADO!')
        # #     # # # #             body['price'] = meli_sale_price
        # #     # # # #             print(f"BUG: SETEO meli_sale_price DE NUEVO EN MELI: {body['price']}")
        # #     # # # #             rta = api_instance2.resource_put(ruta,access_token,body) #async_req=True
        # #     # # # #             status = rta['status']
        # #     # # # #             rtaPrice = rta['price']
        # #     # # # #             print(f'\nBUG: AGAIN PRODUCTO STATUS: {status}  priceResponse {rtaPrice}\n')

        # #     # # # # except Exception as e:
        # #     # # # #     print(f'ERROR CREANDO PROMO MELI, {sku}: {e.body}')
        # #     # # # #     body['price'] = meli_sale_price
        # #     # # # #     print(f"SETEO meli_sale_price DE NUEVO EN MELI: {body['price']}")
        # #     # # # #     rta = api_instance2.resource_put(ruta,access_token,body) #async_req=True                
        # #     # # # #     status = rta['status']
        # #     # # # #     rtaPrice = rta['price']
        # #     # # # #     print(f'\n*** AGAIN PRODUCTO STATUS: {status}  priceResponse {rtaPrice} ***\n')

        # #     # PASO 4. ACTUALIZO PRECIO MERCADO SHOPS
        # #     #COMENTADO 5/11/2022
        # #     # # # # # # FREE SHIPPING MSHOPS  
        # #     # # # # # bodyFreeShipping = { "mshops": { "free_shipping":True } }
        # #     # # # # # api_instance2.resource_put(rutaFreeShippingMshops,access_token,bodyFreeShipping,async_req=True)
            
        # #     # # # # #print('Envio Gratis Mshops response: ',rta)
        # #     #Elimino promo vieja en caso de existir
        # #     try:
        # #         print('ELIMINADO ANTERIOR PROMOCION MSHOPS SI EXISTE')
        # #         bodyDeletePromoMshops = {"items": [{"item_id":id}],"action": "delete"}
        # #         rta2 = api_instance2.resource_put(rutaPromoMshops,access_token,bodyDeletePromoMshops)#,async_req=True)
        # #         print(rta2)
        # #     except:
        # #         print('No Existe Promocion Anterior Mshops')


        # #     if meli_regular_priceMshops!=0 and percent >= 5 and percent <= 75:
        # #         #PASO 5. SI EL PORCENTAJE ES MAYOR A 5% Y MENOR A 75%
        # #         print(f'ACTUALIZANDO PRECIO MSHOPS: {regular_price_mshops} ')
        # #         bodyMshops = { 'amount':regular_price_mshops, 'synced':False,'currency_id':'meli_regular_price'}
        # #         rtaMshops = api_instance2.resource_post(rutaMshops,access_token,bodyMshops)#,async_req=True)   
        # #         #print('Response: ',rtaMshops)  
        # #         try:
        # #             print('Amount Response:',rtaMshops['prices'][1]['amount'])
        # #         except:
        # #             pass
                
        # #         #PASO 6. CREAR PROMOCION MSHOPS
        # #         print(f'CREANDO PROMO MSHOPS, ANTES: {regular_price_mshops} AHORA: {sale_price_mshops} ; {percent}%')
        # #         bodyPromoMshops = { 'items':[{'item_id':id, "value_discount":percent, "discount_type":"PERCENT"}] ,  "action": "add" }
        # #         rtaPromoMshops = api_instance2.resource_put(rutaPromoMshops,access_token,bodyPromoMshops)#,async_req=True)
            
        # #         print('Response CON Descuento: ',rtaPromoMshops)

        # #     elif meli_regular_priceMshops!=0:
        # #         #PASO 6. SI NO HAY DESCUENTO, SOLO ACTUALIZO PRECIO mshops
        # #         print('ACTUALIZANDO PRECIO MSHOPS: ',meli_regular_priceMshops)
        # #         bodyMshops = { 'amount':meli_regular_priceMshops, 'synced':False,'currency_id':'meli_regular_price'}
        # #         rtaMshops = api_instance2.resource_post(rutaMshops,access_token,bodyMshops)#,async_req=True)   
        # #         #print('Response SIN Descuento: ',rtaMshops) 
        # #         try:
        # #             ('NO Descuento. Amount Response:',rtaMshops['prices'][1]['amount'])
        # #         except:
        # #             pass
                        
            
        # #     secure = 2 #OK
        # #     objectConn = DataLogManager("ecommerce")
        # #     objectConn.updateProducts("meli_publications", id,"id_meli_publication",  **{"status_publication": status, "date_updated":date})
        # #     print('===========================================================\n')
        
        # #     #         statusError = e.status
        # #     #         print('Error actualizando item - Enviando a exceptions_meli.py')
        # #     #         secure,access_token = fixerr_item(e,ACCOUNT,access_token)
        # #     #         # Modulo que maneja los errores de mercadolibre 
        # #     #         # publicando o actualizando item. No Descripciones.
        # #     #         # Si el item se elimino de la cuenta (closed) el error tiene el siguiente mensaje.
                    
        # #     item_closed_msg = ('Cannot update item ' + str(id) + 
        # #                     ' [status:closed, has_bids:false]')
        # #     # Imprimo el mensaje para pruebas
        # #     try:
        # #         e_body = json.loads(e.body)['message'] #mesnaje response mercadolibre
        # #     except:
        # #         print('no mensaje Error Mercadolibre')
        # #         e_body = ''
        # #     # Pregunto si el mensaje del error es igual a item_closed_msg
        # #     #print('body ',e_body)
        # #     if e_body == item_closed_msg:

        # #         # 400 - Bad Request
        # #         # Item eliminado de la cuenta  - closed
        # #         print('\n @@@@  Item closed @@@\n' )
        # #         secure = 2 # Sale del Bucle

        # #         objectConn = DataLogManager("ecommerce")
        # #         objectConn.updateProducts("meli_publications", id,"id_meli_publication", **{"status_publication": "closed", "date_updated":date})
            
        # #     # elif e.status == 403 or e.status == 404:
        # #     #     # 404 - id no existe - erroneo
        # #     #     # 403 - id existe, pero no es de esta cuenta de meli
        # #     #     print(f'\nError {e.status} Item No encontrado en Meli o Inaccesible\n')
        # #     #     secure = 2 # Sale del Bucle

        # #     #     objectConn = DataLogManager("ecommerce")
        # #     #     objectConn.updateProducts("meli_publications", id,"id_meli_publication",  **{"status_publication": "noExist", "date_updated":date})