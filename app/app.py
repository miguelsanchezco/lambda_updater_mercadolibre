from datetime import datetime , timedelta
from modules.mysqlCRUD import DataLogManager
from updater_meli import updater_meli

from datetime import datetime
#from random import randint
import json

''' EN BASE DE DATOS SE DEBE REVISAR SI EL USUARIO
    TAMBIEN NECESITA ACTUALIZAR MERCADOSHOPS
'''

def handler(event, context):  #event, context
    
    # start = time.time()

    print('event: ',event)

    try:
        body =  json.loads(event['Records'][0]['body'])
        print(f'event: {body}, type: {type(body)}')
        print('primary_key:',body['primary_key'])
        print('seller_id:',body['seller_id'])
        print('pack1000:',body['pack1000'])
        print('quantity_pack1000:',body['quantity_pack1000'])
        primary_key = body['primary_key'] #Para Base de Datos
        seller_id = body['seller_id']
        pack1000 = body['pack1000']
        quantity_pack1000 = body['quantity_pack1000']
    except Exception as e:
        print(f'error: {e}')
        primary_key = 0
        seller_id = 0
        pack1000 = 0
        quantity_pack1000 = 0

    #FOR TESTINGS PURPOSES
    # seller_id = 116499542
    # pack1000 = 1
    # quantity_pack1000 = 1
    # primary_key = 26

    #crear lista de diccionarios para la response
    date = str(datetime.now()).split(".")[0]
    print(f'\nFECHA: {date}')
    dict_to_update = {}
    dict_to_update['status'] = 'Actualizando...'
    table = 'update_schedule'
    dataBaseName = 'ecommerce_prueba'
    objectMysql = DataLogManager(dataBaseName)
    objectMysql.updateAnyTable(table, primary_key, 'id', **dict_to_update )

    if seller_id != 0 and pack1000 != 0 and quantity_pack1000 != 0:
        try:
            updater_meli(seller_id, pack1000, quantity_pack1000) 
            
            #Guardar en Base de datos!
            
            dict_to_update['status'] = 'Completado'

            #Actualizar done updates !
            table = 'subscriptions'
            objectMysql = DataLogManager(dataBaseName)
            objectMysql.usageIncrement(table, seller_id, 'done_updates', 1 )

      
        except Exception as e:
            print(f'run_spider exception')
            
            #Guardar en Base de datos!
           
            dict_to_update['status'] = 'Exception'

    # tiempo_transcurrido = time.time() - start
    # print(f'TIEMPO TRANSCURRIDO: {tiempo_transcurrido}')
    date = str(datetime.now()).split(".")[0]
    dict_to_update['date_finished'] =  date
    # dict_to_update['time_lapse_seconds'] = str(tiempo_transcurrido).split('.')[0] 
    table = 'update_schedule'
    objectMysql = DataLogManager(dataBaseName)
    objectMysql.updateAnyTable(table, primary_key, 'id', **dict_to_update )


    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': 'http://localhost:3000, http://localhost:9000, https://app.automeli.com', 
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': dict_to_update['status'] #json.dumps('Hello from Lambda!')
    }



    # # seller_id = event['seller_id']
    # # #PASO 1. TRAERNOS INFORMACION DE LAS SUSCRIPCION DE DB
    # # ''' Creo un Objeto para conexión con BaseDeDatos'''
    # # databaseName ='ecommerce_prueba'
    # # objectDataLog = DataLogManager(databaseName)
    # # subscriptions = objectDataLog.subscriptions(seller_id)  #Trae PlanInfo
    # # print(subscriptions)

    # # #PASO 2. SACAMOS TODO LO QUE NECESITAMOS
    # # name_subscription = subscriptions.loc[0,'name_subscription']
    # # print('name_subscription: ',name_subscription)
    # # quantity_pack1000 = subscriptions.loc[0,'quantity_pack1000']
    # # print('quantity_pack1000: ',quantity_pack1000)
    # # status_subscription = subscriptions.loc[0,'status_subscription']
    # # print('status_subscription: ',status_subscription)
    # # calendary_created = subscriptions.loc[0,'calendary_created']
    # # print('calendary_created: ',calendary_created)
    # # start_date = subscriptions.loc[0,'start_date']
    # # print('start_date: ',start_date)

    
    # # #PASO 3. Revisamos status y calendary_created, si calendary =0
    # # #CREAMOS LAS FECHAS DE ACTUALIZACION
    # # if status_subscription == 'active' and calendary_created == 0:
        
    # #     #Procedemos a crear calendario
    # #     print('Creacion de Calendario')
    # #     #PASO 4. Miramos el dia para la primera actualizacion
    # #     day_number = datetime.today().weekday()
    # #     #Si day_number es 3 . Jueves. entonces pasamos a Viernes
    # #     if day_number + 1 != 3:
    # #         #Proseguimos
    # #         date_first_day =  datetime.now()+ timedelta(1)
    # #     else:
    # #         #Pasamos a viernes primera actualizacion
    # #         date_first_day =  datetime.now()+ timedelta(2)

    
    # #     date = datetime.now()
    # #     print('datetime.now():',date)
    # #     print(f'date_first_day: {date_first_day}')
        
    # #     #PASO 5. REVISAMOS EL PLAN. DETERMINAMOS SALTOS Y NUMERO DE FECHAS
        
    # #     if name_subscription == 'Basico':
           
    # #         fechas_quantity = 5
    # #     elif name_subscription == 'Intermedio':
            
    # #         fechas_quantity = 10
    # #     elif (name_subscription == 'Avanzado'  or name_subscription == 'Avanzado_free'):
            
    # #         fechas_quantity = 15

        
    # #     print('fechas_quantity',fechas_quantity)
        
    # #     #PASO 6. DETERMINAMOS DIAS DEL MES Y CALCULAMOS DELTA_INCREMENTO
        
    # #     mes = date.strftime('%Y')
    # #     anio = date.strftime('%m')
    # #     cantidad_dias_mes = numero_dias_mes(mes,anio)
    # #     delta_incremento = (cantidad_dias_mes -1) / fechas_quantity

    # #     print(f'delta_incremento: {delta_incremento}') 
    # #     fechas_definitivas = [] 
    # #     print('+++++++++++++++++++++++++++++++++++')

    # #     #PASO 7. DEFINIMOS LAS FECHAS DEFINITVAS DE ACTUALIZACION
    # #     for i in range(fechas_quantity):
    # #         delta_corregido = round(delta_incremento*i)
    # #         #print(delta_corregido)
    # #         next_date = date_first_day + timedelta(delta_corregido)
    # #         print(f'next date: {next_date}')
    # #         day_week_number = next_date.weekday()
    # #         #print(day_week_number)
    # #         day_name = day_week_name(day_week_number)
    # #         print(day_name)
    # #         if day_name == 'Jueves':
    # #             print('-------------------')
    # #             if i != (fechas_quantity-1): 
    # #                 #si es el ultimo dia un jueves, no sumamos 1 dia, le restamos 1
    # #                 next_date = date_first_day + timedelta(delta_corregido+1)
    # #             else:
    # #                 next_date = date_first_day + timedelta(delta_corregido-1)

    # #             print(f'next date corregida: {next_date}')
    # #             day_week_number = next_date.weekday()
    # #             #print(day_week_number)
    # #             print('-------------------')


    # #         #Guardo en la lista fechas_definitivas
    # #         fechas_definitivas.append(next_date)

    # #     print(f'fechas definitivas: {fechas_definitivas}')

    # #     #PASO 7.5  HACEMOS TODO EL PROCEDIMIENTO PARA CADA PACK1000
    # #     for pack1000 in range(quantity_pack1000):

    # #         #PASO 8. REVISAMOS EN BASE DE DATOS LAS FRANJAS LIBRES Y CREAMOS REGISTRO
    # #         ''' Creo un Objeto para conexión con BaseDeDatos'''
    # #         databaseName ='ecommerce_prueba'
    # #         objectDataLog = DataLogManager(databaseName)
    # #         df_schedules = objectDataLog.get_schedule(date_first_day)  #Trae schedules existing
    # #         print(df_schedules.head)

    # #         pack1000 = pack1000 +1
    # #         print(f'PACK1000: {pack1000}')
    # #         schedule_asignator(fechas_definitivas, pack1000, df_schedules, seller_id)

    # #     #PASO 9. ACTUALIZAMOS SUSCRIPCION PARA INFORMAR QUE YA SE CREO CALENDARIO
    # #     #calendary_created = 1
    # #     ''' Creo un Objeto para conexión con BaseDeDatos'''
    # #     objectDataLog = DataLogManager(databaseName)
    # #     objectDataLog.update_calendary_created(seller_id)  #Trae schedules existing
    # #     print('FIN. calendary_created=1')
    # #     return {
    # #         'statusCode': 201,
    # #         'headers': {
    # #             'Access-Control-Allow-Headers': 'Content-Type',
    # #             'Access-Control-Allow-Origin': 'http://localhost:3000, http://localhost:9000, https://app.automeli.com', 
    # #             'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
    # #         },
    # #         'body': {'status':'calendario creado'} #json.dumps('Hello from Lambda!')
    # #     }

    # # else:
    # #     print('status inactivo o calendario ya creado!')

    # #     return {
    # #         'statusCode': 200,
    # #         'headers': {
    # #             'Access-Control-Allow-Headers': 'Content-Type',
    # #             'Access-Control-Allow-Origin': 'http://localhost:3000, http://localhost:9000, https://app.automeli.com', 
    # #             'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
    # #         },
    # #         'body': {'status':'ya existe calendario'} #json.dumps('Hello from Lambda!')
    # #     }