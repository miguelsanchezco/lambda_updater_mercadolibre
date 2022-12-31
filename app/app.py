from datetime import datetime , timedelta
from modules.day_week_name import day_week_name
from modules.schedule_asignator import schedule_asignator
from modules.numero_dias_mes import numero_dias_mes
from modules.mysqlCRUD import DataLogManager


def handler(event, context):  #event, context
    

    seller_id = event['seller_id']
    #PASO 1. TRAERNOS INFORMACION DE LAS SUSCRIPCION DE DB
    ''' Creo un Objeto para conexión con BaseDeDatos'''
    databaseName ='ecommerce_prueba'
    objectDataLog = DataLogManager(databaseName)
    subscriptions = objectDataLog.subscriptions(seller_id)  #Trae PlanInfo
    print(subscriptions)

    #PASO 2. SACAMOS TODO LO QUE NECESITAMOS
    name_subscription = subscriptions.loc[0,'name_subscription']
    print('name_subscription: ',name_subscription)
    quantity_pack1000 = subscriptions.loc[0,'quantity_pack1000']
    print('quantity_pack1000: ',quantity_pack1000)
    status_subscription = subscriptions.loc[0,'status_subscription']
    print('status_subscription: ',status_subscription)
    calendary_created = subscriptions.loc[0,'calendary_created']
    print('calendary_created: ',calendary_created)
    start_date = subscriptions.loc[0,'start_date']
    print('start_date: ',start_date)

    
    #PASO 3. Revisamos status y calendary_created, si calendary =0
    #CREAMOS LAS FECHAS DE ACTUALIZACION
    if status_subscription == 'active' and calendary_created == 0:
        
        #Procedemos a crear calendario
        print('Creacion de Calendario')
        #PASO 4. Miramos el dia para la primera actualizacion
        day_number = datetime.today().weekday()
        #Si day_number es 3 . Jueves. entonces pasamos a Viernes
        if day_number + 1 != 3:
            #Proseguimos
            date_first_day =  datetime.now()+ timedelta(1)
        else:
            #Pasamos a viernes primera actualizacion
            date_first_day =  datetime.now()+ timedelta(2)

    
        date = datetime.now()
        print('datetime.now():',date)
        print(f'date_first_day: {date_first_day}')
        
        #PASO 5. REVISAMOS EL PLAN. DETERMINAMOS SALTOS Y NUMERO DE FECHAS
        
        if name_subscription == 'Basico':
           
            fechas_quantity = 5
        elif name_subscription == 'Intermedio':
            
            fechas_quantity = 10
        elif (name_subscription == 'Avanzado'  or name_subscription == 'Avanzado_free'):
            
            fechas_quantity = 15

        
        print('fechas_quantity',fechas_quantity)
        
        #PASO 6. DETERMINAMOS DIAS DEL MES Y CALCULAMOS DELTA_INCREMENTO
        
        mes = date.strftime('%Y')
        anio = date.strftime('%m')
        cantidad_dias_mes = numero_dias_mes(mes,anio)
        delta_incremento = (cantidad_dias_mes -1) / fechas_quantity

        print(f'delta_incremento: {delta_incremento}') 
        fechas_definitivas = [] 
        print('+++++++++++++++++++++++++++++++++++')

        #PASO 7. DEFINIMOS LAS FECHAS DEFINITVAS DE ACTUALIZACION
        for i in range(fechas_quantity):
            delta_corregido = round(delta_incremento*i)
            #print(delta_corregido)
            next_date = date_first_day + timedelta(delta_corregido)
            print(f'next date: {next_date}')
            day_week_number = next_date.weekday()
            #print(day_week_number)
            day_name = day_week_name(day_week_number)
            print(day_name)
            if day_name == 'Jueves':
                print('-------------------')
                if i != (fechas_quantity-1): 
                    #si es el ultimo dia un jueves, no sumamos 1 dia, le restamos 1
                    next_date = date_first_day + timedelta(delta_corregido+1)
                else:
                    next_date = date_first_day + timedelta(delta_corregido-1)

                print(f'next date corregida: {next_date}')
                day_week_number = next_date.weekday()
                #print(day_week_number)
                print('-------------------')


            #Guardo en la lista fechas_definitivas
            fechas_definitivas.append(next_date)

        print(f'fechas definitivas: {fechas_definitivas}')

        #PASO 7.5  HACEMOS TODO EL PROCEDIMIENTO PARA CADA PACK1000
        for pack1000 in range(quantity_pack1000):

            #PASO 8. REVISAMOS EN BASE DE DATOS LAS FRANJAS LIBRES Y CREAMOS REGISTRO
            ''' Creo un Objeto para conexión con BaseDeDatos'''
            databaseName ='ecommerce_prueba'
            objectDataLog = DataLogManager(databaseName)
            df_schedules = objectDataLog.get_schedule(date_first_day)  #Trae schedules existing
            print(df_schedules.head)

            pack1000 = pack1000 +1
            print(f'PACK1000: {pack1000}')
            schedule_asignator(fechas_definitivas, pack1000, df_schedules, seller_id)

        #PASO 9. ACTUALIZAMOS SUSCRIPCION PARA INFORMAR QUE YA SE CREO CALENDARIO
        #calendary_created = 1
        ''' Creo un Objeto para conexión con BaseDeDatos'''
        objectDataLog = DataLogManager(databaseName)
        objectDataLog.update_calendary_created(seller_id)  #Trae schedules existing
        print('FIN. calendary_created=1')
        return {
            'statusCode': 201,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': 'http://localhost:3000, http://localhost:9000, https://app.automeli.com', 
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': {'status':'calendario creado'} #json.dumps('Hello from Lambda!')
        }

    else:
        print('status inactivo o calendario ya creado!')

        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': 'http://localhost:3000, http://localhost:9000, https://app.automeli.com', 
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': {'status':'ya existe calendario'} #json.dumps('Hello from Lambda!')
        }
