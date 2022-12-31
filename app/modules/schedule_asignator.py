import pandas as pd
from modules.mysqlCRUD import DataLogManager
from modules.day_week_name import day_week_name
from datetime import datetime, timedelta

def schedule_asignator(fechas_definitivas,pack1000, df_schedule,seller_id,):
    
    print('estoy en shedule_asignator')

    data = []
    #PASO 9. itero las fechas en las que se va a actualizar


    for index,fecha_definitiva in enumerate(fechas_definitivas):

        print(f'fecha {index+1}: {str(fecha_definitiva).split(" ")[0]}')
        #Hasta que se asigne una franja no salimos del while
        concurrency_order = 0
        success = 0
        while success == 0:

            concurrency_order = concurrency_order + 1  

            #buscamos en el dataframe para cada fecha y concurrency_order 
            # # coincidencias = ( df_schedule['date_start'] == str(fecha_definitiva).split(" ")[0] and df_schedule['concurrency_order'] == concurrency_order )   #booleano
            # # df_schedule_depurado = df_schedule[coincidencias]
            
            df_schedule_depurado = df_schedule[(df_schedule['date_start'] == str(fecha_definitiva).split(" ")[0]) & (df_schedule['concurrency_order'] == concurrency_order)]
            coincidencias_number = df_schedule_depurado.shape[0]
            print('df_schedule # coincidencias:',coincidencias_number)
            if coincidencias_number < 72:
                # # # # for franja in range(72): # 72 franjas desde 00:00 , cada franja de 15mn, hasta 18:00
                # # # #     if franja in df_schedule_depurado.franja.values:
                # # # #         print(f'Franja {franja} & concurrency {concurrency_order} ocupada, next!')
                # # # #     else:
                franja = coincidencias_number  #NUEVO!!
                print(f'franja {franja} libre. guardamos en base de datos!!')
                new_registry = {}
                new_registry['seller_id'] = seller_id
                new_registry['date_start'] = str(fecha_definitiva).split(" ")[0]
                new_registry['day_number'] = fecha_definitiva.weekday()
                new_registry['day_name'] = day_week_name(fecha_definitiva.weekday())
                hour_start = str(datetime.strptime("21-02-2021 00:00:00", "%d-%m-%Y %H:%M:%S") + (timedelta(minutes = 15*franja)))
                #print('hour_start: ',hour_start)
                new_registry['hour_start'] = str(hour_start).split(" ")[1]
                new_registry['franja'] = franja
                new_registry['concurrency_order'] = concurrency_order
                new_registry['pack1000'] = pack1000
                new_registry['status'] = 'Pendiente'
                print('new_registry',new_registry)
                data.append(new_registry)
                success = 1
                # # # # break

    dataframe = pd.DataFrame.from_dict(data)
    ''' Creo un Objeto para conexión con BaseDeDatos'''
    databaseName ='ecommerce_prueba'
    objectDataLog = DataLogManager(databaseName)  # <<<--- agrego todas las fechas
    table = 'update_schedule'
    objectDataLog.dfToTableDB(table, dataframe)

   