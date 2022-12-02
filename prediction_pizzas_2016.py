# Importamos las librerías necesarias
import pandas as pd
import datetime as dt

def extract():
    # Leemos los cinco ficheros, creando un dataframe para cada uno de ellos
    orders = pd.read_csv('orders.csv', sep = ';')
    order_details = pd.read_csv('order_details.csv', sep = ';')
    pizza_types = pd.read_csv('pizza_types.csv', encoding = 'latin1')
    data_dictionary = pd.read_csv('data_dictionary.csv')
    pizzas = pd.read_csv('pizzas.csv')
    # Añadimos todos los dataframes a un diccionario para acceder a ellos por su nombre
    dataframes = {'orders': orders, 'order_details': order_details, 'pizzas': pizzas, 'pizza_types': pizza_types, 'data_dictionary': data_dictionary}
    return dataframes

def calidad(dataframes): # Analizaremos la calidad de los datos dados
    for clave in dataframes:
        # Para cada dataframe del diccionario, sacamos un dataframe con la tipología de los datos y el número de NaNs y Nones
        diccionario = {'Tipo de datos': dataframes[clave].dtypes, 'Número de NaNs': dataframes[clave].isna().sum(), 'Número de Nulls': dataframes[clave].isnull().sum()}
        df = pd.DataFrame(diccionario)
        # Imprimimos por pantalla el análisis de calidad
        print(f'Fichero {clave} ')
        print(df)
        print('\n')
    return

def fix_data(dataframes):
    # Juntamos los dataframes necesarios para tener todos los datos relevantes en el mismo 
    dataframe_intermedio = pd.merge(dataframes['order_details'], dataframes['pizzas'], on = 'pizza_id')
    dataframe_conjunto = pd.merge(dataframe_intermedio, dataframes['orders'], on = 'order_id').sort_values(by = 'order_id', ascending = True)
    dataframe_conjunto = dataframe_conjunto.drop(['time'], axis = 1) # Eliminamos la columna 'time' puesto que no nos es útil
    for i in range(len(dataframe_conjunto)):
        # En primer lugar, arreglamos los valores de las fechas que no sean válidos
        dataframe_conjunto.loc[i, 'date'] = pd.to_datetime(dataframe_conjunto.loc[i, 'date'], infer_datetime_format=True, errors = 'coerce') # To deal with rare date formats
        if pd.isna(dataframe_conjunto.loc[i, 'date']): # Si no hay valor para la fecha, se sustituye por la fecha anterior
            dataframe_conjunto.loc[i, 'date'] = dataframe_conjunto.loc[i-1, 'date']

        # Después, arreglaremos los valores de las cantidades
        # Ejecutando dataframe_conjunto['quantity'].unique().tolist() vemos cuáles son los valores que toma esta columna, y reemplazamos los no válidos
        if pd.isna(dataframe_conjunto.loc[i, 'quantity']): # Si el valor está vacío, tomamos que la cantidad es 1
            dataframe_conjunto.loc[i, 'quantity'] = 1
        else:
            try:
                dataframe_conjunto.loc[i, 'quantity'] = int(dataframe_conjunto.loc[i, 'quantity'])
                if dataframe_conjunto.loc[i, 'quantity'] < 0: # Si la cantidad es negativa, le cambiamos el signo
                    dataframe_conjunto.loc[i, 'quantity'] = -dataframe_conjunto.loc[i, 'quantity']
            except:
                if dataframe_conjunto.loc[i, 'quantity'].lower() == 'one': 
                    dataframe_conjunto.loc[i, 'quantity'] = 1 # Si el string es 'one', se sustituye por su número (1)
                elif dataframe_conjunto.loc[i, 'quantity'].lower() == 'two':
                    dataframe_conjunto.loc[i, 'quantity'] = 2 # Si el string es 'two', se sustituye por su número (1)
    
    # En la columna 'weekdate' se asignará a cada fecha el lunes de su semana
    dataframe_conjunto['weekdate'] = dataframe_conjunto.apply(lambda row: row['date'] - dt.timedelta(days = row['date'].weekday()), axis = 1)
    dataframe_conjunto = dataframe_conjunto.sort_values(by = 'weekdate', ascending = True) # Se ordena el dataframe cronológicamente
    dataframe_conjunto['weekdate'] = dataframe_conjunto['weekdate'].astype(str) # Convertimos la fecha a 'str' para quedarnos con las fechas únicas
    for i in range(len(dataframe_conjunto)):
        # Mantenemos la primera sección de la fecha para que no aparezcan fechas duplicadas
        dataframe_conjunto.loc[i, 'weekdate'] = dataframe_conjunto.loc[i, 'weekdate'][:10] 
    
    # Para arreglar los valores vacíos de la columna 'pizza_id', los reemplazaremos por la moda semanal
    # Para ello, crearemos ahora la lista de dataframes semanales y los cambiaremos desde ahí
    orders_byweek = [] # Buscamos sacar una lista con los pedidos de cada semana (en dataframes)
    semanas = dataframe_conjunto['weekdate'].unique().tolist()
    for semana in semanas: # Añadimos a la lista los 53 dataframes semanales
        orders_byweek.append(dataframe_conjunto[dataframe_conjunto['weekdate'] == semana].reset_index())

    for dataframe_semanal in orders_byweek:
        # Si el nombre de la pizza es vacío, se sustituirá por la pizza más pedida esa semana
        moda_semanal = dataframe_semanal['pizza_id'].mode()[0]
        for i in range(len(dataframe_semanal)):
            if pd.isna(dataframe_semanal.loc[i, 'pizza_id']):
                dataframe_semanal.loc[i, 'pizza_id'] = moda_semanal
    return dataframe_conjunto, orders_byweek

def transform(dataframes, dataframe_conjunto, orders_byweek):
    # Queremos obtener los ingredientes de cada pizza en forma de lista (asociados a cada pizza)
    diccionario_pizzas = {}
    for i in range(len(dataframes['pizza_types'])):
        diccionario_pizzas[dataframes['pizza_types']['pizza_type_id'].iloc[i]] = (dataframes['pizza_types']['ingredients'].iloc[i]).split(', ')

    ingredientes_total = [] # Queremos una lista con todos los ingredientes de todas las pizzas
    diccionario_ingredientes = {} # En este diccionario se asociará a cada ingrediente su cantidad
    for i in range(len(dataframes['pizza_types'])): # Añadimos a la lista todos los ingredientes
        ingredientes_total += (dataframes['pizza_types']['ingredients'].iloc[i]).split(', ')
    ingredientes_unicos = (pd.Series(ingredientes_total)).unique().tolist() # Eliminamos los ingredientes repetidos
    for i in range(len(ingredientes_unicos)): # Añadimos cada ingrediente al diccionario como clave, con cantidad 0
        diccionario_ingredientes[ingredientes_unicos[i]] = 0
    
    diccionarios_semanas = [] # Ahora buscamos una lista con el diccionario de ingredientes de cada semana
    for semana in range(len(orders_byweek)): # Recorre todos los dataframes (53 en total)
        ingredientes_semana = dict(diccionario_ingredientes) # Creamos una copia del diccionario vacío
        for i in range(len(orders_byweek[semana])): # Recorre el dataframe semanal en profundidad
            multiplier = 0
            pedido = orders_byweek[semana].loc[i, 'pizza_type_id'] # Necesitamos saber el tipo de pizza
            size = orders_byweek[semana].loc[i, 'size'] # Necesitamos saber el tamaño
            quantity = orders_byweek[semana].loc[i, 'quantity'] # Necesitamos saber la cantidad
            # En base al tamaño de la pizza, la proporción de ingredientes será distinta
            if size == 'S': multiplier = 1
            elif size == 'M': multiplier = 1.5
            elif size == 'L': multiplier = 2
            elif size == 'XL': multiplier = 2.5
            elif size == 'XXL': multiplier = 3
            for j in range(len(diccionario_pizzas[pedido])): # Recorre la lista de ingredientes de cada pizza
                ingredientes_semana[diccionario_pizzas[pedido][j]] += (multiplier*quantity)
        diccionarios_semanas.append(ingredientes_semana) # Añadimos a la lista el diccionario semanal
    
    # Ahora que tenemos todos los ingredientes de cada semana, queremos hacer la media para la recomendación
    media_ingredientes = dict(diccionario_ingredientes)
    for ingrediente in media_ingredientes: # Para cada ingrediente, sumamos sus valores en todos los diccionarios
        for diccionario in diccionarios_semanas: # Recorremos cada diccionario buscando el ingrediente en cuestión
            media_ingredientes[ingrediente] += diccionario[ingrediente] # Sumamos la cantidad del ingrediente
    for ingrediente in media_ingredientes:
        media_ingredientes[ingrediente] /= len(diccionarios_semanas) # Dividimos por el total de semanas para hacer la media
        media_ingredientes[ingrediente] *= 0.1 # Tomamos que una pizza pequeña lleva 100g de cada ingrediente
        media_ingredientes[ingrediente] = round(media_ingredientes[ingrediente], 1) # Aproximamos el resultado obtenido a dos decimales
    return media_ingredientes

def load(media_ingredientes):
    # Sacamos las listas de claves y valores del diccionario de medias para poder construir un dataframe
    ingredientes = list(media_ingredientes.keys())
    cantidades_recomendadas = list(media_ingredientes.values())
    # En el dataframe, la primera columna será la de los ingredientes y la segunda, la de las cantidades en kg
    recomendaciones = pd.DataFrame(list(zip(ingredientes, cantidades_recomendadas)), columns = ['Ingrediente', 'Cantidad a comprar recomendada (kg)']).sort_values(by = 'Cantidad a comprar recomendada (kg)', ascending = False)
    # Exportamos el dataframe de recomendaciones como un csv
    fichero = recomendaciones.to_csv('compra_semanal_ingredientes_2016.csv', index = False)
    return fichero

if __name__ == '__main__':
    # Ejecutamos la ETL, junto con la función de análisis de calidad
    dataframes = extract()
    calidad(dataframes)
    dataframe_conjunto, orders_byweek = fix_data(dataframes)
    media_ingredientes = transform(dataframes, dataframe_conjunto, orders_byweek)
    fichero = load(media_ingredientes)