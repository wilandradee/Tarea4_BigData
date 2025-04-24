import happybase
import pandas as pd
from datetime import datetime

# Bloque principal de ejecución
try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")

    # 2. Crear la tabla con las familias de columnas
    table_name = 'phones'
    families = {
        'info_basic': dict(),   # información básica de los telefonos
        'specs': dict(),     	# especificaciones técnicas
	'features': dict(),     #información de caracteristicas(Sensores,color)
        'pricing': dict()    	# informacion de los precios de los telefonos
    }

    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)

    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print("Tabla 'phones' creada exitosamente")

    # 3. Cargar datos del CSV
    phone_data = pd.read_csv('./phones_hbase.csv',sep=';')

    # Iterar sobre el DataFrame usando el índice
    for index, row in phone_data.iterrows():
        # Generar row key basado en el índice
        row_key = f'phone_{index:04d}'.encode()
        # print(row_key)
        # Organizar los datos en familias de columnas
        data = {
            b'info_basic:phone_brand': str(row['phone_brand']).encode(),
            b'info_basic:phone_model': str(row['phone_model']).encode(),
            b'info_basic:year': str(row['Year']).encode(),

            b'specs:storage': str(row['storage']).encode(),
            b'specs:ram': str(row['ram']).encode(),
            b'specs:battery': str(row['BATTERY']).encode(),
            b'specs:os': str(row['OS']).encode(),
            b'specs:usb': str(row['USB']).encode(),
	    b'specs:display_size':str(row['Display_Size']).encode(),

            b'features:features_sensors': str(row['Features_Sensors']).encode(),
            b'features:colors': str(row['Colors']).encode(),

            b'pricing:price_USD': str(row['price_USD']).encode(),
            b'pricing:price_range': str(row['price_range']).encode()
        }

        table.put(row_key, data)	
    print("Datos cargados exitosamente")
    #4.Consultas Iniciales
    #Verificar el ultimo  valor de la tabla Phone
    print("\n============ Vamos a revisar algunos datos del ultimo registro  de la tabla Phone  =========")
    row = table.row(row_key)
    print('Modelo del Telefono es '+str(row[b'info_basic:phone_model'].decode())+' || Marca del telefono es '+str(row[b'info_basic:phone_brand'].decode()) ) 
    print('Almacenamiento es '+str(row[b'specs:storage'].decode())+' GB ||RAM '+str(row[b'specs:ram'].decode())+' GB')	
   # print(phone_data.head())	
   
 # 5. Consultas y Análisis de Datos
    print("\n=== Los 5 Primeros Telefonos  en la base de datos  ===")
    count = 0
    for key, data in table.scan():
        if count < 5:  # Limitamos a 5 para el ejemplo
            print(f"\nTelefono ID: {key.decode()}")
            print(f"Marca Telefono: {data[b'info_basic:phone_brand'].decode()}")
            print(f"Modelo Telefono: {data[b'info_basic:phone_model'].decode()}")
            print(f"Año Lanzamiento: {data[b'info_basic:year'].decode()}")
            print(f"Almacenamiento: {data[b'specs:storage'].decode()} GB")
            print(f"RAM : {data[b'specs:ram'].decode()} GB")
            print(f"Precio en Dolares: ${data[b'pricing:price_USD'].decode()}")	
            count += 1

    # 6. Encontrar telefonos  por rango de precio
    print("\n=== Telefonos  con precio mayor a 2000 Dolares ===")
    list_key=[]
    precio=2000
    for key, data in table.scan():
        if float(data[b'pricing:price_USD'].decode()) > precio:
            print(f"\nTelefono ID: {key.decode()}")
            print(f"Marca Telefono: {data[b'info_basic:phone_brand'].decode()}")
            print(f"Modelo Telefono: {data[b'info_basic:phone_model'].decode()}")
            print(f"Año Lanzamiento: {data[b'info_basic:year'].decode()}")
            print(f"Almacenamiento: {data[b'specs:storage'].decode()} GB")
            print(f"RAM : {data[b'specs:ram'].decode()} GB")
            print(f"Precio en Dolares: ${data[b'pricing:price_USD'].decode()}")
            list_key.append(key.decode())	
    print("Resumen\n")
    print(f"Existen {len(list_key)} telefonos cuyo precio es superior  a {precio}")
 
   # 7. Análisis de  telefonos por  categoria de precios
    print("\n=== Cantidad de Telefonos por categoria de Precios ===")
    #Se crea un set para coleccionar las categorias de precios  
    categorias_precios=set()
    #Se crea un diccionario para guardar  la cantidad de categorias de precios
    categorizacion={}
    #Se recorre la base de datos,buscando en prince_range las categorias para guardarlas en el set	
    for key,data  in table.scan():
       categorias_precios.add(data[b'pricing:price_range'].decode())   
   
    #De este manera  el diccionario(categorizacion) contendra las categorias 
    for categoria in categorias_precios:
        categorizacion[categoria] = 0
   #Se recorre la base de datos para comparar y contar las categorias
        for key,data in table.scan():
     	    if( data[b'pricing:price_range'].decode()==categoria):
               categorizacion[categoria]+=1
   #Imprimir Resultado
    for clave,valor in categorizacion.items():
       print(f"Hay {valor} telefonos con categoria ' {clave}'\n") 
  
  # 8. Análisis de precios por  Almacenamiento
    print("\n=== Consultar Promedio de precios agrupados por almacenamiento del telefono ===")
    storage_prices = {}
    storage_counts = {}
    storage_avg={}

    for key, data in table.scan():
        storage =int( data[b'specs:storage'].decode())  	#Iterar los datos de almacenamiento
        price = float(data[b'pricing:price_USD'].decode())  #Iterar los datos de los  precios
	
	#Actualizar los diccionarios de Storage_prices y Storage_counts
        storage_prices[storage] = storage_prices.get(storage, 0) + price  #Se  agregan al diccionario los tipos de almacenamiento y se  agrega el precio
        storage_counts[storage] = storage_counts.get(storage, 0) + 1      #Se agregan al diciconario el conteo de cada tipo de almacenamiento
	
    #Para Sacar el promedio por cada tipo de almacenamiento se toma el divide total de precio entre ente el conteo total  de almacenamiento	
    for storage in storage_prices:
        avg_price = storage_prices[storage] / storage_counts[storage]
        storage_avg[storage]=avg_price
    price_avg_sorted=sorted(storage_avg.items(),key=lambda x: x[0])
    for  clave,valor in price_avg_sorted:
         print(f"Almacenamiento {clave} GB:Promedio de Precio en ${valor} USD")

    # 9. Extraer los modelos de telefono que cuenten con FaceID entre sus caracteristicas, se desea conocer precio,año de lanzamiento
    print("\n===================Consultar Modelos de Celulares con FACE ID=============")
    for key,data in table.scan():
        feature='Face ID'
        features = data[b'features:features_sensors'].decode()
        if(feature in features):
           print(f"Modelo : {data[b'info_basic:phone_model'].decode()} - Precio :$ {data[b'pricing:price_USD'].decode()}-Año Lanzamiento: {int(float(data[b'info_basic:year'].decode()))}- Marca : {data[b'info_basic:phone_brand'].decode()}")

    # 10. Ejemplo de actualización de precio
    print("\nActualizacion de Precios del Primer telefono")
    phone_to_update = 'phone_0000'
    new_price = 500
    new_price_range='high price'
    table.put(phone_to_update.encode(), {b'pricing:price_USD': str(new_price).encode()})
    table.put(phone_to_update.encode(),{b'pricing:prince_range':str(new_price_range).encode()})
    print(f"\nPrecio actualizado para el Telefono ID: {phone_to_update}")
    row=table.row(phone_to_update)
    print(f"Modelo : {row[b'info_basic:phone_model'].decode()}")
    print(f"Nuevo Rango precio : {row[b'pricing:price_range'].decode()}")	
    print(f"Nuevo Precio : {row[b'pricing:price_USD'].decode()}")
    print(f"Marca Telefono : {row[b'info_basic:phone_brand'].decode()}")

except Exception as e:
    print(f"Error: {str(e)}")

finally:
    # Cerrar la conexión
    connection.close()