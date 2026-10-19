# CODE REVIEW

Buen trabajo estandarizando las cantidades entregadas y calculando el precio total de cada entrega. 

El resultado final es el que estamos buscando, no hay errores en la logica y el calcuulo, la funcion recibe el pais como parametro, filtra el CSV de entrada y escribe su salida en parquet.

A continuacion te comparto algunos puntos importantes y otras buenas practicas, en orden de criticidad e importancia, para tener en cuenta de modo que el proceso sea mas robusto, eficiente y mantenible.

___

## 🔴 Mejorar estos primero

### PROCESAMIENTO PARALELO

Cuando trabajamos en un cluster Spark, el uso de Pandas hace que los nodos worker esten ociosos y el driver haga todo el trabajo.

En este caso, desde `read_csv()` hasta `createDataFrame`, todo el proceso se realizo en el driver. Esto en un pequeño archivo, por debajo de la RAM disponible en el driver, no trae problemas pero en un ambiente productivo con volumenes de datos grandes de seguro fallaria por OOM (el driver se queda sin memoria disponible, Out of Memory)

Como estamos particionando por pais y fecha, si usaramos `spark.read.csv()` se distribuirian particiones de los datos, su procesamiento y posterior escritura entre los nodos workers disponibles

### SCALAR ROW vs VECTOR COLUMN

En cargas analitcas u OLAP, evitamos trabajar fila por fila y priorizamos aplicar transformaciones de forma vectorizada por columnas. En este caso el metodo de pandas `iterrows` esta haciendo lo primero

### SCHEMA PARA LEER

Pandas a diferencia de Spark, por defecto y automaticamente infiere el schema osea los tipos de datos de las columnas y eso suele causar problemas modificando datos y/o luego en calculos como `qty * precio`.

En este caso, hubiera sido mejor definir el schema de antemano y luego pasarselo como parametro al `spark.read.csv()`. 

En capa Bronze recomiendo leer todo como StringType y no realizar transformaciones, luego en capa Silver si ya definir un schema con el data type correspondiente a cada columna para luego realizar las operaciones necesarias.

### ESCRIBIR A /tmp

En tu local si bien no es problema, al reiniciar la maquina de seguro no haya ningun archivo parquet en ese directorio

En un cluster, si el que escribe es solo el driver caso que escribieras con Pandas tmp/ es el directorio de este y en tu caso usando spark cada nodo escribe la particion que se le asigno

## 🟡 Considerar para mejorar

### HARDCODEADO

Los valores como 20 y ZPRE o ZVE1, podrian cambiar en un futuro o agregarse diferentes, de modo que tenerlos hardcodeados hace que el codigo sea dificil de mantener.

### CHECK DF VACIO

En el caso que no hubiera registros del pais por el que filtramos o los tipos de entrega considerados el pandas df vacio fallaria cuando intetaramos crear el spark df y escribir el parquet posteriormente.

### EXACT MATCHING

Los condicionales `if row["tipo_entrega"] == "ZPRE"` o `row["unidad"] == "CS"` deberian considerar diferencias como upper/lower y quizas espacios

## 🟡 Buenas practicas

- `if __name__ == "__main__":` en vez de ejecutar la funcion al final del script
- `x == "A" or x == "B"` se vuelve largo y se presta a errores, `isin()` por ejemplo seria mas pythonico
- DocString para documentar que hace y como la funcion
- Definir varias funciones que hagan solo 1 tarea y luego usarlas para lograr el objetivo final ademas podes reutilizarlas
- Priorizar `logging` en vez de `print` aun durante desarollo y/o debugeo
