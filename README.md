# Aeropuertos 2000

## Compilación
1. Utilizando el comando **cd** situarse en el directorio del proyecto.
2. Ejecutar el comando **mvn clean install**.

## Ejecucion
1. Una vez compilado situarse en las carpetas target de *client* y *server*.
2. Con el comando **tar -xzf** descomprimir los archivos: *tpe2-g8-client-1.0-SNAPSHOT-bin.tar* y *tpe2-g8-server-1.0-SNAPSHOT-bin.tar*.
Situados en sus respectivas carpetas.
     ```
    tar -xzf ./client/target/tpe2-g8-client-1.0-SNAPSHOT-bin.tar.gz
    tar -xzf ./server/target/tpe2-g8-server-1.0-SNAPSHOT-bin.tar.gz
    ```
3. Ejecutar el comando **chmod u+x** sobre los scripts *queryX.sh* y *run-server.sh* para otorgarles permiso de ejecuccion.
Puede utilizar el script *run-Permits.sh* el cual le otorgara permisos a todos los demas scripts.
    
    ```
    chmod u+x ./tpe2-g8-server-1.0-SNAPSHOT/run-server.sh 
    chmod u+x ./tpe2-g8-client-1.0-SNAPSHOT/run-Permits.sh
    cd ./tpe2-g8-client-1.0-SNAPSHOT
    ./run-Permits.sh
    ```
    
4. Por último copiamos los archivos csv a la raíz del cliente.

    ```
    cp aeropuertos.csv tpe2-g8-client-1.0-SNAPSHOT
    cp movimientos.csv tpe2-g8-client-1.0-SNAPSHOT
    ```
    
    ### Disponibilizar el servidor
    - Correr el script **run-server.sh**.
    ```
    cd ./tpe2-g8-server-1.0-SNAPSHOT
    ./run-server.sh
    ``` 
    ### Clientes de query
    - Para realizar la consulta 1, correr el script **query1.sh**.
    
    Parametros: -Daddresses refiere a las direcciones IP de los nodos con sus puertos (una o más), -DinPath indica el path donde están ambos archivos de entrada aeropuertos.csv y movimientos.csv, -DoutPath indica el path donde estarán ambos archivos de salida query1.csv y query1.txt.
    ```
    ./query1.sh -Daddresses=192.168.1.7:5701 -DinPath=. -DoutPath=.
    ```
    - Para realizar la consulta 2, correr el script **query2.sh**.
    
    Parametros: -Daddresses refiere a las direcciones IP de los nodos con sus puertos (una o más), -DinPath indica el path donde están ambos archivos de entrada aeropuertos.csv y movimientos.csv, -DoutPath indica el path donde estarán ambos archivos de salida query2.csv y query2.txt, -Dn numero de aerolíneas con mayor porcentaje de movimientos de cabotaje a mostrar.
    ```
    ./query2.sh -Daddresses=192.168.1.7:5701 -DinPath=. -DoutPath=. -Dn=5
    ``` 
    - Para realizar la consulta 3, correr el script **query3.sh**.
    
    Parametros: -Daddresses refiere a las direcciones IP de los nodos con sus puertos (una o más), -DinPath indica el path donde están ambos archivos de entrada aeropuertos.csv y movimientos.csv, -DoutPath indica el path donde estarán ambos archivos de salida query3.csv y query3.txt.
    ```
    ./query3.sh -Daddresses=192.168.1.7:5701 -DinPath=. -DoutPath=.
    ```
    - Para realizar la consulta 4, correr el script **query4.sh**.
    
    Parametros: -Daddresses refiere a las direcciones IP de los nodos con sus puertos (una o más), -DinPath indica el path donde están ambos archivos de entrada aeropuertos.csv y movimientos.csv, -DoutPath indica el path donde estarán ambos archivos de salida query4.csv y query4.txt, -Doaci aeropuerto origen, -Dn numero aeropuertos destino con mayor cantidad de movimientos de despegue que tienen como origen a un aeropuerto oaci.
    ```
    ./query4.sh -Daddresses=192.168.1.7:5701 -DinPath=. -DoutPath=. -Doaci=SAEZ -Dn=5
    ```
    - Para realizar la consulta 5, correr el script **query5.sh**.
    
    Parametros: -Daddresses refiere a las direcciones IP de los nodos con sus puertos (una o más), -DinPath indica el path donde están ambos archivos de entrada aeropuertos.csv y movimientos.csv, -DoutPath indica el path donde estarán ambos archivos de salida query5.csv y query5.txt, -Dn numero aeropuertos con menor porcentaje de vuelos privados.
    ```
    ./query5.sh -Daddresses=192.168.1.7:5701 -DinPath=. -DoutPath=. -Dn=5
    ```    
    - Para realizar la consulta 6, correr el script **query6.sh**.
    
    Parametros: -Daddresses refiere a las direcciones IP de los nodos con sus puertos (una o más), -DinPath indica el path donde están ambos archivos de entrada aeropuertos.csv y movimientos.csv, -DoutPath indica el path donde estarán ambos archivos de salida query6.csv y query6.txt, -Dmin= numero minimo de movimientos compartido entre provincias
     ```
     ./query6.sh -Daddresses=192.168.1.7:5701 -DinPath=. -DoutPath=. -Dmin=1000
     ```
    
    
    
    


