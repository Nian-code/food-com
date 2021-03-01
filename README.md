# food-com
Este repositorio tiene 2 scripts para genera generar una base en mysql server

Instalar los paquetes necesario:

`pip3 install -r requirements.txt`

Recordar elegir las credenciales para poder acceder al servidor mysql en main.py
`def connector_mysql():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="food-com-recipes-and-interactions"
    )
    return mydb`
    
    
