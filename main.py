import pandas as pd
import yaml
import mysql.connector

def connector_mysql():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="sebastian12",
    database="food-com-recipes-and-interactions"
    )
    return mydb

def cleanData(df):
    df1 = df.notnull()
    df  = df[df1]
    df  = df.dropna()
    df  = df.drop_duplicates()
    return df

def config():
    """
    Read file config yaml
    """
    yaml_file = open("config.yaml", 'r')
    config = yaml.safe_load(yaml_file)
    yaml_file.close()
    return config


def read_csv(name_file):
    df = pd.read_csv(name_file)
    return df

def relations():
    mycursor = mydb.cursor()
    mycursor.execute("select recipe_id from raw_interactions")
    myresult = mycursor.fetchall()

   
    for i in myresult:
        mycursor.execute("select id from raw_interactions where recipe_id = {}".format(i[0]))
        myresult1 = mycursor.fetchall()  
        for j in myresult1:
            insert_query("raw_recipe_has_raw_interactions", 
            "raw_recipe_id , raw_interactions_id", (i[0], j[0]))


def insert_query(table, columns, values):
    mycursor = mydb.cursor()
    try:
        mycursor.execute('INSERT INTO {} ({}) VALUES {};'.format(table, columns, values))
        mydb.commit()
    except:
        pass

def insert_data_funcion(df, table):
    df1 = df.loc[1,:]
    b = df1.index.tolist()
    b1 = ",".join(b)
    df_new = df.loc[:,b]
    df_new.apply(lambda x: insert_query(table, b1, tuple(x)), axis=1)

def update_query(table, columns, data, id):
    i = columns.index("id")
    columns.remove("id")
    data.pop(i)
    a = "-".join(map(str, zip(columns, data)))
    a = a.replace("(", " ").replace(")", " ").replace("',", ' ="').replace("'", " ").replace("-", '",')
    a = a.replace(" ", "") + '"'
    query = 'UPDATE {} SET {} WHERE id = {};'.format(table, a, id)
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mydb.commit()

def update_data(table, df):
    df = df.loc[:,:]
    df.apply(lambda x: update_query(table, 
    x.index.tolist(), x.values.tolist(), x.id), axis=1)

if __name__ == "__main__":
    config = config()
    RAW_RECIPES = read_csv(config["RAW_RECIPES"])    
    RAW_RECIPES = cleanData(RAW_RECIPES)
    mydb = connector_mysql()
    print("Waiting while it fills raw_recipes table")
    insert_data_funcion(RAW_RECIPES, "raw_recipe")
    print("Waiting while it fills PP_users table")
    PP_USERS = read_csv(config["PP_USERS"])    
    PP_USERS = cleanData(PP_USERS)
    mydb = connector_mysql()
    insert_data_funcion( PP_USERS, "PP_users")
    print("Waiting while it fills PP_recipes table")
    PP_RECIPES = read_csv(config["PP_RECIPES"])
    PP_RECIPES = cleanData(PP_RECIPES)
    mydb = connector_mysql()
    update_data("raw_recipe", PP_RECIPES)
    print("Waiting while it fills raw_interactions table")
    RAW_INTERACTIONS = read_csv(config["RAW_INTERACTIONS"])
    RAW_INTERACTIONS = cleanData(RAW_INTERACTIONS)
    mydb = connector_mysql()
    insert_data_funcion(RAW_INTERACTIONS, "raw_interactions")
    print("Waiting while it fills test_interactions table")
    TEST_INTERACTIONS = read_csv(config["TEST_INTERACTIONS"])
    TEST_INTERACTIONS = cleanData(TEST_INTERACTIONS)
    mydb = connector_mysql()
    insert_data_funcion(TEST_INTERACTIONS, "interactions")
    print("Waiting while it fills train_interactions table")
    TRAIN_INTERACTIONS = read_csv(config["TRAIN_INTERACTIONS"])
    TRAIN_INTERACTIONS = cleanData(TRAIN_INTERACTIONS)
    mydb = connector_mysql()
    insert_data_funcion(TRAIN_INTERACTIONS, "interactions")
    print("Waiting while it fills validation_interactions table")
    VALIDATION_INTERACTIONS = read_csv(config["VALIDATION_INTERACTIONS"])
    VALIDATION_INTERACTIONS = cleanData(VALIDATION_INTERACTIONS)
    mydb = connector_mysql()
    insert_data_funcion(VALIDATION_INTERACTIONS, "interactions")
    relations()

