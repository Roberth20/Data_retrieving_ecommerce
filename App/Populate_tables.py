import mariadb
import pandas as pd
import DATA
import sys

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user= DATA.USER,
        password= DATA.PASSWORD,
        port= DATA.PORT,
        database="multivende"

    )
    # Get Cursor
    cur = conn.cursor()
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(0)
    
def upload_data_falabella(df, cursor):
    for i, row in df.iterrows():
        try:
            data = row.where(row.notna(), None)
            if data["Description"] != None:
                data["Description"] = data["Description"].split("//")[0]
            query = """INSERT INTO Atributos_Falabella(
                Label, GroupName, isMandatory,
                isGlobalAttribute, Description, ProductType,
                AttributeType, Options, Category) VALUES(
                ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(query, data = (data['Label'], data["GroupName"], int(data['isMandatory']),
                                      int(data['IsGlobalAttribute']), data['Description'],
                                      data['ProductType'], data['AttributeType'],
                                      data['Options'], data['Category']))
        except mariadb.Error as e:
            print(f"Error: {e}")
            sys.exit(0)
            
def upload_data_mlc(df, cursor):
    for i, row in df.iterrows():
        try:
            data = row.where(row.notna(), None)
            query = """INSERT INTO Atributos_MercadoLibre(
                Label, Category, Tags, Hierarchy, AttributeType,
                GroupName, Options) VALUES(
                ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(query, data = (data['name'], data["category"], data["tags"],
                                         data["hierarchy"], data["value_type"], 
                                         data["attribute_group_name"], data["values"]))
        except mariadb.Error as e:
            print(f"Error: {e}")
            sys.exit(0)
            
def upload_data_paris(df, cursor):
    for i, row in df.iterrows():
        try:
            data = row.where(row.notna(), None)
            query = """INSERT INTO Atributos_Paris(
                Label, Category, Family, AttributeType,
                GroupName, isMandatory) VALUES(
                ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(query, data = (data['name'], data["category"], data["family"],
                                         data["inputType"], data["attributeGroup"], 
                                         data["isRequired"]))
        except mariadb.Error as e:
            print(f"Error: {e}")
            sys.exit(0)
            
def upload_data_ripley(df, cursor):
    for i, row in df.iterrows():
        try:
            data = row.where(row.notna(), None)
            query = """INSERT INTO Atributos_Ripley(
                Label, Category, RequirementLevel, AttributeType,
                Options, Variant) VALUES(
                ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(query, data = (data['label'], data["category"], data["requirement_level"],
                                         data["type"], data["type_parameters"], 
                                         data["variant"]))
        except mariadb.Error as e:
            print(f"Error: {e}")
            sys.exit(0)