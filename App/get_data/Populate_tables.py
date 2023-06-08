import pandas as pd
import mariadb
from flask import current_app

def get_cursor_db():
    # Connect to MariaDB Platform
    try:
        conn = mariadb.connect(
            user= current_app.config["USER_DB"],
            password= current_app.config["PASSWORD_DB"],
            port= current_app.config["PORT_DB"],
            database= current_app.config["NAME_DB"]

        )
        # Get Cursor
        cur = conn.cursor()
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(0)

def upload_data_products(df, db):
    message = "<ul>"
    engine = db.engine
    rename_columns = pd.DataFrame(columns = ["Viejo", "Nuevo"])
    for c in df.columns:
        if len(c) > 64:
            if "netbook" in c:
                rename_columns.loc[len(rename_columns), :] = [c, "Tamaño máximo de la notebook-Mercado Libre Productos (HB)"]
                df.columns = df.columns.where(~df.columns.str.contains("netbook"), "Tamaño máximo de la notebook-Mercado Libre Productos (HB)")
                continue
            if "RESISTANCE_BAND" in c:
                rename_columns.loc[len(rename_columns), :] = [c, "RESISTANCE_BAND_WITH_HANDLES-Mercado Libre Productos (HB)"]
                df.columns = df.columns.where(~df.columns.str.contains("RESISTANCE_BAND"), "RESISTANCE_BAND_WITH_HANDLES-Mercado Libre Productos (HB)")
                continue
            if "vajilla" in c:
                rename_columns.loc[len(rename_columns), :] = [c, "Material del escurridor de vajilla-Mercado Libre Productos (HB)"]
                df.columns = df.columns.where(~df.columns.str.contains("vajilla"), "Material del escurridor de vajilla-Mercado Libre Productos (HB)")
                continue
            if len(c.split("-")) > 2:
                rename_columns.loc[len(rename_columns), :] = [c, "-".join([c.split("-")[0], c.split("-")[2]])]
                df.columns = df.columns.where(~df.columns.str.contains(c.split("-")[1]), "-".join([c.split("-")[0], c.split("-")[2]]))

    with engine.connect() as connection:
        result = connection.execute(db.text("SELECT * FROM Renombre_categorias LIMIT 1"))
        if result.first() == None:
            rename_columns.to_sql("Renombre_categorias", engine, if_exists = "append", index=False)
            message += "<li>Tabla 'Renombre_categorias' actualizada con exito.</li>"
        else:
            message += "<li>Hubo un error al actualizar 'Renombre_categorias'</li>"            


    df.drop(df.columns[df.columns.str.contains("Material del trípode")][1], axis=1, inplace=True)
    df.drop("Número de focos-Ripley Productos", axis=1, inplace=True)

    std = df.columns[:21]
    mlc = df.columns[df.columns.str.contains("Mercado Libre")]
    fl = df.columns[df.columns.str.contains("Falabella")]
    rp = df.columns[df.columns.str.contains("Ripley")]
    pr = df.columns[df.columns.str.contains("Paris")]
    sp = df.columns[df.columns.str.contains("Shopify")]

    tables = {"standard":std, 
              "MercadoLibre": mlc.to_list() + ["IDENTIFICADOR_HIJO","IDENTIFICADOR_PADRE"],
              "Falabella": fl.to_list() + ["IDENTIFICADOR_HIJO","IDENTIFICADOR_PADRE"],
              "Ripley":rp.to_list() + ["IDENTIFICADOR_HIJO","IDENTIFICADOR_PADRE"],
              "Paris":pr.to_list() + ["IDENTIFICADOR_HIJO","IDENTIFICADOR_PADRE"],
              "Shopify":sp.to_list() + ["IDENTIFICADOR_HIJO","IDENTIFICADOR_PADRE"]}

    for table in tables:
        with engine.connect() as connection:
            result = connection.execute(db.text(f"SELECT * FROM Productos_{table} LIMIT 1"))
            if result.first() == None:
                df[tables[table]].to_sql(f"Productos_{table}", engine, if_exists = "append", index=False)
                message += "<li>Tabla 'Productos_{table}' populada con exito.</li>"
            else:
                message += "<li>La tabla 'Productos_standard' no esta vacia. Intente con insert o append filas.</li>"
                
    message += "</ul>"
    return message


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
            
def upload_data_falabella2(df, cursor):
    for i, row in df.iterrows():
        try:
            data = row.where(row.notna(), None)
            query = """INSERT INTO Mapeo_Falabella(
                Mapeo, Atributo) VALUES(
                ?, ?)
            """
            cursor.execute(query, data = (data['Mapeo'], data["Atributo"]))
        except mariadb.Error as e:
            print(f"Error: {e}")
            sys.exit(0)
            
def upload_data_MLC2(df, cursor):
    for i, row in df.iterrows():
        try:
            data = row.where(row.notna(), None)
            query = """INSERT INTO Mapeo_MercadoLibre(
                Mapeo, Atributo) VALUES(
                ?, ?)
            """
            cursor.execute(query, data = (data['Mapeo'], data["Atributo"]))
        except mariadb.Error as e:
            print(f"Error: {e}")
            sys.exit(0)
            
def upload_data_Ripley2(df, cursor):
    for i, row in df.iterrows():
        try:
            data = row.where(row.notna(), None)
            query = """INSERT INTO Mapeo_Ripley(
                Mapeo, Atributo) VALUES(
                ?, ?)
            """
            cursor.execute(query, data = (data['Mapeo'], data["Atributo"]))
        except mariadb.Error as e:
            print(f"Error: {e}")
            sys.exit(0)
            
def upload_data_Paris2(df, cursor):
    for i, row in df.iterrows():
        try:
            data = row.where(row.notna(), None)
            query = """INSERT INTO Mapeo_Paris(
                Mapeo, Atributo) VALUES(
                ?, ?)
            """
            cursor.execute(query, data = (data['Mapeo'], data["Atributo"]))
        except mariadb.Error as e:
            print(f"Error: {e}")
            sys.exit(0)
            
def upload_cat_map(df, cursor):
    for i, row in df.iterrows():
        try:
            query = """INSERT INTO Mapeo_Categorias(
                Multivende, MercadoLibre, Falabella, Ripley,
                Paris, Paris_Familia) VALUES(
                ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(query, data = (row["Categoria Multivende"], 
                                          row["Categoria Mercadolibre"],
                                         row["Categoria Falabella"],
                                         row["Categoria Ripley "],
                                         row["Categoria Paris"],
                                         row["Paris Familia"]))
        except mariadb.Error as e:
            print(f"Error: {e}")
            sys.exit(0)