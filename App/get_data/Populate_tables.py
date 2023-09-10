import pandas as pd
from flask import current_app

#def get_cursor_db():
    # Connect to MariaDB Platform
 #   try:
  #      conn = mariadb.connect(
   #         user= current_app.config["USER_DB"],
    #        password= current_app.config["PASSWORD_DB"],
     #       port= current_app.config["PORT_DB"],
      #      database= current_app.config["NAME_DB"],
       #     host=current_app.config["HOST_DB"]
        #)
        # Get Cursor
        #cur = conn.cursor()
    #except mariadb.Error as e:
     #   print(f"Error connecting to MariaDB Platform: {e}")
      #  sys.exit(0)

def upload_data_products(df, db):
    message = "<ul>"
    engine = db.engine
    rename_columns = pd.DataFrame(columns = ["Viejo", "Nuevo"])
    for c in df.columns:
        if len(c) > 64:
            if "Mercado Libre Productos" in c:
                words = c.split("Mercado Libre Productos")
                rename_columns.loc[len(rename_columns), :] = [c, "".join(words)]
                df.columns = df.columns.where(~df.columns.str.contains(words[0]), "".join(words))
            if "Paris Productos" in c:
                words = c.split("-")
                rename_columns.loc[len(rename_columns), :] = [c, "".join([words[0], words[-1]])]
                df.columns = df.columns.where(~df.columns.str.contains(words[1]), "".join([words[0], words[-1]]))

    with engine.connect() as connection:
        try:
            rename_columns.to_sql("Renombre_categorias", engine, if_exists = "replace", index=False)
            message += "<li>Tabla 'Renombre_categorias' actualizada con exito.</li>"  
        except Exception as e:
            message += f"<li>Hubo un error al actualizar 'Renombre_categorias' + {e}</li>"   
        

    try:
        df.drop(df.columns[df.columns.str.contains("Material del trípode")][1], axis=1, inplace=True)
    except:
        current_app.logger.info("The column: Material del trípode. NO se encuentra en el dataframe")
        
    try:
        df.drop("Número de focos-Ripley Productos", axis=1, inplace=True)
    except:
        current_app.logger.info("The column: Número de focos-Ripley Productos. NO se encuentra en el dataframe")

    std = df.columns[:23]
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
            try:
                df[tables[table]].to_sql(f"Productos_{table}", engine, if_exists = "replace", index=False)
                current_app.logger.debug(f"<li>Tabla 'Productos_{table}' populada con exito.</li>")
            except Exception as e:
                current_app.logger.debug(f"<li>La tabla 'Productos_{table}' tuvo un error {e}</li>")
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