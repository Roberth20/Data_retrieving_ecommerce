import os

### Revisamos instalaciones
try:
    import requests
except ImportError:
    print("Instalando: requests\n")
    os.system("pip install requests")
finally:
    import requests

try:
    import pandas as pd
except ImportError:
    print("Instalando: pandas\n")
    os.system("pip install pandas")
finally:
    import pandas as pd
    
import json
    
print("Conectando con la API.")
# Definimos url de accesso a las categorias
base_url = "https://api.mercadolibre.com/sites/MLC/categories"
# Obtenemos los datos
message = requests.get(base_url)
cat_p = message.json()

# Definimos una funcion que nos ayude a construir el arbol de categorias
def category_tree(categories: list, tree: list):
    """Funcion para construccion del arbol de categorias.
    
    Input : 
    --------
    * categories : list, lista con diccionarios con informacion de cada 
    categoria.
    * tree : list, Objeto para almacenar los datos.
    
    Output : 
    ---------
    None
    """
    for item in categories:
        # Para cada item, obtenemos la informacion individual
        base_url = f"https://api.mercadolibre.com/categories/{item['id']}"
        message = requests.get(base_url)
        sub_cat = message.json()
        # Si la categoria tiene subcategorias, se llama la funcion de nuevo.
        sub_cat = sub_cat["children_categories"]
        tree.append(item)
        category_tree(sub_cat, tree)
        
def category_tree_d(categories: list, tree: dict):
    """Funcion para construccion del arbol de categorias.
    
    Input : 
    --------
    * categories : list, lista con diccionarios con informacion de cada 
    categoria.
    * tree : list, Objeto para almacenar los datos.
    
    Output : 
    ---------
    None
    """
    for item in categories:
        # Para cada item, obtenemos la informacion individual
        base_url = f"https://api.mercadolibre.com/categories/{item['id']}"
        message = requests.get(base_url)
        sub_cat = message.json()
        # Si la categoria tiene subcategorias, se llama la funcion de nuevo.
        sub_cat = sub_cat["children_categories"]
        tree[item["name"]] = {}
        category_tree_d(sub_cat, tree[item["name"]])

# Creamos arbol de categorias
categories = []
print("Recuperando categorias... Puede tardar algunos minutos.")
#category_tree(cat_p, categories)
cat_dict = {}
category_tree_d(cat_p, cat_dict)

# Definimos el objeto para almacenar los atributos de todas las categorias
attributes = []
print("Generando atributos.")
#for d in categories:
    # De cada categoria obtenemos sus atributos
 #   url = f"https://api.mercadolibre.com//categories/{d['id']}/attributes"
  #  message = requests.get(url)
   # message = message.json()
    #for item in message:
        # Para mayor facilidad, agregamos a que categoria pertenece el atributo
     #   item["category"] = d["name"]
      #  attributes.append(item)

# Cargamos DataFrame
#df = pd.DataFrame(attributes)
# Removemos los valores que no son necesarios para el estudio
#df.drop(["id", "value_max_length", "example", "relevance", "attribute_group_id"], axis=1, inplace=True)
#df.drop(["tooltip", "allowed_units", "default_unit", "hint", "type"], axis=1,inplace=True)

# Organizamos la tabla de datos y guardamos
#df = df[["category", "name", "tags", "hierarchy", "value_type", "attribute_group_name", "values"]]
#df.to_excel("MLC_atributos_categorias.xlsx", index=False)
with open("cat_mlc.json", "w") as f:
    json.dump(cat_dict, f)
print("Proceso finalizado.")