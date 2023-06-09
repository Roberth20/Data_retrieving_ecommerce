import pandas as pd
import textdistance

"""Modulo de asistencia para la logica de asignacion de colores de los atributos
de un producto. Y funciones de utilidad para mapeo de categorias y atributos."""

def translate_standard(atm, transformation):
    """Traduccion de atributos marletkplace (espannol) al ingles (multivende).
    Esta funcion se encadena despues de la funcion get_att_map()

    Input :
    -------
    * atm: tuple or list. Contiene la etiqueta del marketplace correspondiente y la lista
    de atributos mapeados. len(atm) == 2 con atm[1] conteniendo los atributos

    * transformation: pandas.DataFrame. Tabla con las traducciones de los atributos estandar

    Output :
    -------
    * atm: tuple or list. Igual a la de entrada pero con los atributos estandar traducidos al ingles
    de multivende
    """
    # Para cada atributo
    for i in range(len(atm[1])):
        # Revisamos si tiene traduccion
        if any(transformation.Nuevo == atm[1][i]):
            # Lo cambiamos por su traduccion
            atm[1][i] = transformation.Original[transformation.Nuevo == atm[1][i]].values[0]

    return atm

def get_attributes(cat_map, atts_map):
    """
    Para una categoria en especifico del marketplace, obtenemos los atributos que
    debe tener dicho producto.

    Input :
    -------
    * cat_map: tuple or list of len(cat_map) == 2. Contiene como primer valor el indicador del
    marketplace correspondiente y como segundo valor, el nombre de la categoria de interes.

    * atts_map: dict. Diccionario en cual cada key representa un marketplace y contiene la
    tabla con los atributos y categorias del marketplace.

    Return :
    --------
    * tuple: El primer valor, es el marketplace con el cual se esta trabajando y el segundo,
    la lista de atributos unicos pertencientes a la categoria buscada.
    """
    # Obtenemos la tabla de interes
    frame = atts_map[cat_map[0]]
    # Segun sea el caso, obtenemos los atributos del marketplace
    if cat_map[0] == "PR" or cat_map[0] == "RP":
        return cat_map[0], frame[frame.iloc[:, 1] == cat_map[1]].iloc[:, 0].unique()
    else:
        return cat_map[0], frame[frame.iloc[:, 2] == cat_map[1]].iloc[:, 0].unique()

def get_att_map(lm, map_att):
    """
    Funcion mapeadora entre los atributos de un marketplace y los atributos en multivende.
    Esta funcion se encadena en despues de get_attributes().

    Input :
    -------
    * lm: tuple, len(lm) == 2. Contiene la identificacion del marketplace y una lista de los
    atributos asociados a la categoria que se trabaja.

    * map_att: dict. Diccionario que contiene como keys las identificacion de los marketplaces
    y como valores, las tablas de datos que mapean los atributos ente multivende y el martketplace.

    Return :
    --------
    * Tuple. El primer elemento es la identificacion del marketplace. El segundo, una lista de
    los atributos mapeados a multivende.
    """
    atributos = []
    # Para cada atributo
    for l in lm[1]:
        # Buscamos en la tabla correspondiente todas las coincidencias del mismo
        # en toda la columna de atributos del marketplace
        mask = map_att[lm[0]].iloc[:, 1].str.lower() == l.lower()
        # Obtenemos los atributos de multivende que se corresponden
        items = map_att[lm[0]][mask].copy()
        # GUardamos los atributos unicos
        if items.shape[0] == 0:
            continue
        # GUardamos los atributos unicos
        items["Score"] = [textdistance.jaccard.similarity(i.lower().split(" "), l.lower().split(" ")) for i in items.iloc[:, 0]]
        item = items.sort_values("Score", ascending=False).iloc[0, 0]
        if item not in atributos:
            atributos.append(item)

    return lm[0], atributos

def get_map(categoria, maps, atts_map, map_att, transformation):
    """Funcion para mapeo de valores multivende a valores de marketplace.
    
    Input : 
    -------
      *  categoria : string. Categoria del producto a mapear.
      
      *  maps : pandas.DataFrame. Tabla con el mapeo de categorias Multivende
    a categoria correspondiente en cada marketplace
    
      *  atts_map : dict. Diccionario en cual cada key representa un marketplace y contiene la
    tabla con los atributos y categorias del marketplace.
    
      *  map_att : dict. Diccionario que contiene como keys las identificacion de los marketplaces
    y como valores, las tablas de datos que mapean los atributos ente multivende y el martketplace.
    
      * transformation : pandas.DataFrame. Tabla con las traducciones de los atributos estandar
      
    Output : 
    --------
      *  mcat : list. Lista con mapeos de atributos para cada marketplace
    """
    mcat = []
    for c in maps.columns[1:5].values:
        # Get the mapped category
        info = maps[c][categoria == maps.iloc[:, 0]].values
        if len(info) >= 1:
            info = info[0]
        else:
            continue
        if "Paris" in c:
            # For Paris, we use the family instead category
            info = maps[categoria == maps.iloc[:, 0]].iloc[:, -1].values
            if len(info) >= 1:
                info = info[0]
            # Make adjustments
            info = info.replace(u"\xa0", "")
            category = "PR", info
            # Process the data
            lm = get_attributes(category, atts_map)
            atm = get_att_map(lm, map_att)
            latm = translate_standard(atm, transformation)
            mcat.append(latm)
        elif "Ripley" in c:
            # Make adjustments
            words = info.split(" > ")
            words[-1] = words[-1].replace(u"\xa0", "")
            category = "RP", words[-1]
            # Process the data
            lm = get_attributes(category, atts_map)
            atm = get_att_map(lm, map_att)
            latm = translate_standard(atm, transformation)
            mcat.append(latm)
        elif "MercadoLibre" in c:
            # Make adjustments
            words = info.split(" - ")
            category = "MLC", words[-1]
            # Process data
            lm = get_attributes(category, atts_map)
            atm = get_att_map(lm, map_att)
            latm = translate_standard(atm, transformation)
            mcat.append(latm)
        else:
            # Make adjustments
            words = info.split(" > ")
            words[-1] = words[-1].replace(u"\xa0", "")
            category = "FL", words[-1]
            # Process data
            lm = get_attributes(category, atts_map)
            atm = get_att_map(lm, map_att)
            latm = translate_standard(atm, transformation)
            mcat.append(latm)
    return mcat

def limpieza_de_atributo(atributo):
    """Funcion de limpiea de atributos. Hay atributos con nombres largos y
    no mapeados, se obtiene la parte importante y que si esta mapeada de cada 
    uno.
    """
    if "-" in atributo:
        return atributo.split(" - ")[0]
    else:
        return atributo

def missing_info(c, maps, atts, map_att, std_transformation):
    """Funcion que prepara la coloracion de los datos en el excel.
    
    Toma cada producto y denota en color los atributos faltantes que debe tener
    segun la categoria correspondiente y en cada marketplace.
    
    Input : 
    -------
      *  c : pandas.Series. Producto y atributos.
      
      *  maps : pandas.DataFrame. Tabla con el mapeo de categorias Multivende
    a categoria correspondiente en cada marketplace
    
      *  atts_map : dict. Diccionario en cual cada key representa un marketplace y contiene la
    tabla con los atributos y categorias del marketplace.
    
      *  map_att : dict. Diccionario que contiene como keys las identificacion de los marketplaces
    y como valores, las tablas de datos que mapean los atributos ente multivende y el martketplace.
    
      * transformation : pandas.DataFrame. Tabla con las traducciones de los atributos estandar
    
    """
    # Get attributes for the category of the product
    labels = get_map(c.ProductCategory, maps, atts, map_att, std_transformation)
    # Set an empty Serie equal in shape that will hold the colors
    arr = pd.Series(index=c.index, dtype="object")
    for market in labels:
        if market[0] == "MLC":
            # Get the attributes
            std = arr.index[:20]
            index = [idx for idx in arr.index if "HB" in idx]
            mask = []
            for l in market[1]:
                for i in index:  
                    arr[i] = None
                    if l.lower() in limpieza_de_atributo(i[:-29]).lower() and pd.isnull(c[i]):
                        # If the attribute is one on the table of products and is empty
                        mask.append(i)
                        break
                
                for s in std:
                    arr[s] = None
                    if l.lower() in s.lower() and pd.isnull(c[s]):
                        # Check is there is some standard attribute empty
                        mask.append(s)
                        break
            
            # Fill the selected attributes with the color
            for m in mask:
                arr[m] = "background-color: #D7DF01"

        # Repeated with each market
        elif market[0] == "FL":
            std = arr.index[:20]
            index = [idx for idx in arr.index if "Falabella" in idx]
            mask = []
            for l in market[1]:
                for i in index:
                    arr[i] = None
                    if l.lower() == limpieza_de_atributo(i[:-20]).lower() and pd.isnull(c[i]):
                        mask.append(i)
                        break
                for s in std:
                    arr[s] = None
                    if l.lower() == s.lower() and pd.isnull(c[s]):
                        mask.append(s)
                        break
                        
            for m in mask:
                arr[m] = "background-color: #FAAC58"
                        
        elif market[0] == "PR":
            std = arr.index[:20]
            index = [idx for idx in arr.index if "Paris" in idx]
            mask = []
            for l in market[1]:
                for i in index:
                    arr[i] = None
                    if l.lower() == limpieza_de_atributo(i[:-16]).lower() and pd.isnull(c[i]):
                        mask.append(i)
                        break
                for s in std:
                    arr[s] = None
                    if l.lower() == s[:-16].lower() and pd.isnull(c[s]):
                        mask.append(s)
                        break
                        
            for m in mask:
                arr[m] = "background-color: #819FF7"
                
        elif market[0] == "RP":
            std = arr.index[:20]
            index = [idx for idx in arr.index if "Ripley" in idx]
            mask = []
            for l in market[1]:
                for i in index:
                    arr[i] = None
                    if l.lower() == limpieza_de_atributo(i[:-17]).lower() and pd.isnull(c[i]):
                        mask.append(i)
                        break
                for s in std:
                    arr[s] = None
                    if l.lower() == s.lower() and pd.isnull(c[s]):
                        mask.append(i)
                        break
                        
            for m in mask:
                arr[m] = "background-color: #DA81F5"
                
        else:
            pass

    return arr

################################################################################

def limpieza_de_atributos(atributos):
    """Funcion de limpiea de atributos. Hay atributos con nombres largos y
    no mapeados, se obtiene la parte importante y que si esta mapeada de cada 
    uno."""
    att = []
    for cat in atributos:
        if "-" in cat:
            att.append(cat.split(" - ")[0])
        else:
            att.append(cat)
    return att

def mapeo_atributos(att_multi, att_market, sw):
    """Funcion de mapeo de atributos. Toma los atributos dentro de multivende y dentro
    de los marketplaces y los compara entre si.
    
    Input :
    -------
      *  att_multi : like list. Atributos dentro de multivende
      
      *  att_market : like list. Atributos del marketplace
      
      *  sw : list, array like. Lista de palabras stop del espanol.
      
    Return : 
    --------
      *  mapeo : pandas.DataFrame. Cada atributo del marketplace con su correspondiente
      equivalencia en multivende
    """
    mapeo = pd.DataFrame(columns = ["Atributo", "Mapeo"])
    for att in att_multi:
        words_multi = set(sorted([w.lower() for w in att.split() if w.lower() not in sw]))
        for i in att_market:
            words = set(sorted([w.lower() for w in i.split() if w.lower() not in sw]))
            if words_multi.issubset(words) and len(words_multi) == len(words):
                mapeo.loc[len(mapeo), :] = [att, i]
            elif words_multi.issubset(words):
                mapeo.loc[len(mapeo), :] = [att, i]
            else:
                pass

    return mapeo

def col_color(c):
    if "HB" in c:
        color = "background-color: yellow;"
    elif "Falabella" in c:
        color = "background-color: orange;"
    elif "Ripley" in c:
        color = "background-color: #BB8FCE;"
    elif "Paris" in c:
        color = "background-color: cyan;"
    elif "Shopify" in c:
        color = "background-color: green;"
    else:
        color = None

    return color