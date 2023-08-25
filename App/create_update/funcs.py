"""Funciones de utilidad para el ajuste de los datos antes de ser
enviados a Multivende"""
# Import libraries
import numpy as np
import pandas as pd

def get_serialized_data(p, customs_data, std, market):
    """Funcion para transformacion de datos en formato apto para
    ser enviado como JSON
    
    Input : 
    -------
      *  p : pandas.Series. Informacion del producto.
        
      *  customs_data : pandas.DataFrame. Tabla con la informacion de
    los atributos customs creados por el usuario (Deben estar registrados
    en multivende).
    
    Output:
    -------
      *  custom_p : dict. Informacion ajustada relacionada al producto.
    
      *  custom_v : dict. Informacion ajustada relacionada a la version del 
    producto.
    
      *  std : array-like. Atributos estandar de los productos.
      
      *  market : str. Marketplace de atributos a procesar.
    """
    # Preparing data
    p = p.fillna(np.nan)
    p_customs = p.dropna()
    p_customs = p_customs.drop(p_customs.index[p_customs.index.isin(std)])
    # Depending of the market, we use the customs attribute need it
    if market == "Falabella":
        p_customs = p_customs[p_customs.index.str.contains("Falabella")]
    if market == "Paris":
        p_customs = p_customs[p_customs.index.str.contains("Paris")]
    if market == "Ripley":
        p_customs = p_customs[p_customs.index.str.contains("Ripley")]
    if market == "MercadoLibre":
        p_customs = p_customs[p_customs.index.str.contains("Mercado")]
    # Set de variables
    custom_p = {}
    custom_v = {}
    for i in p_customs.index:
        words = i.split("-")
        if len(words) > 2:
            w = "-".join([words[0], words[1]])
            words[0] = w
        if "Productos" in words[-1]:
            # adding custom_attribute id : custom_attribute value
            # Check the id of the attribute
            mask = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0])
            id = customs_data["id"][mask].values[0]
            # Filter if the attribute have options equal to the one in the product
            mask2 = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0]) & (customs_data["option_name"] == p[i])
            # Filter if the attribute dont have options
            mask3 = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0]) & (customs_data["option_name"].isna())
            if customs_data["option_id"][mask2].shape[0] > 0:
                custom_p[id] = customs_data["option_id"][mask2].values[0]
            elif customs_data["option_id"][mask3].shape[0] > 0:
                custom_p[id] = p[i]
            elif customs_data["option_id"][mask3].shape[0] == 0:
                # Filter if the attribute have options id equal in the product
                mask4 = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0]) & (customs_data["option_id"] == p[i])
                if customs_data["option_id"][mask4].shape[0] > 0:
                    custom_p[id] = p[i]
                else:
                    return f"El atributo {p[i]} de {i} para el producto {p['name']} no es valido. Se recomienda asegurar que exista en la base de datos de Multivende o actualizar la local.", p[i]
            
        elif "Versiones" in words[-1]:
            # adding custom_attribute id : custom_attribute value
            mask = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0])
            id = customs_data["id"][mask].values[0]
            mask2 = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0]) & (customs_data["option_name"] == p[i])
            mask3 = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0]) & (customs_data["option_name"].isna())
            if customs_data["option_id"][mask2].shape[0] > 0:
                custom_v[id] = customs_data["option_id"][mask2].values[0]
            elif customs_data["option_id"][mask3].shape[0] > 0:
                custom_v[id] = p[i]
            elif customs_data["option_id"][mask3].shape[0] == 0:
                mask4 = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0]) & (customs_data["option_id"] == p[i])
                if customs_data["option_id"][mask4].shape[0] > 0:
                    custom_v[id] = p[i]
                else:
                    return f"El atributo {p[i]} de {i} para el producto {p['name']} no es valido. Se recomienda asegurar que exista en la base de datos de Multivende o actualizar la local.", p[i]
                
    return custom_p, custom_v