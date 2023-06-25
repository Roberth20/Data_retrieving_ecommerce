"""Funciones de utilidad para el ajuste de los datos antes de ser
enviados a Multivende"""
# Import libraries
import numpy as np
import pandas as pd

def get_serialized_data(p, customs_data):
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
    """
    # Preparing data
    p = p.fillna(np.nan)
    p_customs = p.dropna()
    p_customs = p_customs.drop(p_customs.index[p_customs.index.isin(std)])
    custom_p = {}
    custom_v = {}
    for i in p_customs.index:
        words = i.split("-")
        if len(words) > 2:
            # If the custom attribute have more words when splitting, join them again
            w = "-".join([words[0], words[1]])
            words[0] = w
        if "Productos" in words[-1]:
            # adding custom_attribute id : custom_attribute value
            mask = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0])
            id = customs_data["id"][mask].values[0]
            mask2 = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0]) & (customs_data["option_name"] == p[i])
            # If the attribute have options, obtain it
            if customs_data["option_id"][mask2].shape[0] > 0:
                custom_p[id] = customs_data["option_id"][mask2].values[0]
            else:
                custom_p[id] = p[i]
        elif "Versiones" in words[-1]:
            # adding custom_attribute id : custom_attribute value
            mask = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0])
            id = customs_data["id"][mask].values[0]
            mask2 = (customs_data["name_set"] == words[-1]) & (customs_data["name"] == words[0]) & (customs_data["option_name"] == p[i])
            # If the attribute have options, obtain it
            if customs_data["option_id"][mask2].shape[0] > 0:
                custom_v[id] = customs_data["option_id"][mask2].values[0]
            else:
                custom_v[id] = p[i]
                
    return custom_p, custom_v