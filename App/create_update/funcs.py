import numpy as np
import pandas as pd

def get_serialized_data(p, customs_data):
    p = p.fillna(np.nan)
    p_customs = p.dropna()
    p_customs = p_customs.drop(p_customs.index[p_customs.index.isin(std)])
    custom_p = {}
    custom_v = {}
    for i in p_customs.index:
        words = i.split("-")
        if len(words)>2:
            # caso mercado libre
            continue
        if "Productos" in words[1]:
            # adding custom_attribute id : custom_attribute value
            mask = (customs_data["name_set"] == words[1]) & (customs_data["name"] == words[0])
            id = customs_data["id"][mask].values[0]
            mask2 = (customs_data["name_set"] == words[1]) & (customs_data["name"] == words[0]) & (customs_data["option_name"] == p[i])
            if customs_data["option_id"][mask2].shape[0] > 0:
                custom_p[id] = customs_data["option_id"][mask2].values[0]
            else:
                custom_p[id] = p[i]
        elif "Versiones" in words[1]:
            # adding custom_attribute id : custom_attribute value
            mask = (customs_data["name_set"] == words[1]) & (customs_data["name"] == words[0])
            id = customs_data["id"][mask].values[0]
            mask2 = (customs_data["name_set"] == words[1]) & (customs_data["name"] == words[0]) & (customs_data["option_name"] == p[i])
            if customs_data["option_id"][mask2].shape[0] > 0:
                custom_v[id] = customs_data["option_id"][mask2].values[0]
            else:
                custom_v[id] = p[i]
                
    return custom_p, custom_v