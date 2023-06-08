def check_differences_and_upload_maps(df, db_market, db, market):
    diff = df[~df.isin(db_market)].dropna(how="all")
    for i, row in diff.iterrows():
        if row.isna().any():
            ID = db_market[db_market.index == i].iloc[0, 0]
            mapeo = db.session.get(market, ID)
            if row[row.notna()].index[0] == "Mapeo":
                mapeo.Mapeo = row[row.notna()][0]
            else:
                mapeo.Atributo = row[row.notna()][0]
            db.session.commit()
        else:
            mapeo = market(Mapeo=row.Mapeo, Atributo=row.Atributo)
            db.session.add(mapeo)
            db.session.commit()

def check_differences_and_upload_cats(df, db_cat, db, model):
    diff = df[~df.isin(db_cat)].dropna(how="all")
    for i, row in diff.iterrows():
        if row.isna().any():
            ID = db_cat[db_cat.index == i].iloc[0, 0]
            mapeo = db.session.get(model, ID)
            if row[row.notna()].index[0] == "Multivende":
                mapeo.Multivende = row[row.notna()][0]
            elif row[row.notna()].index[0] == "MercadoLibre":
                mapeo.MercadoLibre = row[row.notna()][0]
            elif row[row.notna()].index[0] == "Falabella":
                mapeo.Falabella = row[row.notna()][0]
            elif row[row.notna()].index[0] == "Ripley":
                mapeo.Ripley = row[row.notna()][0]
            elif row[row.notna()].index[0] == "Paris":
                mapeo.Paris = row[row.notna()][0]
            else:
                mapeo.Paris_Familia = row[row.notna()][0]
            db.session.commit()
        else:
            mapeo = model(Multivende=row.Multivende, 
                        MercadoLibre=row.MercadoLibre,
                        Falabella=row.Falabella,
                        Ripley = row.Ripley,
                        Paris = row.Paris,
                        Paris_Familia = row.Paris_Familia)
            db.session.add(mapeo)
            db.session.commit()
            
def check_differences_and_upload_products(df, db_products, db, market):
    diff = df[~df.isin(db_market)].dropna(how="all")
    for i, row in diff.iterrows():
        if row.isna().any():
            ID = db_market[db_market.index == i].iloc[0, 0]
            mapeo = db.session.get(market, ID)
            if row[row.notna()].index[0] == "Mapeo":
                mapeo.Mapeo = row[row.notna()][0]
            else:
                mapeo.Atributo = row[row.notna()][0]
            db.session.commit()
        else:
            mapeo = market(Mapeo=row.Mapeo, Atributo=row.Atributo)
            db.session.add(mapeo)
            db.session.commit()