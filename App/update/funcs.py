

def check_differences_and_upload(df, db_market, db, market):
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
