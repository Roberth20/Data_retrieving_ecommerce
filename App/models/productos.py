from App.extensions.db import db
import pandas as pd

def get_products():
    with db.engine.connect() as connection:
        stmt = db.text("SELECT * FROM Productos_standard")
        result = connection.execute(stmt)
        Ps = pd.DataFrame(result.fetchall(), columns = result.keys())
        Ps.set_index(["IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO"], inplace=True)

        stmt = db.text("SELECT * FROM Productos_MercadoLibre")
        result = connection.execute(stmt)
        Pm = pd.DataFrame(result.fetchall(), columns = result.keys())
        Pm.set_index(["IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO"], inplace=True)

        stmt = db.text("SELECT * FROM Productos_Paris")
        result = connection.execute(stmt)
        Pp = pd.DataFrame(result.fetchall(), columns = result.keys())
        Pp.set_index(["IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO"], inplace=True)

        stmt = db.text("SELECT * FROM Productos_Falabella")
        result = connection.execute(stmt)
        Pf = pd.DataFrame(result.fetchall(), columns = result.keys())
        Pf.set_index(["IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO"], inplace=True)

        stmt = db.text("SELECT * FROM Productos_Ripley")
        result = connection.execute(stmt)
        Pr = pd.DataFrame(result.fetchall(), columns = result.keys())
        Pr.set_index(["IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO"], inplace=True)

        stmt = db.text("SELECT * FROM Renombre_categorias")
        result = connection.execute(stmt)
        R_MLC = pd.DataFrame(result.fetchall(), columns = result.keys())

    productos = Ps.join([Pm, Pp, Pf, Pr], how="outer")
    cols = productos.columns.to_list()
    for i, c in enumerate(cols):
        if any(R_MLC.Nuevo == c):
            cols[i] = R_MLC.Viejo[R_MLC.Nuevo == c].values[0]

    productos.columns = cols
    return productos