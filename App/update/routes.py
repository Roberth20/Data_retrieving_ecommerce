from App.update import update
import pandas as pd
from flask import render_template, request, redirect
from App.models.mapeo_atributos import *
from App.extensions.db import db
from App.update.funcs import *
from App.models.mapeo_categorias import Mapeo_categorias
from flask_security import auth_required
from App.models.auth import auth_app
from App.auth.funcs import decrypt
from flask import current_app
from App.models.productos import get_products
from App.get_data.Populate_tables import upload_data_products

ALLOWED_EXTENSIONS = ["xlsx"]

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@update.get("/")
@auth_required("basic")
def update_main():
    return render_template("update/main.html")

@update.route("/paris", methods=["GET", "POST"])
@auth_required("basic")
def update_paris():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            df = pd.read_excel(file)
            db_paris = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_Paris.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            if ~df.columns.isin(db_paris.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload_maps(df, db_paris, db, Mapeo_Paris)
            
            return render_template("update/success.html", market = "Paris")
    
    return render_template("update/sample.html", market="Paris")

@update.route("/falabella", methods=["GET", "POST"])
@auth_required("basic")
def update_falabella():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            df = pd.read_excel(file)
            db_falabella = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_Falabella.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            if ~df.columns.isin(db_falabella.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload_maps(df, db_falabella, db, Mapeo_Falabella)
            
            return render_template("update/success.html", market = "Falabella")
    
    return render_template("update/sample.html", market="Falabella")

@update.route("/mercadolibre", methods=["GET", "POST"])
@auth_required("basic")
def update_mercadolibre():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            df = pd.read_excel(file)
            db_mlc = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_MercadoLibre.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            if ~df.columns.isin(db_mlc.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload_maps(df, db_mlc, db, Mapeo_MercadoLibre)
            
            return render_template("update/success.html", market = "Mercado Libre")
    
    return render_template("update/sample.html", market="Mercado Libre")

@update.route("/ripley", methods=["GET", "POST"])
@auth_required("basic")
def update_ripley():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            df = pd.read_excel(file)
            db_ripley = pd.DataFrame([[m.id, m.Mapeo, m.Atributo] for m in Mapeo_Ripley.query.all()], 
                      columns=["Id","Mapeo", "Atributo"])
            if ~df.columns.isin(db_ripley.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload_maps(df, db_ripley, db, Mapeo_Ripley)
            
            return render_template("update/success.html", market = "Ripley")
    
    return render_template("update/sample.html", market="Ripley")

@update.route("/map_cat", methods=["GET", "POST"])
@auth_required("basic")
def update_mapcat():
    if request.method == "POST":
        # check if the post request has the file part
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            return redirect(request.url)
        if file and allowed_file(file.filename):
            df = pd.read_excel(file)
            db_cat = pd.DataFrame([[m.id, m.Multivende, m.MercadoLibre, m.Falabella,
                       m.Ripley, m.Paris, m.Paris_Familia] for m in Mapeo_categorias.query.all()], 
                      columns=["Id","Multivende", "MercadoLibre", "Falabella", "Ripley",
                              "Paris", "Paris_Familia"])
            if ~df.columns.isin(db_cat.columns).all():
                return render_template("update/error.html")
            
            check_differences_and_upload_cats(df, db_cat, db, Mapeo_categorias)
            
            return render_template("update/success.html", market = "Mapeo categorias")
    
    return render_template("update/sample.html", market="Mapeo categorias")

@update.get("/products")
@auth_required("basic")
def update_products():
    url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/all-product-attributes"
    last_auth = db.session.scalars(db.select(auth_app).order_by(auth_app.expire.desc())).first()
    diff = datetime.utcnow() - last_auth.expire
    if diff.total_seconds()/3600 > 6:
        return render_template("update/token_error.html")
    
    token = decrypt(last_auth.token, current_app.config["SECRET_KEY"])
    headers = {
            'Authorization': f'Bearer {token}'
    }
    response = requests.request("GET", url, headers=headers).json()
    # Obtenemos dos grupos de atributos, lo separamos
    att = response["customAttributes"]
    att_std = ["Season", "model", "description", "htmlDescription", "shortDescription",
                "htmlShortDescription", "Warranty", "Brand", "name", "ProductCategory", "sku_name", "color",
                "size", "sku", "internalSku", "width", "length", "height", "weight", "IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO"]
    # Transformamos los nombres para mayor comodidad
    att_names = [item["name"]+"-"+item["CustomAttributeSet.name"] for item in att]
    att_short_names = [item["name"] for item in att]

    # Obtenemos la lista de todos los productos.
    app.logger.info("Solicitando productos")
    url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/products/light/p/1"
    response = requests.request("GET", url, headers=headers).json()
    data = response["entries"]
    pages = response["pagination"]["total_pages"]
    # Los productos se organizan en paginas, pasamos por todas, guardando los resultados
    for p in range(pages-1):
        url = f"https://app.multivende.com/api/m/{current_app.config['MERCHANT_ID']}/products/light/p/{p+2}"
        response = requests.request("GET", url, headers=headers).json()
        data += response["entries"]

    # Extraemos los id de cada uno
    ids = [item["_id"] for item in data]

    # Para cada producto, guardamos sus atributos
    app.logger.info("Solicitando atributos de productos.")
    data = []
    for i in ids:
        url = f"https://app.multivende.com/api/products/{i}?_include_product_picture=false"
        response = requests.request("GET", url, headers=headers).json()
        data.append(response)

    # Dentro de los atributos normales, extraemos los atributos hechos por el usuario
    app.logger.info("Procesando atributos personales")
    customs = []
    for d in data:
        tmp_dict = {}
        for at in d["CustomAttributeValues"]:
            tmp_dict[at["CustomAttribute"]["name"]] =  at["text"]

        tmp_dict["tags"] = []
        for tag in d["ProductTags"]:
            tmp_dict["tags"].append(tag["Tag"]["name"])

        customs.append(tmp_dict)
        d.pop("CustomAttributeValues")

    # Unimos cada conjunto de atributos con sus custom atributos
    info = []
    for i in range(len(data)):
        info.append(data[i] | customs[i])

    # Hay atributos estandar que tienen informacion anidada, extraemos la misma
    app.logger.info("Procesando atributos estandar")
    all_data = []
    for i in info:
        sku = i["code"]
        i["sku_name"] = sku
        i["IDENTIFICADOR_PADRE"] = i["_id"]
        # Cuando el atributo no tiene valor, no posee informacion anidada
        # evitamos errores tomando este aspecto en consideracion
        try:
            brand = i["Brand"]["name"]
        except:
            brand = None
        i["Brand"] = brand
        try:
            cat = i["ProductCategory"]["name"]
        except:
            cat = None
        i["ProductCategory"] = cat
        try:
            war = i["Warranty"]["name"]
        except:
            war = None
        i["Warranty"] = war
        # Extraemos la misma informacion para cada version de producto
        for pv in i["ProductVersions"]:
            j = i.copy()
            tmp_dict = {}
            for at in pv["CustomAttributeValues"]:
                tmp_dict[at["CustomAttribute"]["name"]] =  at["text"]
            pv.pop("CustomAttributeValues")
            j = j | tmp_dict
            j["color"] = pv["Color"]["name"]
            j["size"] = pv["Size"]["name"]
            j["sku"] = pv["code"]
            j["internalSku"] = pv["internalCode"]
            j["height"] = pv["height"]
            j["length"] = pv["length"]
            j["weight"] = pv["weight"]
            j["width"] = pv["width"]
            j["IDENTIFICADOR_HIJO"] = pv["_id"]
            j.pop("ProductVersions")
            all_data.append(j)

    # Generamos la tabla de datos
    df = pd.DataFrame(all_data, columns = att_std + att_short_names)
    df.columns = att_std + att_names

    # Limpiamos atributos de informacion que no es relavante o complementaria
    # de la base de datos de multivende
    markets = []
    for market in ["HB", "Falabella", "Ripley", "Paris", "Shopify"]:
        for c in df.columns:
            if market in c:
                markets.append(c)

    basics = [d for d in df.columns if d not in markets]
    df = df[basics+markets]

    # Limpiamos columnas duplicadas
    df.drop(columns = df.columns[df.columns.duplicated()], inplace =True)
    data = get_products()
    diff = df[~df.isin(data)].dropna(how="all")
    if diff.shape[0] == 0:
        return render_template("update/products.html")
        
    stmt = db.text("DELETE FROM Productos_standard")
    db.session.execute(stmt)
    stmt = db.text("DELETE FROM Productos_MercadoLibre")
    db.session.execute(stmt)
    stmt = db.text("DELETE FROM Productos_Paris")
    db.session.execute(stmt)
    stmt = db.text("DELETE FROM Productos_Falabella")
    db.session.execute(stmt)
    stmt = db.text("DELETE FROM Productos_Ripley")
    db.session.execute(stmt)
    stmt = db.text("DELETE FROM Renombre_categorias")
    db.session.execute(stmt)
    db.session.commit()
    
    message = upload_data_products(df, db)
    return render_template("update/products_updated", message=message)