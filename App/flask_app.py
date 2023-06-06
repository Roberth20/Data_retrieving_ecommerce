from flask import Flask, flash, redirect, request, session, Response, jsonify
from flask import render_template
from werkzeug.utils import secure_filename
from logging.config import dictConfig
from datetime import datetime
import requests
import pandas as pd
import json
import secrets
import io
from funcs import *
import DATA

# Set the logging configurations
# Para manejar temporlmente las notificaciones
dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "default",
            },
            "file":{
                "class":"logging.FileHandler",
                "filename":"notifications.log",
                "formatter": "default"
            }
        },
        "root": {"level": "INFO", "handlers": ["console", "file"]},
    }
)

# Iniciamos la aplicacion
UPLOAD_FOLDER = "mysite"
ALLOWED_EXTENSIONS = {'xlsx'}
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = secrets.token_hex()

@app.route("/")
def index():
    # Funcion base para funcionamiento
    return render_template("index.html")

# Corregir para desabilitar la autenticacion
@app.post("/notifications")
def post_notification():
    app.logger.info("Notification received.")
    app.logger.info(request.form.to_dict())
    return {"message":"Accepted"}, 200

###############################################################################
@app.get("/auth/<code>")
def multi_auth(code):
    """Autenticacion de usuario.

    La aplicacion recibe el token y
    hace la solicitud al correspondiente end-point de multivende.

    En caso de estar correcto, guarda el token, el refreshtoken y
    el tiempo de actualizacion.

    **NOTA**: A falta de una base de datos, se trabaja con una sesion en
    FLASK. ACTUALIZAR A BASE DE DATOS, idealmente."""
    # Definimos url de acceso
    url = "https://app.multivende.com/oauth/access-token"

    # Creamos la informacion a enviar
    payload = {
        "client_id": DATA.CLIENT_ID,
        "client_secret": DATA.CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code
    }
    # Definimos los headers del POST
    headers = {
    'cache-control': 'no-cache',
    'Content-Type': 'application/json'
    }

    # Hacemos la solicitud
    app.logger.info("Solicitando autorizacion.")
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    try:
        # Guardamos la informacion requerida y logueamos
        session["token"] = response.json()["token"]
        app.logger.info("Token obtenido exitosamente")
        session["refreshToken"] = response.json()["refreshToken"]
        app.logger.info("refresh Token obtenido exitosamente")
        session["refreshTime"] = pd.to_datetime(response.json()["updatedAt"])
        app.logger.info("refresh time obtenido exitosamente")
        return {"message":"Token recibido", "data": response.json()}, 200
    except:
        app.logger.error("Error en la autenticacion")
        return render_template("error_token.html")

################################################################################################
@app.get("/refresh")
def refresh_token():
    """Actualizacion de token.

    El token tiene una duracion de 6 horas y requiere actualizacion cada cierto periodo de tiempo,
    a su vez, el RefreshToken tiene una duracion de 48 horas para ser utilizado. Primero se evalua
    si existe el mismo en la sesion (cambiar po base de datos), en caso positivo, confirmamos que
    la hora actual sea la sugerida para su actualizacion o mayor.

    **RECOMENDACION** Cambiar toda esta seccion por un proceso en segundo plano que actualice
    el token sin la necesidad de chequear si es tiempo de refrescar."""
    # Comprobacion de existencia
    if "refreshTime" not in session.keys():
        app.logger.error("No hay refreshTime disponible")
        return "No hay refresh token disponible"

    # Revisamos si es tiempo de ser actualizado
    now = pd.to_datetime(datetime.utcnow(), utc = True)
    diff = session["refreshTime"] - now
    if (diff.seconds / 60) > 60:
        app.logger.info(f"Actualizacion de token muy temprana, faltan {diff.seconds / 60} minutos.")
        return f"Faltan {diff.seconds / 60} minutos para refrescar."

    # Actualizamos toke
    message = r_token(session["refreshToken"])

    return message

def r_token(refreshToken):
    url = "https://app.multivende.com/oauth/access-token"

    #"refresh_token": session["refreshToken"]
    payload = {
        "client_id": DATA.CLIENT_ID,
        "client_secret": DATA.CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refreshToken
    }

    headers = {
      'cache-control': 'no-cache',
      'Content-Type': 'application/json'
    }

    app.logger.info("Solicitando actualizacion de token")
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload))

    try:
        session["token"] = response.json()["token"]
        app.logger.info("Token obtenido exitosamente")
        session["refreshToken"] = response.json()["refreshToken"]
        app.logger.info("refresh Token obtenido exitosamente")
        session["refreshTime"] = pd.to_datetime(response.json()["ExpiresAt"])
        app.logger.info("Token obtenido exitosamente")
        return {"message":"Token recibido", "data": response.json()}, 200
    except:
        app.logger.error("Error en la autenticacion")
        return render_template("error_token.html")


###############################################################################
@app.get("/test")
def test_token():
    """Prueba de conexion, este endpoint se utiliza para confirmar que el token
    se guardo correctamente. No es necesaria para el funcionamiento del sistema."""
    url = "https://app.multivende.com/api/d/info"
    try:
        headers = {
            'Authorization': f'Bearer {session["token"]}'
        }
    except:
        app.logger.error("No hay token disponible")
        return render_template("error_token.html")

    app.logger.info("Realizando prueba de conexion.")
    response = requests.request("GET", url, headers=headers)

    return response.json()

####################################################################
@app.get("/products")
def retrive_data():
    """Generacion de registro de productos.

    Esta seccion se encarga de cargar, procesar y limpiar todos los productos con
    sus respectivos atributos. El resultado final se almacena en un excel (Temporalmente)
    para luego, con el endpoint 'download', descargar la planilla excel con resaltado
    de todos lo atributos vacios correspondientes a un producto."""
    url = f"https://app.multivende.com/api/m/{DATA.MERCHANT_ID}/all-product-attributes"
    try:
        headers = {
            'Authorization': f'Bearer {session["token"]}'
                }
    except:
        app.logger.info("No hay token disponible")
        return render_template("error_token.html")

    app.logger.info("Solicitando atributos")
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
    url = f"https://app.multivende.com/api/m/{DATA.MERCHANT_ID}/products/light/p/1"
    response = requests.request("GET", url, headers=headers).json()
    data = response["entries"]
    pages = response["pagination"]["total_pages"]
    # Los productos se organizan en paginas, pasamos por todas, guardando los resultados
    for p in range(pages-1):
        url = f"https://app.multivende.com/api/m/{DATA.MERCHANT_ID}/products/light/p/{p+2}"
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

    # Limpiamos columnas duplicadas y guardamos el documento
    df.drop(columns = df.columns[df.columns.duplicated()], inplace =True)
    df.to_excel("mysite/output.xlsx", index=False)

    return render_template("file_ready.html")

###################################################################
@app.get("/download")
def download_file():
    """Ajuste visual del excel con la informacion.

    Se procede a cargar los documentos que contienen los atributos originales por categorias
    de cada marketplace, y el mapeo de los mismos con los atributos creados y/o disponibles en
    Multivende.

    **NOTA** Se recomienda tener toda esta informacion en una base de datos, facilita la accesibilidad
    y mejora el rendimiento de la aplicacion."""
    # Cargamos el documento principal
    app.logger.info("Cargando datos para descarga.")
    df = pd.read_excel("mysite/output.xlsx")

    # Cargamos los mapeos
    map_att =  {"RP": pd.read_excel("mysite/Mapeo_atributos_Ripley.xlsx"), "PR": pd.read_excel("mysite/Mapeo_atributos_Paris.xlsx"),
                "MLC": pd.read_excel("mysite/Mapeo_atributos_MLC.xlsx"), "FL": pd.read_excel("mysite/Mapeo_atributos_Falabella.xlsx")}

    # Cargamos los atributos de cada marketplace
    atts = {"MLC": pd.read_excel("mysite/atributos_mercadolibre.xlsx")[["name", "values", "category"]],
            "FL": pd.read_excel("mysite/atributos_falabella.xlsx")[["Label", "Options", "Category"]],
            "PR": pd.read_excel("mysite/atributos_Paris.xlsx")[["name", "family"]],
            "RP": pd.read_excel("mysite/atributos_ripley.xlsx")[["label", "category"]]}

    # Para equivalencia entre multivende y el marketplace, construimos tabla de traduccion
    # de atributos estandar
    std_transformation = pd.DataFrame({
        "Original": df.columns[:21],
        "Nuevo": ["Temporada", "Modelo", "Descripción", "Descripción html", "Descripción corta",
                  "Descripción corta html", "Garantía", "Marca", "Nombre", "Categoría de producto",
                 "Nombre Sku", "Color", "Tamaño", "SKU", "SKU interno", "Ancho", "Largo",
                 "Alto", "Peso", "IDENTIFICADOR_PADRE", "IDENTIFICADOR_HIJO"]
    })
    std_transformation.loc[len(std_transformation), :] = ["size", "Talla"]

    # Cargamos el mapeo de categorias entre el marketplace y multivende
    maps = pd.read_excel("mysite/Master de Categorias_AMano.xlsx")

    # Definimos la funcion que dara color a cada columna
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

    app.logger.info("Preparando excel para su envio.")
    # Preparamos el buffer
    buffer = io.BytesIO()
    # Generamos la tabla de datos con los colores y filtros adecuados
    #
    # La funcion missing_info dedicada a la logica del color puede sser consultada
    # en el documento func.py
    df.style.applymap_index(col_color, axis=1)\
            .apply(missing_info, maps=maps, atts = atts, map_att=map_att, std_transformation=std_transformation, axis=1)\
            .to_excel(buffer, index=False)

    headers = {
        'Content-Disposition': 'attachment; filename=output.xlsx',
        'Content-type': 'application/vnd.ms-excel'
    }
    return Response(buffer.getvalue(), mimetype='application/vnd.ms-excel', headers=headers)

###################################################################
@app.get("/refresh_maps")
def update_att_map():
    """Funcion para generar el mapeo de atributos entre multivende y los marktplace.
    Presenta su mayor utilidad para recuperar los mapeos en caso de perdida.

    En caso de ejecutar la misma con mapeos modificados, se borrara dicha informacion al
    actualizasce.
    """
    # Importamos stopwords
    app.logger.info("Importando stopwords")
    try:
        from nltk.corpus import stopwords
    except:
        app.logger.warning("Stopwords no esta disponible. Intentando descargar.")
        # En caso que no este disponible, se descargara
        import nltk
        nltk.download("stopwords")

        app.logger.info("Descarga completa. Importando stopwords")

        from nltk.corpus import stopwords

    # Cargamos toda la informacion necesaria
    app.logger.info("Loading data to create maps")
    sw = stopwords.words(fileids = "spanish")
    df = pd.read_excel("mysite/output.xlsx")
    mlc = pd.read_excel("mysite/atributos_mercadolibre.xlsx")
    fl = pd.read_excel("mysite/atributos_falabella.xlsx")
    pr = pd.read_excel("mysite/atributos_Paris.xlsx")
    rp = pd.read_excel("mysite/atributos_ripley.xlsx")

    # Separamos el dataframe principal para facilitar el trabajo
    std_col = df.columns[:20]
    ml_col = [c[:-29].lower() for c in df.columns if "Mercado Libre" in c]
    fb_col = [c[:-20] for c in df.columns if "Falabella" in c]
    rp_col = [c[:-17] for c in df.columns if "Ripley" in c]
    p_col = [c[:-16] for c in df.columns if "Paris" in c]

    # Definimos la tabla de traduccion
    std_transformation = pd.DataFrame({
        "Original": std_col,
        "Nuevo": ["Temporada", "Modelo", "Descripción", "Descripción html", "Descripción corta",
                  "Descripción corta html", "Garantía", "Marca", "Nombre", "Categoría de producto",
                 "Nombre Sku", "Color", "Tamaño", "SKU", "SKU interno", "Ancho", "Largo",
                 "Alto", "Peso", "id"]
    })
    std_transformation.loc[len(std_transformation), :] = ["size", "Talla"]

    # Ejecutamos la funcion de mapeo para cada marketplace
    mp = mapeo_atributos(limpieza_de_atributos(ml_col) + std_transformation.Nuevo.tolist(), mlc.name.unique(), sw)
    # Guardamos los datos
    mp.to_excel("mysite/Mapeo_atributos_MLC.xlsx", index=False)

    fb = mapeo_atributos(fb_col + std_transformation.Nuevo.tolist(), fl.Label.unique(), sw)
    fb.to_excel("mysite/Mapeo_atributos_Falabella.xlsx", index = False)

    p = mapeo_atributos(limpieza_de_atributos(p_col) + std_transformation.Nuevo.tolist(), pr.name.unique(), sw)
    p.to_excel("mysite/Mapeo_atributos_Paris.xlsx", index=False)

    rip = mapeo_atributos(limpieza_de_atributos(rp_col) + std_transformation.Nuevo.tolist(), rp.label.unique(), sw)
    rip.to_excel("mysite/Mapeo_atributos_Ripley.xlsx", index=False)

    return render_template("updated_file.html")

###################################################################
@app.get("/refresh_maps/download")
def download_maps():
    """Seccion para descargar los mapeos de atributos"""
    # Importamos herramientas
    import os
    import zipfile
    from flask import send_file

    app.logger.info("Making zip file.")
    # Creamos el documento zip
    zipfolder = zipfile.ZipFile("mysite/datos.zip", "w", compression = zipfile.ZIP_STORED)

    app.logger.info("Writing file.")
    # Llenamos el comprimido con los archivos de mapeos
    for root, dirs, files in os.walk("mysite/"):
        for file in files:
            if "Mapeo" in file or "Master" in file:
                zipfolder.write('mysite/'+file)
    zipfolder.close()

    # Sub-seccion particular para eliminar el zip del servidor
    # despues de ser enviado
    @app.after_this_request
    def remove(response):
        app.logger.info("Deleting zip file")
        os.remove("mysite/datos.zip")
        return response

    app.logger.info("Sending compressed file")
    return send_file('datos.zip',
            mimetype = 'zip',
            attachment_filename= 'datos.zip',
            as_attachment = True)

##################################################################################
def allowed_file(filename):
    """Funcion de ayuda para comprobar la extension del documento subido al servidor"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/refresh_maps/upload', methods=['GET', 'POST'])
def upload_file():
    """Funcion para la subida y actualizacion de los excel que mapean los atributos
    entre multivende y los marketplace."""
    # Importamos el paquete requerido
    import os
    # Revisamos si se envio un post
    if request.method == 'POST':
        # Revisamos si el post contiene un archivo
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # En caso de no subir un documento se reenvia la solicitud
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        # Si el archivo es de la extension deseada
        if file and allowed_file(file.filename):
            # Confirmamos que el archivo sea seguro para ser guardado
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            return render_template("successful_upload.html")

    return render_template("upload_file.html")
############################################################################

if __name__ == "__main__":
    app.run(debug=False, port=8080)