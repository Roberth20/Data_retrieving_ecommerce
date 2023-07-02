from App.extensions.celery import celery
import time
import requests
import pandas as pd
import json
from App.models.checkouts import checkouts, deliverys
from App.extensions.db import db
import numpy as np
from App.update.funcs import check_difference_and_update_checkouts
from App.update.funcs import check_diferences_and_update_deliverys

@celery.task
def celery_long_task(duration):
    for i in range(duration):
        print("Working... {}/{}".format(i + 1, duration))
        time.sleep(2)
        if i == duration - 1:
            print('Completed work on {}'.format(duration))
            
@celery.task
def update_deliverys(token, merchant_id, last, now):
    # Get marketplace connections
    url = f"https://app.multivende.com/api/m/{merchant_id}/marketplace-connections/p/1"
    headers = {
            'Authorization': f'Bearer {token}'
        }
    #current_app.logger.info("Solicitando ids de marketplaces connections")
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        return "Error" + response.text
    
    data=[]
    s = len(response["entries"])
    for i, conn in enumerate(response["entries"]):
        print(f"Working on update deliverys... connection {i}/{s}")
        url = f"https://app.multivende.com/api/m/{merchant_id}/delivery-orders/documents/p/1?_delivery_statuses=completed&_delivery_statuses=pending&_shipping_label_print_statuses=not_printed&_shipping_label_status=ready&include_only_delivery_order_with_traking_number=true&_marketplace_connection_id={conn['_id']}&_updated_at_from={last}&_updated_at_to={now}"
        # Get shipping labels for connections
        try:
            info = requests.get(url, headers=headers).json()
        except:
            print(f"No shipping labels from {requests.get(url, headers=headers).text}")
            #current_app.logger.info(f"No shipping labels from {requests.get(url, headers=headers).text}")
        
        if len(info["entries"]) > 0:
            pages = info["pagination"]["total_pages"]
            for p in range(0, pages):
                print(f"Working on update deliverys... page {p+1}/{pages}")
                url = f"https://app.multivende.com/api/m/{merchant_id}/delivery-orders/documents/p/{p}?_delivery_statuses=completed&_delivery_statuses=pending&_shipping_label_print_statuses=not_printed&_shipping_label_status=ready&include_only_delivery_order_with_traking_number=true&_marketplace_connection_id={conn['_id']}&_updated_at_from={last}&_updated_at_to={now}"
                entries = requests.get(url, headers=headers).json()
                # Store important data
                l = len(entries["entries"])
                for i, entry in enumerate(entries["entries"]):
                    print(f"Working on update deliverys... {i}/{l}")
                    tmp = {}
                    tmp["fecha promesa"] = entry["promisedDeliveryDate"]
                    tmp["direccion"] = entry["deliveryAddress"]
                    tmp["codigo"] = entry["code"]
                    tmp["courier"] = entry["courierName"]
                    tmp["fecha despacho"] = entry["handlingDateLimit"]
                    tmp["delivery status"] = entry["deliveryStatus"]
                    tmp["N seguimiento"] = entry["trackingNumber"]
                    tmp["codigo venta"] = entry["DeliveryOrderInCheckouts"][0]["Checkout"]["code"]
                    tmp["id venta"] = entry["DeliveryOrderInCheckouts"][0]["CheckoutId"]
                    tmp["status etiqueta"] = entry["shippingLabelStatus"]
                    tmp["estado impresion etiqueta"] = entry["shippingLabelPrintStatus"]
                    data.append(tmp)
    
    # Create dataframe and adjust formats
    df = pd.DataFrame(data)
    df.fillna(np.nan, inplace=True)
    df["fecha despacho"] = pd.to_datetime(df["fecha despacho"])
    df["fecha despacho"] = df["fecha despacho"].dt.tz_convert(None)
    df["fecha promesa"] = pd.to_datetime(df["fecha promesa"])
    df["fecha promesa"] = df["fecha promesa"].dt.tz_convert(None)
    
    # Load data from checkouts
    ventas = pd.DataFrame([[v.id_venta, v.n_venta] for v in checkouts.query.all()],
                         columns = ["id", "n venta"])
    # Add n venta to df
    for i in df.index:
        if (ventas["id"] == df.loc[i, "id venta"]).any():
            df.loc[i, "n venta"] = ventas["n venta"][ventas["id"] == df.loc[i, "id venta"]].values[0]
    
    # Fill empty values with None
    df = df.replace({np.NaN: None})
    # Clear duplicated
    df = df.drop_duplicates()
    # Only store the items with n venta (a checkout registered)
    df = df[df["n venta"].notna()]
    
    check_diferences_and_update_deliverys(df, deliverys, db)
    print("Deliverys updated")
            
@celery.task
def update_checkouts(token, merchant_id, last, now):
    url = f"https://app.multivende.com/api/m/{merchant_id}/checkouts/light/p/1?_updated_at_from={last}&_updated_at_to={now}"
    headers = {
            'Authorization': f'Bearer {token}'
    }
    # Get data
    response = requests.request("GET", url, headers=headers)
    try:
        response = response.json()
    except:
        return "Error" + response.text
    
    pages = response["pagination"]["total_pages"]
    ids= []
    # First all ids
    for p in range(0, pages):
        print(f"Working to get ids... {p+1}/{pages}")
        url = f"https://app.multivende.com/api/m/{merchant_id}/checkouts/light/p/{p+1}?_updated_at_from={last}&_updated_at_to={now}"
        data = requests.get(url, headers=headers).json()
        for d in data["entries"]:
            ids.append(d["_id"])
    
    # Now the information completed
    ventas = []
    total_ids = len(ids)
    for i, id in enumerate(ids):
        print(f"Working to get data... {i}/{total_ids}")
        tmp = {}
        url = f"https://app.multivende.com/api/checkouts/{id}"
        checkout = requests.get(url, headers=headers).json()
        tmp["fecha"] = checkout["soldAt"]
        tmp["nombre"] = checkout["Client"]["fullName"]
        tmp["n venta"] = checkout["CheckoutLink"]["externalOrderNumber"] # Numero de orden en marketplace
        tmp["id"] = checkout["CheckoutLink"]["CheckoutId"] # Codigo en multivende
        tmp["estado entrega"] = checkout["deliveryStatus"]
        tmp["costo de envio"] = checkout["DeliveryOrderInCheckouts"][0]["DeliveryOrder"]["cost"]
        tmp["market"] = checkout["origin"]
        tmp["mail"] = checkout["Client"]["email"]
        tmp["phone"] = checkout["Client"]["phoneNumber"]
        # Try to find the billing files
        try:
            url = f"https://app.multivende.com/api/checkouts/{id}/electronic-billing-documents/p/1"
            billing = requests.get(url, headers=headers).json()
            tmp["estado boleta"] = billing["entries"][-1]["ElectronicBillingDocumentFiles"][-1]["synchronizationStatus"]
            tmp["url boleta"] = billing["entries"][-1]["ElectronicBillingDocumentFiles"][-1]["url"]
        except:
            tmp["estado boleta"] = None
            tmp["url boleta"] = None
        
        # Getting all status of ventas
        tmp["estado venta"] = []
        for status in checkout["CheckoutPayments"]:
            tmp["estado venta"].append(status["paymentStatus"])
        # For each item we split the checkout
        for product in checkout["CheckoutItems"]:
            item = tmp.copy()
            item["codigo producto"] = product["code"]
            item["nombre producto"] = product["ProductVersion"]["Product"]["name"]
            item["id padre producto"] = product["ProductVersion"]["ProductId"]
            item["id hijo producto"] = product["ProductVersionId"]
            item["cantidad"] = product["count"]
            item["precio"] = product["gross"]
            ventas.append(item)

    # Load data to be processed
    df = pd.DataFrame(ventas)
    df["fecha"] = pd.to_datetime(df["fecha"])
    df["fecha"] = df["fecha"].dt.tz_convert(None)
    df= df.fillna(np.nan)
    for i in df["estado venta"].index:
        df.loc[i, "estado venta"] = df["estado venta"][i][-1]
        
    df = df.replace({np.NaN: None})
    
    check_difference_and_update_checkouts(df, checkouts, db)
    print("Updated checkouts database")