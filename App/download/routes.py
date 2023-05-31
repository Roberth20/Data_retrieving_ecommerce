from flask import render_template
from App.download import download

@download.route("/")
def index():
    # TODO: Add an index to select what file to download 
    return "<h1>Under construction</h1>"

@download.route("/products")
def download_products():
    # TODO: Download the product's excel highlithed
    return  "<h1>Under construction</h1>"

@download.route("/maps")
def download_maps():
    # TODO: Download a compressed file with mapping data
    return  "<h1>Under construction</h1>"