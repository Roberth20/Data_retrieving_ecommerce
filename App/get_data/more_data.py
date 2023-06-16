import requests
import json

url = "https://app.multivende.com/api/m/aa2477a7-1df7-466c-92b8-4e823ae12c08/warranties"
headers = {
    'Authorization': f'Bearer {token}'
}
warranties = requests.request("GET", url, headers=headers).json()

url = "https://app.multivende.com/api/m/aa2477a7-1df7-466c-92b8-4e823ae12c08/tags/p/1"
tags = requests.request("GET", url, headers=headers).json()

url = "https://app.multivende.com/api/m/aa2477a7-1df7-466c-92b8-4e823ae12c08/official-stores"
stores = requests.request("GET", url, headers=headers).json()

url = "https://app.multivende.com/api/m/aa2477a7-1df7-466c-92b8-4e823ae12c08/shipping-classes/p/1"
shipping = requests.request("GET", url, headers=headers).json()

url = "https://app.multivende.com/api/m/aa2477a7-1df7-466c-92b8-4e823ae12c08/custom-attribute-sets/products"
custom_p = requests.request("GET", url, headers=headers).json()

url = "https://app.multivende.com/api/m/aa2477a7-1df7-466c-92b8-4e823ae12c08/custom-attribute-sets/product_versions"
custom_pv = requests.request("GET", url, headers=headers).json()