# Data retrieving ecommerce

Simple app interface and Back-end on Flask to provide tools that simplify commons task in Multivende website.

## Configuration

In the instance folder you can store all *.py* configuration files for differents instances of the App. There lays and example of one.

## First steps

1. Change the configuration file with current values
2. Change the name of the instances in *App/\__init__.py*
3. Run *create_user.py* to create the DataBase and the user
4. Run the app with the instance with  `python wsgi.py`
5. Also, set the workers and scheduler (if required) with `celery -A worker:celery worker -l INFO` and `celery -A worker:celery beat`

## Other configurations

* To download the excel is required to map all attributes and categories between Multivende and each Marketplace, it can be done easily with code and only is need it to do it once. Also, you can *fine-tune* them downloading the Excel.

* The download of the Excel help to highlight the information missing for each product in each marketplace to have a *well defined* product.

# Note

Currently, I am finished the basis of the App. It can download from multivende all the data to a desired DB, you can change all the products downloading an Excel and it can upload all change to Multivende without problems.

I'll be glad to do changes, not related to front-end, if someone suggest them.