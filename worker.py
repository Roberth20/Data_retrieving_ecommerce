from App import create_app, configure_celery

app = create_app(test_config=True)
celery = configure_celery(app)