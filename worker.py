from App import create_app, configure_celery

app = create_app(test_config=False)
celery = configure_celery(app)