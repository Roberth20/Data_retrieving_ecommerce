from celery import Celery

celery = Celery(__name__, include=["App.task"])