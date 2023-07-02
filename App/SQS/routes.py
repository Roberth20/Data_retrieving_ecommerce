from App.SQS import sqs
from App.task.long_task import celery_long_task

@sqs.route('/celery/<int:duration>', methods=['GET'])
def add_celery_task(duration):
    celery_long_task.delay(duration)
    return 'Task queued'
