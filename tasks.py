"""
Example:

    @app.task(ignore_result=True)
    def your_task():
        ...
"""

from anthill.platform.core.celery import app
from anthill.framework.utils.asynchronous import as_future
from tornado.ioloop import IOLoop


@as_future
def _event_on_start(event_id):
    from event.models import Event
    event = Event.query.get(event_id)
    if event is not None:
        event.on_start()


@as_future
def _event_on_finish(event_id):
    from event.models import Event
    event = Event.query.get(event_id)
    if event is not None:
        event.on_finish()


@as_future
def _events_generator_run(id_):
    from event.models import EventGenerator
    obj = EventGenerator.query.get(id_)
    if obj is not None:
        obj.run()


@as_future
def _events_generators_pool_run(id_):
    from event.models import EventGeneratorPool
    obj = EventGeneratorPool.query.get(id_)
    if obj is not None:
        obj.run()


@app.task(ignore_result=True)
def on_event_start(event_id):
    """
    Run this callback when event started.
    """
    IOLoop.current().add_callback(_event_on_start, event_id)


@app.task(ignore_result=True)
def on_event_finish(event_id):
    """
    Run this callback when event finished.
    """
    IOLoop.current().add_callback(_event_on_finish, event_id)


@app.task(ignore_result=True)
def events_generator_run(id_):
    """
    Periodically generates new event according to generator settings.
    """
    IOLoop.current().add_callback(_events_generator_run, id_)


@app.task(ignore_result=True)
def events_generators_pool_run(id_):
    """
    Periodically activates generators according to generator pool settings.
    """
    IOLoop.current().add_callback(_events_generators_pool_run, id_)
