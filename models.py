# For more details, see
# http://docs.sqlalchemy.org/en/latest/orm/tutorial.html#declare-a-mapping
from anthill.framework.db import db, ma
from anthill.framework.utils import timezone
from anthill.framework.utils.asynchronous import as_future
from anthill.framework.utils.translation import translate as _
from anthill.platform.api.internal import InternalAPIMixin
from anthill.platform.core.celery import app as celery_app
from anthill.platform.core.celery.beatsqlalchemy.models import PeriodicTask, CrontabSchedule
from sqlalchemy_utils.types import JSONType, UUIDType, ChoiceType
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.event import listens_for
from sqlalchemy.types import TypeDecorator, VARCHAR
from celery.worker.control import revoke
from tornado.ioloop import IOLoop
from typing import Optional
from datetime import timedelta
import enum
import logging
import json
import re
import random


logger = logging.getLogger('anthill.application')


EVENT_PARTICIPATION_STATUS_CHANGED = 'EVENT_PARTICIPATION_STATUS_CHANGED'


@enum.unique
class EventStatus(enum.Enum):
    STARTED = 0
    FINISHED = 1


class EventCategory(db.Model):
    __tablename__ = 'event_categories'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(128), nullable=False)
    description = db.Column(db.String(512), nullable=False)
    payload = db.Column(JSONType, nullable=False, default={})
    events = db.relationship('Event', backref='category', lazy='dynamic')
    generators = db.relationship('EventGenerator', backref='category', lazy='dynamic')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.Schema = self.get_schema_class()

    @classmethod
    def get_schema_class(cls):
        class _Schema(ma.Schema):
            class Meta:
                model = cls
                fields = ('id', 'name', 'description', 'payload')

        return _Schema


class Event(db.Model):
    __tablename__ = 'events'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(128), nullable=False, unique=True)
    category_id = db.Column(db.Integer, db.ForeignKey('event_categories.id'))
    generator_id = db.Column(db.Integer, db.ForeignKey('event_generators.id'))
    created_at = db.Column(db.DateTime, nullable=False, default=timezone.now)
    start_at = db.Column(db.DateTime, nullable=False)
    finish_at = db.Column(db.DateTime, nullable=False)
    payload = db.Column(JSONType, nullable=False, default={})
    is_active = db.Column(db.Boolean, nullable=False, default=True)

    on_start_task_id = db.Column(UUIDType(binary=False))
    on_finish_task_id = db.Column(UUIDType(binary=False))

    participations = db.relationship('EventParticipation', backref='event', lazy='dynamic')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.Schema = self.get_schema_class()

    @classmethod
    def get_schema_class(cls):
        class _Schema(ma.Schema):
            class Meta:
                model = cls
                fields = ('id', 'start_at', 'finish_at', 'payload', 'category')

        return _Schema

    def dumps(self) -> dict:
        return self.Schema().dump(self).data

    @hybrid_property
    def active(self) -> bool:
        return self.finish_at > timezone.now() >= self.on_start() and self.is_active

    @hybrid_property
    def started(self) -> bool:
        return self.start_at >= timezone.now()

    @hybrid_property
    def finished(self) -> bool:
        return self.finish_at < timezone.now()

    @hybrid_property
    def start_in(self) -> Optional[timedelta]:
        if self.start_at >= timezone.now():
            return self.start_at - timezone.now()

    @hybrid_property
    def finish_in(self) -> Optional[timedelta]:
        if self.finish_at >= timezone.now():
            return self.finish_at - timezone.now()

    async def on_start(self) -> None:
        # TODO: bulk get_users request
        for p in self.participations:
            user = await p.get_user()
            msg = {
                'type': EventStatus.STARTED.name,
                'data': self.dumps()
            }
            await user.send_message(message=json.dumps(msg),
                                    content_type='application/json')

    async def on_finish(self) -> None:
        # TODO: bulk get_users request
        for p in self.participations:
            user = await p.get_user()
            msg = {
                'type': EventStatus.FINISHED.name,
                'data': self.dumps()
            }
            await user.send_message(message=json.dumps(msg),
                                    content_type='application/json')

    @as_future
    def join(self, user_id: str) -> None:
        EventParticipation.create(user_id=user_id, event_id=self.id, status='joined')

    @as_future
    def leave(self, user_id: str) -> None:
        kwargs = dict(user_id=user_id, event_id=self.id, status='joined')
        p = EventParticipation.query.filter_by(**kwargs).first()
        if p is not None:
            p.status = 'leaved'
            p.save()
        else:
            logger.warning('User (%s) is not joined to event (%s), '
                           'so cannot leave.' % (user_id, self.id))


@listens_for(Event, 'before_insert')
def on_event_create(mapper, connection, target):
    from event import tasks

    if not target.is_active:
        return

    if target.start_in:
        task = tasks.on_event_start.apply_async(
            (target.id,), countdown=target.start_in.seconds)
        target.on_start_task_id = task.id

    if target.finish_in:
        task = tasks.on_event_finish.apply_async(
            (target.id,), countdown=target.finish_in.seconds)
        target.on_finish_task_id = task.id


@listens_for(Event, 'before_update')
def on_event_update(mapper, connection, target):
    from event import tasks

    if target.on_start_task_id:
        revoke(celery_app, target.on_start_task_id)
    if target.on_finish_task_id:
        revoke(celery_app, target.on_finish_task_id)

    if not target.is_active:
        return

    if target.start_in:
        task = tasks.on_event_start.apply_async(
            (target.id,), countdown=target.start_in.seconds)
        target.on_start_task_id = task.id

    if target.finish_in:
        task = tasks.on_event_finish.apply_async(
            (target.id,), countdown=target.finish_in.seconds)
        target.on_finish_task_id = task.id


@listens_for(Event, 'after_delete')
def on_event_delete(mapper, connection, target):
    if target.on_start_task_id:
        revoke(celery_app, target.on_start_task_id)
    if target.on_finish_task_id:
        revoke(celery_app, target.on_finish_task_id)


class EventParticipation(InternalAPIMixin, db.Model):
    __tablename__ = 'event_participations'
    __table_args__ = (
        db.UniqueConstraint('user_id', 'event_id'),
    )

    STATUSES = (
        ('joined', _('Joined')),
        ('leaved', _('Leaved'))
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    created_at = db.Column(db.DateTime, nullable=False, default=timezone.now)
    status = db.Column(ChoiceType(STATUSES))
    payload = db.Column(JSONType, nullable=False, default={})
    user_id = db.Column(db.Integer, nullable=False)
    event_id = db.Column(db.Integer, db.ForeignKey('events.id'), nullable=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.Schema = self.get_schema_class()

    @classmethod
    def get_schema_class(cls):
        class _Schema(ma.Schema):
            class Meta:
                model = cls
                fields = ('payload', 'created_at', 'status', 'event')

        return _Schema

    def dumps(self) -> dict:
        return self.Schema().dump(self).data

    async def on_status_changed(self) -> None:
        user = await self.get_user()
        msg = {
            'type': EVENT_PARTICIPATION_STATUS_CHANGED,
            'data': self.dumps()
        }
        await user.send_message(message=json.dumps(msg),
                                content_type='application/json')

    async def get_user(self):
        return await self.internal_request('login', 'get_user', user_id=self.user_id)


@listens_for(EventParticipation.status, 'set', active_history=True)
def on_event_participation_status_changed(target, value, oldvalue, initiator):
    if value != oldvalue:
        IOLoop.current().add_callback(target.on_status_changed)


class CrontabType(TypeDecorator):
    impl = VARCHAR(128)
    keys = ('minute', 'hour', 'day_of_week', 'day_of_month', 'month_of_year')

    def process_literal_param(self, value, dialect):
        pass

    def process_bind_param(self, value, dialect):
        if value is not None:
            values = re.split(r'\s+', value)
            if len(values) != 5:
                raise ValueError('Illegal crontab field value: %s' % value)
            return dict(zip(self.keys, values))

    def process_result_value(self, value, dialect):
        if value is not None:
            values = list(value.values())
            if len(values) != 5:
                raise ValueError('Illegal crontab field value: %s' % value)
            return ' '.join(values)

    @property
    def python_type(self):
        return self.impl.type.python_type


class EventGenerator(db.Model):
    __tablename__ = 'event_generators'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(128), nullable=False, unique=True)
    pool_id = db.Column(db.Integer, db.ForeignKey('event_generator_pools.id'))
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    enabled = db.Column(db.Boolean, nullable=False, default=True)
    last_run_at = db.Column(db.DateTime)
    total_run_count = db.Column(db.Integer, nullable=False, default=0)
    plan = db.Column(CrontabType)

    # Event parameters
    category_id = db.Column(db.Integer, db.ForeignKey('event_categories.id'))
    start_at = db.Column(db.DateTime, nullable=False)
    finish_at = db.Column(db.DateTime, nullable=False)
    payload = db.Column(JSONType, nullable=False, default={})

    events = db.relationship('Event', backref='generator', lazy='dynamic')
    task_id = db.Column(db.Integer, db.ForeignKey('periodic_task.id'))
    task = db.relationship('PeriodicTask')

    @as_future
    def run(self, is_active=True) -> Event:
        """Generates new event."""
        self.last_run_at = timezone.now()
        self.total_run_count += 1
        self.save()
        kwargs = {
            'name': None,  # TODO: auto
            'category_id': self.category_id,
            'start_at': self.start_at,
            'finish_at': self.finish_at,
            'payload': self.payload,
            'is_active': is_active,
            'generator_id': self.id,
        }
        return Event.create(**kwargs)

    @as_future
    def task_create(self):
        schedule = CrontabSchedule.get_or_create(**self.plan)
        task = PeriodicTask.create(crontab=schedule,
                                   name=_('Start events generator'),
                                   task='event.tasks.events_generator_run',
                                   args=json.dumps([self.id]),
                                   enabled=self.active)
        self.task_id = task.id
        # self.save()

    @as_future
    def task_disable(self):
        self.task.enabled = False
        self.task.save()

    @as_future
    def task_enable(self):
        self.task.enabled = True
        self.task.save()

    @as_future
    def task_delete(self):
        self.task.delete()

    @as_future
    def task_update(self):
        schedule = CrontabSchedule.get_or_create(**self.plan)
        self.task.crontab = schedule
        self.task.enabled = self.active
        self.task.save()

    @hybrid_property
    def active(self) -> bool:
        if not self.enabled:
            return False
        if self.pool_id is not None:
            return self.pool.is_active
        return self.is_active


@listens_for(EventGenerator, 'before_insert')
def on_event_generator_create(mapper, connection, target):
    IOLoop.current().add_callback(target.task_create)


@listens_for(EventGenerator, 'before_update')
def on_event_generator_update(mapper, connection, target):
    IOLoop.current().add_callback(target.task_update)


@listens_for(EventGenerator, 'after_delete')
def on_event_generator_delete(mapper, connection, target):
    IOLoop.current().add_callback(target.task_delete)


class EventGeneratorPool(db.Model):
    __tablename__ = 'event_generator_pools'

    RUN_SCHEMES = (
        ('all', _('All')),
        ('any', _('Any')),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(128), nullable=False, unique=True)
    description = db.Column(db.String(512), nullable=False)
    generators = db.relationship('EventGenerator', backref='pool', lazy='dynamic')
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    run_scheme = db.Column(ChoiceType(RUN_SCHEMES), default='any')
    last_run_at = db.Column(db.DateTime)
    total_run_count = db.Column(db.Integer, nullable=False, default=0)
    plan = db.Column(CrontabType)

    task_id = db.Column(db.Integer, db.ForeignKey('periodic_task.id'))
    task = db.relationship('PeriodicTask')

    @as_future
    def run(self):
        enabled_generators = self.generators.query.filter_by(enabled=True).all()
        if self.run_scheme is 'any':
            prepared_generators = [random.choice(enabled_generators)]
        elif self.run_scheme is 'all':
            prepared_generators = enabled_generators
        else:
            prepared_generators = []

        for gen in enabled_generators:
            gen.is_active = False
            gen.save()

        for gen in prepared_generators:
            gen.is_active = True
            gen.save()

    @as_future
    def task_create(self):
        schedule = CrontabSchedule.get_or_create(**self.plan)
        task = PeriodicTask.create(crontab=schedule,
                                   name=_('Start events generators pool'),
                                   task='event.tasks.events_generators_pool_run',
                                   args=json.dumps([self.id]),
                                   enabled=self.active)
        self.task_id = task.id
        # self.save()

    @as_future
    def task_disable(self):
        self.task.enabled = False
        self.task.save()

    @as_future
    def task_enable(self):
        self.task.enabled = True
        self.task.save()

    @as_future
    def task_update(self):
        schedule = CrontabSchedule.get_or_create(**self.plan)
        self.task.crontab = schedule
        self.task.enabled = self.active
        self.task.save()

    @as_future
    def task_delete(self):
        self.task.delete()

    @hybrid_property
    def active(self) -> bool:
        return self.is_active


@listens_for(EventGeneratorPool, 'before_insert')
def on_event_generator_pool_create(mapper, connection, target):
    IOLoop.current().add_callback(target.task_create)


@listens_for(EventGeneratorPool, 'before_update')
def on_event_generator_pool_update(mapper, connection, target):
    IOLoop.current().add_callback(target.task_update)


@listens_for(EventGeneratorPool, 'after_delete')
def on_event_generator_pool_delete(mapper, connection, target):
    IOLoop.current().add_callback(target.task_delete)
