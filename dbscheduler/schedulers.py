from __future__ import absolute_import

from multiprocessing.util import Finalize

from celery import current_app
from celery import schedules
from celery.beat import Scheduler, ScheduleEntry
from celery.utils.log import get_logger

from django.db import transaction
from django.core.exceptions import ObjectDoesNotExist

from .models import PeriodicTask
from .utils import DATABASE_ERRORS, make_aware

ADD_ENTRY_ERROR = """\
Couldn't add entry %r to database schedule: %r. Contents: %r
"""

logger = get_logger(__name__)
debug, info, error = logger.debug, logger.info, logger.error

DEFAULT_ENTRIES = {
    'celery.backend_cleanup': {
        'task': 'celery.backend_cleanup',
        'schedule': schedules.crontab('0', '4', '*'),
        'options': {'expires': 12 * 3600},
    },
}


class TaskRemovedException(Exception):
    pass


class ModelEntry(ScheduleEntry):

    def __init__(self, model):
        self.app = current_app._get_current_object()
        CELERYBEAT_SCHEDULE = self.app.conf.CELERYBEAT_SCHEDULE.copy()
        for k, v in DEFAULT_ENTRIES.iteritems():
            CELERYBEAT_SCHEDULE.setdefault(k, v)
        if model.name not in CELERYBEAT_SCHEDULE:
            raise TaskRemovedException
        self.name = model.name

        self.schedule = CELERYBEAT_SCHEDULE[self.name]['schedule']
        self.task = CELERYBEAT_SCHEDULE[self.name]['task']
        self.args = CELERYBEAT_SCHEDULE[self.name].get('args', [])
        self.kwargs = CELERYBEAT_SCHEDULE[self.name].get('kwargs', {})
        self.options = CELERYBEAT_SCHEDULE[self.name].get('options', {})

        self.model = model
        model.last_run_at = model.last_run_at or self._default_now()
        self.last_run_at = model.last_run_at

    def is_due(self):
        return self.schedule.is_due(self.last_run_at)

    def _default_now(self):
        return make_aware(self.app.now())

    def next(self):
        self.model.last_run_at = make_aware(self.app.now())
        return self.__class__(self.model)

    def save(self):
        self.model.save()

    @classmethod
    def from_entry(cls, name):
        ptask, created = PeriodicTask.objects.update_or_create(name=name)
        if created:
            msg = (
                "DatabaseScheduler: "
                "Task %s has been added to CELERYBEAT_SCHEDULE, "
                "creating a db entry"
            )
            info(msg, name)
        self = cls(ptask)
        return self


class DatabaseScheduler(Scheduler):
    Entry = ModelEntry
    Model = PeriodicTask
    _schedule = None
    _initial_read = False

    def __init__(self, *args, **kwargs):
        self._dirty = set()
        self._finalize = Finalize(self, self.sync, exitpriority=5)
        Scheduler.__init__(self, *args, **kwargs)

    def setup_schedule(self):
        self.install_default_entries(None)
        self.update_from_dict(self.app.conf.CELERYBEAT_SCHEDULE)

    def all_as_schedule(self):
        info('DatabaseScheduler: Fetching database schedule')
        tasks = {}
        for model in self.Model.objects.all():
            try:
                tasks[model.name] = self.Entry(model)
            except TaskRemovedException:
                msg = (
                    "DatabaseScheduler: "
                    "Task %s has been deleted from CELERYBEAT_SCHEDULE, "
                    "removing it from db."
                )
                info(msg, model.name)
                model.delete()
        return tasks

    def reserve(self, entry):
        new_entry = super(DatabaseScheduler, self).reserve(entry)
        # Need to store entry by name, because the entry may change
        # in the mean time.
        self._dirty.add(new_entry.name)
        return new_entry

    def sync(self):
        _tried = set()
        if self._dirty:
            info(
                "DatabaseScheduler: Writing tasks to db: %s",
                ','.join(self._dirty)
            )
        try:
            with transaction.atomic():
                while self._dirty:
                    try:
                        name = self._dirty.pop()
                        _tried.add(name)
                        self.schedule[name].save()
                    except (KeyError, ObjectDoesNotExist):
                        pass
        except DATABASE_ERRORS as exc:
            # retry later
            self._dirty |= _tried
            error('Database error while sync: %r', exc, exc_info=1)

    def update_from_dict(self, dict_):
        s = {}
        for name, entry in dict_.items():
            try:
                entry = self.Entry.from_entry(name)
                s[name] = entry
            except Exception as exc:
                error(ADD_ENTRY_ERROR, name, exc, entry)
        self.schedule.update(s)

    def install_default_entries(self, data):
        entries = {}
        if self.app.conf.CELERY_TASK_RESULT_EXPIRES:
            for k, v in DEFAULT_ENTRIES.iteritems():
                entries.setdefault(k, v)
        self.update_from_dict(entries)

    @property
    def schedule(self):
        if not self._initial_read:
            info("DatabaseScheduler: intial read")
            self._initial_read = True
            self.sync()
            self._schedule = self.all_as_schedule()
            msg = "DatabaseScheduler: Current CELERYBEAT_SCHEDULE:\n%s"
            info(msg, '\n'.join(
                repr(entry) for entry in self._schedule.itervalues()),
            )
        return self._schedule
