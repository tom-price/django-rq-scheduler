from __future__ import unicode_literals
import importlib
from datetime import timedelta

import croniter

from django.conf import settings
from django.contrib.contenttypes.fields import GenericForeignKey, GenericRelation
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import models
from django.templatetags.tz import utc
from django.utils.encoding import python_2_unicode_compatible
from django.utils.translation import ugettext_lazy as _

import django_rq
from model_utils import Choices
from model_utils.models import TimeStampedModel


@python_2_unicode_compatible
class BaseJobArg(models.Model):

    ARG_NAME = Choices(
        ('str_val', _('string')),
        ('int_val', _('int')),
        ('datetime_val', _('Datetime')),
    )
    str_val = models.CharField(_('String Value'), blank=True, max_length=255)
    int_val = models.IntegerField(_('Int Value'), blank=True, null=True)
    datetime_val = models.DateTimeField(_('Datetime Value'), blank=True, null=True)
    
    arg_name = models.CharField(
        _('Argument Type'), max_length=12, choices=ARG_NAME, default=ARG_NAME.str_val
    )

    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    content_object = GenericForeignKey()

    def clean(self):
        self.clean_one_value()

    def clean_one_value(self):
        count = 0
        count += 1 if self.str_val != '' else 0
        count += 1 if self.int_val else 0
        count += 1 if self.datetime_val else 0
        if count == 0:
            raise ValidationError({
                'arg_name': ValidationError(
                    _('At least one arg type must have a value'), code='invalid')
            })
        if count > 1:
            raise ValidationError({
                'arg_name': ValidationError(
                    _('There are multiple arg types with values'), code='invalid')
            })

    class Meta:
        abstract = True


class JobArg(BaseJobArg):
    pass


class JobKwarg(BaseJobArg):
    key = models.CharField(max_length=255)


@python_2_unicode_compatible
class BaseJob(TimeStampedModel):

    name = models.CharField(_('name'), max_length=128, unique=True)
    callable = models.CharField(_('callable'), max_length=2048)
    callable_args = GenericRelation(JobArg)
    callable_kwargs = GenericRelation(JobKwarg)
    enabled = models.BooleanField(_('enabled'), default=True)
    queue = models.CharField(_('queue'), max_length=16)
    job_id = models.CharField(
        _('job id'), max_length=128, editable=False, blank=True, null=True)
    timeout = models.IntegerField(
        _('timeout'), blank=True, null=True,
        help_text=_(
            'Timeout specifies the maximum runtime, in seconds, for the job '
            'before it\'ll be considered \'lost\'. Blank uses the default '
            'timeout.'
        )
    )
    result_ttl = models.IntegerField(
        _('result ttl'), blank=True, null=True,
        help_text=_('The TTL value (in seconds) of the job result. -1: '
                    'Result never expires, you should delete jobs manually. '
                    '0: Result gets deleted immediately. >0: Result expires '
                    'after n seconds.')
    )

    def __str__(self):
        return self.name

    def callable_func(self):
        path = self.callable.split('.')
        module = importlib.import_module('.'.join(path[:-1]))
        func = getattr(module, path[-1])
        if callable(func) is False:
            raise TypeError("'{}' is not callable".format(self.callable))
        return func

    def clean(self):
        self.clean_callable()
        self.clean_queue()

    def clean_callable(self):
        try:
            self.callable_func()
        except:
            raise ValidationError({
                'callable': ValidationError(
                    _('Invalid callable, must be importable'), code='invalid')
            })

    def clean_queue(self):
        queue_keys = settings.RQ_QUEUES.keys()
        if self.queue not in queue_keys:
            raise ValidationError({
                'queue': ValidationError(
                    _('Invalid queue, must be one of: {}'.format(
                        ', '.join(queue_keys))), code='invalid')
            })

    def is_scheduled(self):
        return self.job_id in self.scheduler()
    is_scheduled.short_description = _('is scheduled?')
    is_scheduled.boolean = True

    def save(self, **kwargs):
        self.unschedule()
        if self.enabled:
            self.schedule()
        super(BaseJob, self).save(**kwargs)

    def delete(self, **kwargs):
        self.unschedule()
        super(BaseJob, self).delete(**kwargs)

    def scheduler(self):
        return django_rq.get_scheduler(self.queue)

    def is_schedulable(self):
        if self.job_id:
            return False
        return self.enabled

    def schedule(self):
        if self.is_schedulable() is False:
            return False
        kwargs = self.parse_kwargs()
        if self.timeout:
            kwargs['timeout'] = self.timeout
        if self.result_ttl is not None:
            kwargs['result_ttl'] = self.result_ttl
        job = self.scheduler().enqueue_at(
            self.schedule_time_utc(),
            self.callable_func(),
            *self.parse_args(),
            **kwargs
        )
        self.job_id = job.id
        return True

    def unschedule(self):
        if self.is_scheduled():
            self.scheduler().cancel(self.job_id)
        self.job_id = None
        return True

    def schedule_time_utc(self):
        return utc(self.scheduled_time)

    def parse_args(self):
        args = self.callable_args.values().order_by('id')
        return [arg[arg['arg_name']] for arg in args]

    def parse_kwargs(self):
        kwargs = self.callable_kwargs.values().order_by('id')
        return {kwarg['key']: kwarg[kwarg['arg_name']] for kwarg in kwargs}

    def function_string(self):
        func = self.callable + "(\u200b{})"  # zero-width space allows textwrap
        args = self.parse_args()
        args_list = [repr(arg) for arg in args]
        kwargs = self.parse_kwargs()
        kwargs_list = [k + '=' + repr(v) for (k, v) in kwargs.items()]
        return func.format(', '.join(args_list + kwargs_list))

    function_string.short_description = 'Callable'

    class Meta:
        abstract = True


class ScheduledTimeMixin(models.Model):

    scheduled_time = models.DateTimeField(_('scheduled time'))

    def schedule_time_utc(self):
        return utc(self.scheduled_time)

    class Meta:
        abstract = True


class ScheduledJob(ScheduledTimeMixin, BaseJob):

    class Meta:
        verbose_name = _('Scheduled Job')
        verbose_name_plural = _('Scheduled Jobs')
        ordering = ('name', )


class RepeatableJob(ScheduledTimeMixin, BaseJob):

    UNITS = Choices(
        ('minutes', _('minutes')),
        ('hours', _('hours')),
        ('days', _('days')),
        ('weeks', _('weeks')),
    )

    interval = models.PositiveIntegerField(_('interval'))
    interval_unit = models.CharField(
        _('interval unit'), max_length=12, choices=UNITS, default=UNITS.hours
    )
    repeat = models.PositiveIntegerField(_('repeat'), blank=True, null=True)

    def interval_display(self):
        return '{} {}'.format(self.interval, self.get_interval_unit_display())

    def interval_seconds(self):
        kwargs = {
            self.interval_unit: self.interval,
        }
        return timedelta(**kwargs).total_seconds()

    def schedule(self):
        if self.is_schedulable() is False:
            return False
        kwargs = {
            'args': self.parse_args(),
            'kwargs': self.parse_kwargs(),
            'interval': self.interval_seconds(),
            'repeat': self.repeat
        }
        if self.timeout:
            kwargs['timeout'] = self.timeout
        if self.result_ttl is not None:
            kwargs['result_ttl'] = self.result_ttl
        job = self.scheduler().schedule(
            self.schedule_time_utc(),
            self.callable_func(),
            **kwargs
        )
        self.job_id = job.id
        return True

    class Meta:
        verbose_name = _('Repeatable Job')
        verbose_name_plural = _('Repeatable Jobs')
        ordering = ('name', )


class CronJob(BaseJob):
    result_ttl = None

    cron_string = models.CharField(
        _('cron string'), max_length=64,
        help_text=_('Define the schedule in a crontab like syntax.')
    )
    repeat = models.PositiveIntegerField(_('repeat'), blank=True, null=True)

    def clean(self):
        super(CronJob, self).clean()
        self.clean_cron_string()

    def clean_cron_string(self):
        try:
            croniter.croniter(self.cron_string)
        except ValueError as e:
            raise ValidationError({
                'cron_string': ValidationError(
                    _(str(e)), code='invalid')
            })

    def schedule(self):
        if self.is_schedulable() is False:
            return False
        kwargs = {
            'args': self.parse_args(),
            'kwargs': self.parse_kwargs(),
            'repeat': self.repeat
        }
        if self.timeout:
            kwargs['timeout'] = self.timeout
        job = self.scheduler().cron(
            self.cron_string,
            self.callable_func(),
            **kwargs
        )
        self.job_id = job.id
        return True

    class Meta:
        verbose_name = _('Cron Job')
        verbose_name_plural = _('Cron Jobs')
        ordering = ('name', )

