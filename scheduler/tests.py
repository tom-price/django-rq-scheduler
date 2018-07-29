# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from datetime import datetime, timedelta

from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone

import factory
from fakeredis import FakeStrictRedis
import pytz
import django_rq
from django_rq import job
from scheduler.models import BaseJob, CronJob, RepeatableJob, ScheduledJob,\
    BaseJobArg, JobArg, JobKwarg
import django_rq.queues


class BaseJobFactory(factory.DjangoModelFactory):
    name = factory.Sequence(lambda n: 'Scheduled Job %d' % n)
    queue = list(settings.RQ_QUEUES.keys())[0]
    callable = 'scheduler.tests.test_job'
    enabled = True
    timeout = None
    # job_id = None

    class Meta:
        django_get_or_create = ('name',)
        abstract = True


class ScheduledJobFactory(BaseJobFactory):
    result_ttl = None

    @factory.lazy_attribute
    def scheduled_time(self):
        return timezone.now() + timedelta(days=1)

    class Meta:
        model = ScheduledJob


class RepeatableJobFactory(BaseJobFactory):
    result_ttl = None
    interval = 1
    interval_unit = 'hours'
    repeat = None

    @factory.lazy_attribute
    def scheduled_time(self):
        return timezone.now() + timedelta(minutes=1)

    class Meta:
        model = RepeatableJob


class CronJobFactory(BaseJobFactory):
    cron_string = "0 0 * * *"
    repeat = None

    class Meta:
        model = CronJob


class BaseJobArgFactory(factory.DjangoModelFactory):
    arg_name = 'str_val'
    str_val = ''
    int_val = None
    datetime_val = None
    object_id = factory.SelfAttribute('content_object.id')
    content_type = factory.LazyAttribute(
        lambda o: ContentType.objects.get_for_model(o.content_object))
    content_object = factory.SubFactory(ScheduledJobFactory)

    class Meta:
        exclude = ['content_object']
        abstract = True


class JobArgFactory(BaseJobArgFactory):
    class Meta:
        model = JobArg


class JobKwargFactory(BaseJobArgFactory):
    key = factory.Sequence(lambda n: 'key%d' % n)

    class Meta:
        model = JobKwarg


@job
def test_job():
    return 1 + 1


@job
def test_args_kwargs(*args, **kwargs):
    func = "test_args_kwargs({})"
    args_list = [repr(arg) for arg in args]
    kwargs_list = [k + '=' + repr(v) for (k, v) in kwargs.items()]
    return func.format(', '.join(args_list + kwargs_list))


test_non_callable = 'I am a teapot'


class BaseTestCases:

    class TestBaseJobArg(TestCase):
        JobArgClass = BaseJobArg
        JobArgClassFactory = BaseJobArgFactory

        def test_clean_one_value_empty(self):
            arg = self.JobArgClassFactory()
            with self.assertRaises(ValidationError):
                arg.clean_one_value()

        def test_clean_one_value_invalid_str_int(self):
            arg = self.JobArgClassFactory(str_val='not blank', int_val=1, datetime_val=None)
            with self.assertRaises(ValidationError):
                arg.clean_one_value()

        def test_clean_one_value_invalid_str_datetime(self):
            arg = self.JobArgClassFactory(str_val='not blank', int_val=None, datetime_val=timezone.now())
            with self.assertRaises(ValidationError):
                arg.clean_one_value()

        def test_clean_one_value_invalid_int_datetime(self):
            arg = self.JobArgClassFactory(str_val= '', int_val=1, datetime_val=timezone.now())
            with self.assertRaises(ValidationError):
                arg.clean_one_value()

        def test_clean_invalid(self):
            arg = self.JobArgClassFactory(str_val='str', int_val=1, datetime_val=timezone.now())
            with self.assertRaises(ValidationError):
                arg.clean()

        def test_clean(self):
            arg = self.JobArgClassFactory(str_val='something')
            self.assertIsNone(arg.clean())


    class TestBaseJob(TestCase):
        def setUp(self):
            django_rq.queues.get_redis_connection = lambda _, strict: FakeStrictRedis()

        JobClass = BaseJob
        JobClassFactory = BaseJobFactory

        def test_callable_func(self):
            job = self.JobClass()
            job.callable = 'scheduler.tests.test_job'
            func = job.callable_func()
            self.assertEqual(test_job, func)

        def test_callable_func_not_callable(self):
            job = self.JobClass()
            job.callable = 'scheduler.tests.test_non_callable'
            with self.assertRaises(TypeError):
                job.callable_func()

        def test_clean_callable(self):
            job = self.JobClass()
            job.callable = 'scheduler.tests.test_job'
            self.assertIsNone(job.clean_callable())

        def test_clean_callable_invalid(self):
            job = self.JobClass()
            job.callable = 'scheduler.tests.test_non_callable'
            with self.assertRaises(ValidationError):
                job.clean_callable()

        def test_clean_queue(self):
            for queue in settings.RQ_QUEUES.keys():
                job = self.JobClass()
                job.queue = queue
                self.assertIsNone(job.clean_queue())

        def test_clean_queue_invalid(self):
            job = self.JobClass()
            job.queue = 'xxxxxx'
            job.callable = 'scheduler.tests.test_job'
            with self.assertRaises(ValidationError):
                job.clean()

        def test_clean(self):
            job = self.JobClass()
            job.queue = list(settings.RQ_QUEUES)[0]
            job.callable = 'scheduler.tests.test_job'
            self.assertIsNone(job.clean())

        def test_clean_invalid_callable(self):
            job = self.JobClass()
            job.queue = list(settings.RQ_QUEUES)[0]
            job.callable = 'scheduler.tests.test_non_callable'
            with self.assertRaises(ValidationError):
                job.clean()

        def test_clean_invalid_queue(self):
            job = self.JobClass()
            job.queue = 'xxxxxx'
            job.callable = 'scheduler.tests.test_job'
            with self.assertRaises(ValidationError):
                job.clean()

        def test_is_schedulable_already_scheduled(self):
            job = self.JobClass()
            job.job_id = 'something'
            self.assertFalse(job.is_schedulable())

        def test_is_schedulable_disabled(self):
            job = self.JobClass()
            job.enabled = False
            self.assertFalse(job.is_schedulable())

        def test_is_schedulable_enabled(self):
            job = self.JobClass()
            job.enabled = True
            self.assertTrue(job.is_schedulable())

        def test_schedule(self):
            job = self.JobClassFactory()
            self.assertIsNotNone(job.job_id)

        def test_unschedulable(self):
            job = self.JobClassFactory(enabled=False)
            self.assertIsNone(job.job_id)

        def test_schedule2(self):
            job = self.JobClass()
            job.queue = list(settings.RQ_QUEUES)[0]
            job.enabled = False
            self.assertFalse(job.schedule())

        def test_unschedule(self):
            job = self.JobClassFactory()
            self.assertTrue(job.unschedule())
            self.assertIsNone(job.job_id)

        def test_unschedule_not_scheduled(self):
            job = self.JobClassFactory(enabled=False)
            self.assertTrue(job.unschedule())
            self.assertIsNone(job.job_id)

        def test_save_disabled(self):
            job = self.JobClassFactory(enabled=False)
            job.save()
            self.assertIsNone(job.job_id)

        def test_delete_and_unschedule(self):
            job = self.JobClassFactory()
            job.save()
            is_scheduled = job.is_scheduled()
            self.assertIsNotNone(job.job_id)
            self.assertTrue(is_scheduled)
            scheduler = job.scheduler()
            job.delete()
            is_scheduled = job.job_id in scheduler
            self.assertFalse(is_scheduled)

        def test_job_build(self):
            self.JobClassFactory.build()
            self.assertEqual(self.JobClass.objects.count(), 0)

        def test_job_create(self):
            self.JobClassFactory.create()
            self.assertEqual(self.JobClass.objects.count(), 1)

        def test_str(self):
            name = "test"
            job = self.JobClassFactory(name=name)
            self.assertEqual(str(job), name)

        def test_callable_passthroug(self):
            job = self.JobClassFactory()
            scheduler = django_rq.get_scheduler(job.queue)
            entry = next(i for i in scheduler.get_jobs() if i.id == job.job_id)
            self.assertEqual(entry.func, test_job)

        def test_timeout_passthroug(self):
            job = self.JobClassFactory(timeout=500)
            scheduler = django_rq.get_scheduler(job.queue)
            entry = next(i for i in scheduler.get_jobs() if i.id == job.job_id)
            self.assertEqual(entry.timeout, 500)

        def test_parse_args(self):
            job = self.JobClassFactory()
            date = timezone.now()
            JobArgFactory(str_val='one', content_object=job)
            JobArgFactory(arg_name='int_val', int_val=2, content_object=job)
            JobArgFactory(arg_name='datetime_val', datetime_val=date, content_object=job)
            self.assertEqual(job.parse_args(), ['one', 2, date])

        def test_parse_kwargs(self):
            job = self.JobClassFactory()
            date = timezone.now()
            JobKwargFactory(key='key1', arg_name='str_val', str_val='one', content_object=job)
            JobKwargFactory(key='key2', arg_name='int_val', int_val=2, content_object=job)
            JobKwargFactory(key='key3', arg_name='datetime_val', datetime_val=date, content_object=job)
            self.assertEqual(job.parse_kwargs(), dict(key1='one', key2=2, key3=date))

        def test_function_string(self):
            job = self.JobClassFactory()
            date = timezone.now()
            JobArgFactory(arg_name='str_val', str_val='one', content_object=job)
            JobArgFactory(arg_name='int_val', int_val=1, content_object=job)
            JobArgFactory(arg_name='datetime_val', datetime_val=date, content_object=job)
            JobKwargFactory(key='key1', arg_name='str_val', str_val='one', content_object=job)
            JobKwargFactory(key='key2', arg_name='int_val', int_val=2, content_object=job)
            JobKwargFactory(key='key3', arg_name='datetime_val', datetime_val=date, content_object=job)
            self.assertEqual(job.function_string(),
                             ("scheduler.tests.test_job(\u200b'one', 1, {date}, " +
                              "key1='one', key2=2, key3={date})").format(date=repr(date)))

        def test_callable_result(self):
            job = self.JobClassFactory()
            scheduler = django_rq.get_scheduler(job.queue)
            entry = next(i for i in scheduler.get_jobs() if i.id == job.job_id)
            self.assertEqual(entry.perform(), 2)

        def test_callable_empty_args_and_kwagrgs(self):
            job = self.JobClassFactory(callable='scheduler.tests.test_args_kwargs')
            scheduler = django_rq.get_scheduler(job.queue)
            entry = next(i for i in scheduler.get_jobs() if i.id == job.job_id)
            self.assertEqual(entry.perform(), 'test_args_kwargs()')

        def test_callable_args_and_kwagrgs(self):
            job = self.JobClassFactory(callable='scheduler.tests.test_args_kwargs')
            date = timezone.now()
            JobArgFactory(arg_name='str_val', str_val='one', content_object=job)
            JobKwargFactory(key='key1', arg_name='int_val', int_val=2, content_object=job)
            JobKwargFactory(key='key2', arg_name='datetime_val', datetime_val=date, content_object=job)
            job.save()
            scheduler = django_rq.get_scheduler(job.queue)
            entry = next(i for i in scheduler.get_jobs() if i.id == job.job_id)
            self.assertEqual(entry.perform(),
                             "test_args_kwargs('one', key1=2, key2={})".format(repr(date)))

    class TestSchedulableJob(TestBaseJob):
        # Currently ScheduledJob and RepeatableJob
        JobClass = BaseJob
        JobClassFactory = BaseJobFactory

        def test_schedule_time_utc(self):
            job = self.JobClass()
            est = pytz.timezone('US/Eastern')
            scheduled_time = datetime(2016, 12, 25, 8, 0, 0, tzinfo=est)
            job.scheduled_time = scheduled_time
            utc = pytz.timezone('UTC')
            expected = scheduled_time.astimezone(utc).isoformat()
            self.assertEqual(expected, job.schedule_time_utc().isoformat())

        def test_result_ttl_passthroug(self):
            job = self.JobClassFactory(result_ttl=500)
            scheduler = django_rq.get_scheduler(job.queue)
            entry = next(i for i in scheduler.get_jobs() if i.id == job.job_id)
            self.assertEqual(entry.result_ttl, 500)


class TestJobArg(BaseTestCases.TestBaseJobArg):
    JobArgClass = JobArg
    JobArgClassFactory = JobArgFactory

    def test_value(self):
        arg = self.JobArgClassFactory(arg_name='str_val', str_val='something')
        self.assertEqual(arg.value(), 'something')

    def test__str__str_val(self):
        arg = self.JobArgClassFactory(arg_name='str_val', str_val='something')
        self.assertEqual('something', str(arg))

    def test__str__int_val(self):
        arg = self.JobArgClassFactory(arg_name='int_val', int_val=1)
        self.assertEqual('1', str(arg))

    def test__str__datetime_val(self):
        time = timezone.now()
        arg = self.JobArgClassFactory(arg_name='datetime_val', datetime_val=time)
        self.assertEqual(str(time), str(arg))

    def test__repr__str_val(self):
        arg = self.JobArgClassFactory(arg_name='str_val', str_val='something')
        self.assertEqual("'something'", repr(arg))

    def test__repr__int_val(self):
        arg = self.JobArgClassFactory(arg_name='int_val', int_val=1)
        self.assertEqual('1', repr(arg))

    def test__repr__datetime_val(self):
        time = timezone.now()
        arg = self.JobArgClassFactory(arg_name='datetime_val', datetime_val=time)
        self.assertEqual(repr(time), repr(arg))


class TestJobKwarg(BaseTestCases.TestBaseJobArg):
    JobArgClass = JobKwarg
    JobArgClassFactory = JobKwargFactory

    def test_value(self):
        kwarg = self.JobArgClassFactory(key='key', arg_name='str_val', str_val='value')
        self.assertEqual(kwarg.value(), ('key', 'value'))

    def test__str__str_val(self):
        kwarg = self.JobArgClassFactory(key='key1', arg_name='str_val', str_val='something')
        self.assertEqual("key=key1 value=something", str(kwarg))

    def test__str__int_val(self):
        kwarg = self.JobArgClassFactory(key='key1', arg_name='int_val', int_val=1)
        self.assertEqual("key=key1 value=1", str(kwarg))

    def test__str__datetime_val(self):
        time = timezone.now()
        kwarg = self.JobArgClassFactory(key='key1', arg_name='datetime_val', datetime_val=time)
        self.assertEqual("key=key1 value={}".format(time), str(kwarg))

    def test__repr__str_val(self):
        kwarg = self.JobArgClassFactory(key='key', arg_name='str_val', str_val='something')
        self.assertEqual("('key', 'something')", repr(kwarg))

    def test__repr__int_val(self):
        kwarg = self.JobArgClassFactory(key='key', arg_name='int_val', int_val=1)
        self.assertEqual("('key', 1)", repr(kwarg))

    def test__repr__datetime_val(self):
        time = timezone.now()
        kwarg = self.JobArgClassFactory(key='key', arg_name='datetime_val', datetime_val=time)
        self.assertEqual("('key', {})".format(repr(time)), repr(kwarg))


class TestScheduledJob(BaseTestCases.TestSchedulableJob):
    JobClass = ScheduledJob
    JobClassFactory = ScheduledJobFactory


class TestRepeatableJob(BaseTestCases.TestSchedulableJob):
    JobClass = RepeatableJob
    JobClassFactory = RepeatableJobFactory

    def test_interval_seconds_weeks(self):
        job = RepeatableJobFactory(interval=2, interval_unit='weeks')
        self.assertEqual(1209600.0, job.interval_seconds())

    def test_interval_seconds_days(self):
        job = RepeatableJobFactory(interval=2, interval_unit='days')
        self.assertEqual(172800.0, job.interval_seconds())

    def test_interval_seconds_hours(self):
        job = RepeatableJobFactory(interval=2, interval_unit='hours')
        self.assertEqual(7200.0, job.interval_seconds())

    def test_interval_seconds_minutes(self):
        job = RepeatableJobFactory(interval=15, interval_unit='minutes')
        self.assertEqual(900.0, job.interval_seconds())

    def test_interval_display(self):
        job = RepeatableJobFactory(interval=15, interval_unit='minutes')
        self.assertEqual(job.interval_display(), '15 minutes')

    def test_result_interval(self):
        job = self.JobClassFactory()
        scheduler = django_rq.get_scheduler(job.queue)
        entry = next(i for i in scheduler.get_jobs() if i.id == job.job_id)
        self.assertEqual(entry.meta['interval'], 3600)

    def test_repeat(self):
        job = self.JobClassFactory(repeat=10)
        scheduler = django_rq.get_scheduler(job.queue)
        entry = next(i for i in scheduler.get_jobs() if i.id == job.job_id)
        self.assertEqual(entry.meta['repeat'], 10)


class TestCronJob(BaseTestCases.TestBaseJob):
    JobClass = CronJob
    JobClassFactory = CronJobFactory

    def test_clean(self):
        job = self.JobClass()
        job.cron_string = '* * * * *'
        job.queue = list(settings.RQ_QUEUES)[0]
        job.callable = 'scheduler.tests.test_job'
        self.assertIsNone(job.clean())

    def test_clean_cron_string_invalid(self):
        job = self.JobClass()
        job.cron_string = 'not-a-cron-string'
        job.queue = list(settings.RQ_QUEUES)[0]
        job.callable = 'scheduler.tests.test_job'
        with self.assertRaises(ValidationError):
            job.clean_cron_string()

    def test_repeat(self):
        job = self.JobClassFactory(repeat=10)
        scheduler = django_rq.get_scheduler(job.queue)
        entry = next(i for i in scheduler.get_jobs() if i.id == job.job_id)
        self.assertEqual(entry.meta['repeat'], 10)
