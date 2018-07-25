# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from datetime import datetime, timedelta

from django.conf import settings
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone

import factory
from fakeredis import FakeStrictRedis
import pytz
import django_rq
from django_rq import job
from scheduler.models import BaseJob, CronJob, RepeatableJob, ScheduledJob
import django_rq.queues


class BaseJobFactory(factory.DjangoModelFactory):
    class Meta:
        django_get_or_create = ('name',)

    name = factory.Sequence(lambda n: 'Scheduled Job %d' % n)
    queue = list(settings.RQ_QUEUES.keys())[0]
    callable = 'scheduler.tests.test_job'
    enabled = True
    timeout = None
    # job_id = None


class ScheduledJobFactory(BaseJobFactory):
    class Meta:
        model = ScheduledJob

    result_ttl = None

    @factory.lazy_attribute
    def scheduled_time(self):
        return timezone.now() + timedelta(days=1)


class RepeatableJobFactory(BaseJobFactory):
    class Meta:
        model = RepeatableJob

    result_ttl = None
    interval = 1
    interval_unit = 'hours'
    repeat = None

    @factory.lazy_attribute
    def scheduled_time(self):
        return timezone.now() + timedelta(minutes=1)


class CronJobFactory(BaseJobFactory):
    class Meta:
        model = CronJob

    cron_string = "0 0 * * *"
    repeat = None


@job
def test_job():
    return 1 + 1


@job
def test_args_kwargs(*args, **kwargs):
    func = "test_args_kwargs({0}, {1})"
    args_list = [repr(arg) for arg in args]
    arg_string = ', '.join(args_list)
    kwarg_string = ', '.join([k + '=' + repr(kwargs[k]) for k in kwargs])
    return func.format(arg_string, kwarg_string)

test_non_callable = 'I am a teapot'


class BaseTestCases:

    class TestBaseJob(TestCase):
        def setUp(self):
            django_rq.queues.get_redis_connection = lambda _, strict: FakeStrictRedis()
            # Turn on synchronous mode to execute jobs immediately instead of passing them off to the workers
            for config in settings.RQ_QUEUES.values():
                config["ASYNC"] = False

        # Never runs as BaseJob but helps IDE auto-completes
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
            job_id = 1
            job = self.JobClassFactory()
            job.id = job_id
            job.save()
            is_scheduled = job.is_scheduled()
            self.assertIsNotNone(job.job_id)
            self.assertTrue(is_scheduled)
            scheduler = job.scheduler()
            job.delete()
            is_scheduled = job_id in scheduler
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

    class TestSchedulableJob(TestBaseJob):
        # Currently ScheduledJob and RepeatableJob
        JobClass = ScheduledJob
        JobClassFactory = ScheduledJobFactory

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
