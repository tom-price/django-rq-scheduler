from __future__ import unicode_literals

from django.conf import settings
from django.contrib import admin
from django.contrib.contenttypes.admin import GenericStackedInline
from django.utils.translation import ugettext_lazy as _

from scheduler.models import CronJob, RepeatableJob, ScheduledJob,\
    JobArg, JobKwarg


QUEUES = [(key, key) for key in settings.RQ_QUEUES.keys()]


class QueueMixin(object):
    actions = ['delete_model']

    def get_actions(self, request):
        actions = super(QueueMixin, self).get_actions(request)
        del actions['delete_selected']
        return actions

    def get_form(self, request, obj=None, **kwargs):
        queue_field = self.model._meta.get_field('queue')
        queue_field.choices = QUEUES
        return super(QueueMixin, self).get_form(request, obj, **kwargs)

    def delete_model(self, request, obj):
        if hasattr(obj, 'all'):
            for o in obj.all():
                o.delete()
        else:
            obj.delete()
    delete_model.short_description = _("Delete selected %(verbose_name_plural)s")


class JobArgInline(GenericStackedInline):
    model = JobArg
    extra = 0
    fieldsets = (
        (None, {
            'fields': ('arg_name', 'str_val', 'int_val', 'datetime_val',),
        }),
    )
    class Media:
        js = ('scheduler/js/base.js',)


class JobKwargInline(GenericStackedInline):
    model = JobKwarg
    extra = 0
    fieldsets = (
        (None, {
            'fields': ('key', 'arg_name', 'str_val', 'int_val', 'datetime_val',),
        }),
    )

    class Media:
        js = ('scheduler/js/base.js',)


@admin.register(ScheduledJob)
class ScheduledJobAdmin(QueueMixin, admin.ModelAdmin):
    list_display = (
        'name', 'job_id', ScheduledJob.function_string, 'is_scheduled', 'scheduled_time', 'enabled')
    list_filter = ('enabled', )
    list_editable = ('enabled', )

    readonly_fields = ('job_id', )
    fieldsets = (
        (None, {
            'fields': ('name', 'callable', 'enabled', ),
        }),
        (_('RQ Settings'), {
            'fields': ('queue', 'job_id', ),
        }),
        (_('Scheduling'), {
            'fields': (
                'scheduled_time',
                'timeout',
                'result_ttl'
            ),
        }),
    )
    inlines = [JobArgInline, JobKwargInline]


@admin.register(RepeatableJob)
class RepeatableJobAdmin(QueueMixin, admin.ModelAdmin):
    list_display = (
        'name', 'job_id', RepeatableJob.function_string, 'is_scheduled', 'scheduled_time', 'interval_display',
        'enabled')
    list_filter = ('enabled', )
    list_editable = ('enabled', )

    readonly_fields = ('job_id', )
    fieldsets = (
        (None, {
            'fields': ('name', 'callable', 'enabled', ),
        }),
        (_('RQ Settings'), {
            'fields': ('queue', 'job_id', ),
        }),
        (_('Scheduling'), {
            'fields': (
                'scheduled_time',
                ('interval', 'interval_unit', ),
                'repeat',
                'timeout',
                'result_ttl'
            ),
        }),
    )
    inlines = [JobArgInline, JobKwargInline]


@admin.register(CronJob)
class CronJobAdmin(QueueMixin, admin.ModelAdmin):
    list_display = (
        'name', 'job_id', CronJob.function_string, 'is_scheduled', 'cron_string', 'enabled')
    list_filter = ('enabled', )
    list_editable = ('enabled', )

    readonly_fields = ('job_id', )
    fieldsets = (
        (None, {
            'fields': ('name', 'callable', 'enabled', ),
        }),
        (_('RQ Settings'), {
            'fields': ('queue', 'job_id', ),
        }),
        (_('Scheduling'), {
            'fields': (
                'cron_string',
                'repeat',
                'timeout',
            ),
        }),
    )
    inlines = [JobArgInline, JobKwargInline]

