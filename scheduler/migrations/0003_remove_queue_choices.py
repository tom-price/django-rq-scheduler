# -*- coding: utf-8 -*-
# Generated by Django 1.9.5 on 2016-04-30 03:10

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('scheduler', '0002_add_timeout'),
    ]

    operations = [
        migrations.AlterField(
            model_name='repeatablejob',
            name='queue',
            field=models.CharField(max_length=16, verbose_name='queue'),
        ),
        migrations.AlterField(
            model_name='scheduledjob',
            name='queue',
            field=models.CharField(max_length=16, verbose_name='queue'),
        ),
    ]
