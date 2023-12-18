# celery-beat-tenants-scheduler
A DatabaseScheduler adaptation designed for use with django-tenants.

# tenant_celery_scheduler
Task scheduler based on DatabaseScheduler. Allows configuration Celery tasks in DB for each individual tenant. Precisely tested with these types of tasks: 
- periodic
- crontab

## Installation:
1. First, configure `django-tenants` and `django-tenants-celery` as you need.
2. `django_celery_beat` must be in your `TENANT_APPS`
3. Specify the scheduler in the settings.py of your project:
```python
CELERY_BEAT_SCHEDULER = "celery_beat_tenants_scheduler.scheduler.TenantDatabaseScheduler"
```
or in docker-compose.yml
```
command: celery -A project beat -l debug --scheduler celery_beat_tenants_scheduler.scheduler.TenantDatabaseScheduler
```
