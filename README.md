# celery-beat-tenants-scheduler
DatabaseScheduler adapted for use with django-tenants

# tenant_celery_scheduler
Планировщик задач на основе DatabaseScheduler. Позволяет конфигурировать периодические задачи Celery для каждого тенанта отдельно.
## Установка:
0. Настроить django-tenants, django-tenants-celery
1. В settings.py: проекта
```python
CELERY_BEAT_SCHEDULER = "celery_beat_tenants_scheduler.scheduler.TenantDatabaseScheduler"
```
Или в docker-compose/helm (docker-compose.yml)
```
command: celery -A catalog beat -l debug --scheduler celery_beat_tenants_scheduler.scheduler.TenantDatabaseScheduler
```
