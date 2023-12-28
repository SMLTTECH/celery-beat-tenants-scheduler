import logging
from typing import ClassVar

from django.core.cache import cache
from django.db import close_old_connections, transaction
from django.db.utils import DatabaseError, InterfaceError
from django_celery_beat.schedulers import ADD_ENTRY_ERROR, DatabaseScheduler, ModelEntry
from django_tenants.utils import get_public_schema_name, get_tenant_model, schema_context
from tenant_schemas_celery.scheduler import TenantAwareScheduleEntry

logger = logging.getLogger("celery-scheduler")
TenantModel = get_tenant_model()


class TenantModelEntry(ModelEntry, TenantAwareScheduleEntry):
    def __init__(self, model, app=None, *args, **kwargs):
        if args:
            # Unpickled from database
            self.tenant_schemas = args[-1]
        else:
            # Initialized from code
            self.tenant_schemas = kwargs.pop("tenant_schemas", None)
            if not self.tenant_schemas:
                self.tenant_schemas = ["public"]

        super().__init__(model, app)

    def __next__(self):
        self.model.last_run_at = self._default_now()
        self.model.total_run_count += 1
        self.model.no_changes = True
        next_entry = self.__class__(self.model)
        next_entry.tenant_schemas = self.tenant_schemas
        return next_entry

    def save(self):
        with schema_context(self.tenant_schemas[0]):
            super().save()

    def _disable(self, model):
        with schema_context(self.tenant_schemas[0]):
            model.no_changes = True
            model.enabled = False
            model.save()


class TenantDatabaseScheduler(DatabaseScheduler):
    Entry = TenantModelEntry
    diffs: ClassVar[dict] = {}

    @classmethod
    def get_schemas_list(cls) -> list:
        schemas_cache = cache.get("scheduler_schemas_list")
        if schemas_cache:
            return schemas_cache
        schemas = list(TenantModel.objects.values_list("schema_name", flat=True))
        schemas.append(get_public_schema_name())
        cache.set("scheduler_schemas_list", schemas, 60)  # one minute cache

        return schemas

    def all_as_schedule(self):
        schedule = {}
        schemas = self.get_schemas_list()

        for schema in schemas:
            with schema_context(schema):
                for model in self.Model.objects.enabled():
                    try:
                        schedule[f"{schema}:{model.name}"] = self.Entry(model, app=self.app, tenant_schemas=(schema,))
                    except ValueError as e:
                        logger.error(e)
        return schedule

    def reserve(self, entry):
        with schema_context(entry.tenant_schemas[0]):
            new_entry = next(entry)
            self._dirty.add(f"{new_entry.tenant_schemas[0]}:{new_entry.name}")
            return new_entry

    def is_due(self, entry):
        with schema_context(entry.tenant_schemas[0]):
            return entry.is_due()

    def apply_entry(self, entry: TenantModelEntry, producer=None):
        logger.info(
            "TenantDatabaseScheduler: Sending due task %s (%s) to %s tenant",
            entry.name,
            entry.task,
            str(len(entry.tenant_schemas)),
        )
        schema_name = entry.tenant_schemas[0] if entry.tenant_schemas else get_public_schema_name()
        with schema_context(schema_name):
            logger.debug(
                "Sending due task %s (%s) to tenant %s",
                entry.name,
                entry.task,
                schema_name,
            )
            try:
                result = self.apply_async(entry, producer=producer, advance=False)
            except Exception as exc:
                logger.exception(exc)
            else:
                logger.debug("%s sent. id->%s", entry.task, result.id)

    def schedule_changed(self):
        schemas = self.get_schemas_list()

        for schema in schemas:
            with schema_context(schema):
                try:
                    close_old_connections()
                    try:
                        transaction.commit()
                    except transaction.TransactionManagementError:
                        pass  # not in transaction management.

                    ts = self.Changes.last_change()
                except DatabaseError as exc:
                    logger.exception("Database gave error: %r", exc)
                    continue
                except InterfaceError:
                    logger.warning(
                        "DatabaseScheduler: InterfaceError in schedule_changed(), " "waiting to retry in next call...",
                    )
                    continue

                try:
                    last = self.diffs.get(schema)
                    if ts and ts > (last if last else ts):
                        return True
                finally:
                    self.diffs[schema] = ts
        return False

    def update_from_dict(self, mapping):
        schemas = self.get_schemas_list()

        for schema in schemas:
            with schema_context(schema):
                s = {}
                for name, entry_fields in mapping.items():
                    try:
                        entry = self.Entry.from_entry(name, app=self.app, **entry_fields)
                        if entry.model.enabled:
                            s[name] = entry

                    except Exception as exc:
                        logger.exception(ADD_ENTRY_ERROR, name, exc, entry_fields)
                self.schedule.update(s)
