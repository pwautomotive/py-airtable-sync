import datetime as dt
import pytz as tz
from enum import Enum
from logging import getLogger
from typing import List, Dict, Optional, Any, Callable

from pyairtable import Api
from pyairtable.api.types import RecordDict, Fields, UpdateRecordDict, CreateRecordDict
from pydantic import BaseModel, ConfigDict

from py_airtable_sync.config import FieldUpdateType, ForeignKeyUpdateType, FieldConfig, TableConfig, AirtableBaseConfig

logger = getLogger(__name__)

# Type alias for the cache of Airtable tables.
# The cache is a dictionary where the key is the table ID and the value is another dictionary.
# The inner dictionary's key is the record ID and the value is the record data.
type TableCache = Dict[str, Dict[str, RecordDict]]

# Type alias for primary key to record id index
type TableCacheIndex = dict[str, dict[tuple, str]]

# Type alias for a source record.
# A source record is a dictionary where the key is a string and the value can be of any type.
type SourceRecord = Dict[str, Any]

# Type alias for a list of source records.
# A source record list is a list of source records.
type SourceRecordList = List[SourceRecord]


class AirtableManagerSyncResult(BaseModel):
    """
    Result of synchronizing source records with Airtable tables.

    Attributes:
        inserted_records (int): The number of records inserted.
        updated_records (int): The number of records updated.
    """
    inserted_records: int
    updated_records: int


class AirtableManager:
    api: Api
    config: AirtableBaseConfig

    cache: TableCache = {}
    cache_index: TableCacheIndex = {}
    cache_refresh_datetime: Optional[dt.datetime] = None

    def __init__(self, api_key: str, config: AirtableBaseConfig):
        self.config = config
        self.api = Api(api_key)
        self.api.session.verify = config.verify_ssl

    def get_table_config(self, table_id_or_name: str) -> TableConfig:
        """
        Returns the TableConfig for the table with the specified table id or name.
        :param table_id_or_name: The id or name of the table.
        :return: The TableConfig for the table.
        """
        return next(table for table in self.config.tables if
                    table.table_id == table_id_or_name or table.table_name == table_id_or_name)

    def refresh_cache(self, force: bool = False):
        """
        Refreshes the cache by clearing the existing cache and repopulating it with
        the latest records from the Airtable tables specified in the configuration.
        :param force: Indicates if the cache should be refreshed even if it has not expired. Defaults to False.
        """
        # Only refresh the cache if the cache is empty or the cache has expired
        cache_expired = (
            not self.cache or
            not self.cache_refresh_datetime or
            (dt.datetime.now() - self.cache_refresh_datetime).total_seconds() > self.config.cache_max_age
        )

        if not force and not cache_expired:
            return
        else:
            self.cache_refresh_datetime = dt.datetime.now()

        self.cache.clear()
        self.cache_index.clear()

        for table in self.config.tables:
            logger.info(f"Refreshing cache for table {table.table_name if table.table_name else table.table_id}")
            table_obj = self.api.table(table.base_id, table.table_id)
            fields = [field.field_name for field in table.fields]
            logger.info(f"Including fields {fields}")
            records = table_obj.all(fields=[field.field_name for field in table.fields])
            self.cache[table.table_id] = {record['id']: record for record in records}

            # Build index for primary key fields
            if table.primary_key_fields:
                self._build_primary_key_index(table)

            logger.info(f"Loaded {len(records)} records into cache")

    def _build_primary_key_index(self, table_config: TableConfig):
        if not table_config.primary_key_fields:
            return

        table_id = table_config.table_id
        self.cache_index[table_id] = {}

        for record_id, record in self.cache[table_id].items():
            # Create a tuple of primary key values to use as dictionary key
            pk_values = tuple(record["fields"].get(pk_field) for pk_field in table_config.primary_key_fields)
            # Skip records with missing primary key values
            if None in pk_values:
                continue
            self.cache_index[table_id][pk_values] = record_id

    @staticmethod
    def get_source_record_field_value(source_record: SourceRecord, field_config: FieldConfig) -> Any:
        """
        Returns the value of the field in the source record specified by the field configuration.
        :param source_record: The source record.
        :param field_config: The field configuration.
        :return: The value of the field in the source record.
        """
        if field_config.source_field_name and field_config.source_field_name in source_record:
            return source_record[field_config.source_field_name]
        if field_config.field_name in source_record:
            return source_record[field_config.field_name]

        raise ValueError(f"Field {field_config.field_name} not found in source record")

    def sync_records(self, table_name_or_id: str, source_records: SourceRecordList,
                     before_update_fn: Callable | None = None) -> AirtableManagerSyncResult:
        """
        Synchronizes the source_records with the records in the Airtable table specified by table_id.
        :param table_name_or_id: The ID of the Airtable table to sync the records with.
        :param source_records: The list of records to sync with the Airtable table. Records might be mutated in place.
        :param before_update_fn: A function to call before comparing source and remote records. Defaults to None.
        :return:
        """
        self.refresh_cache()

        table_config = self.get_table_config(table_name_or_id)
        table_id = table_config.table_id
        self.populate_foreign_keys(table_config, source_records)
        AirtableManager.clean_date_formats(table_config, source_records)

        # Insert new records
        logger.info("Looking for new records...")
        records_to_insert = self.get_new_records(table_config, source_records)
        logger.info(f"Found {len(records_to_insert)} new records to insert")
        created_records = self.api.table(table_config.base_id, table_config.table_id).batch_create(records_to_insert)
        logger.info(f"Inserted {len(created_records)} new records")

        # Update existing records
        logger.info("Looking for changed records...")
        records_to_update = self.get_updated_records(table_config, source_records, before_update_fn)
        logger.info(f"Found {len(records_to_update)} existing records to update")
        updated_records = self.api.table(table_config.base_id, table_config.table_id).batch_update(records_to_update)
        logger.info(f"Updated {len(updated_records)} existing records")

        # Update the cache
        for record in created_records + updated_records:
            self.cache[table_id][record["id"]] = record

        return AirtableManagerSyncResult(
            inserted_records=len(created_records),
            updated_records=len(updated_records)
        )

    def get_remote_record(self, table_config: TableConfig, source_record: SourceRecord) -> RecordDict | None:
        """
        Gets the remote record with matching primary key fields using an index for faster lookup.
        """
        table_id = table_config.table_id

        # Use the index if available
        if table_id in self.cache_index:
            # Create tuple of primary key values from source record
            pk_values = tuple(source_record.get(pk_field) for pk_field in table_config.primary_key_fields)

            # Look up record ID in the index
            record_id = self.cache_index[table_id].get(pk_values)
            return self.cache[table_id].get(record_id) if record_id else None

        # Fallback to linear search if no index or record not found in index
        return next(
            (
                remote_record for remote_record in self.cache[table_config.table_id].values()
                if all(primary_key_field in remote_record["fields"] and
                       source_record[primary_key_field] == remote_record["fields"][primary_key_field]
                       for primary_key_field in table_config.primary_key_fields)
            ),
            None
        )

    def get_new_records(self, table_config: TableConfig, source_records: SourceRecordList) -> List[Dict[str, Any]]:
        """
        Returns the records that are new in the source_records compared to the records in the cache.
        :param table_config: The configuration of the table for which to find new records.
        :param source_records: The list of records to sync with the Airtable table. 
        :return: The records that are new in the source_records.
        """
        source_field_name = lambda fc: fc.source_field_name or fc.field_name

        new_records = []
        for source_record in source_records:
            # If the record already exists in the remote table, skip it
            if self.get_remote_record(table_config, source_record):
                continue

            new_record = {
                field_config.field_name: source_record[source_field_name(field_config)]
                for field_config in table_config.fields
                if source_field_name(field_config) in source_record
            }
            new_records.append(new_record)

        return new_records

    def get_updated_records(self, table_config: TableConfig, source_records: SourceRecordList,
                            before_update_fn: Callable | None = None) -> List[
        UpdateRecordDict]:
        """
        Returns the records that have been updated in the source_records compared to the records in the cache.
        :param table_config: The configuration of the table for which to find updated records.
        :param source_records: The list of records to sync with the Airtable table.
        
        :return: 
        """
        updated_records = []
        for source_record in source_records:
            remote_record = self.get_remote_record(table_config, source_record)

            # If the record does not exist in the remote table, skip it
            if not remote_record:
                continue

            if before_update_fn:
                before_update_fn(source_record, remote_record)

            updated_fields = self.get_updated_fields(table_config, source_record, remote_record)
            if len(updated_fields) > 0:
                updated_records.append({"id": remote_record["id"], "fields": updated_fields})

        return updated_records

    @staticmethod
    def get_updated_fields(table_config: TableConfig, source_record: SourceRecord,
                           remote_record: RecordDict) -> Fields:
        """
        Returns the fields that have been updated in the source_record compared to the remote record.
        :param table_config: The configuration of the table.
        :param source_record: The source record to compare.
        :param remote_record: The remote record to compare.
        :return: The fields that have been updated in the source_record.
        """
        updated_fields: Fields = {}
        for field_config in table_config.fields:
            # If the update type is never we don't need to do anything with this field
            if field_config.update_type == FieldUpdateType.never:
                continue

            # Compare the source field value with the remote field value
            source_field_name = field_config.source_field_name or field_config.field_name
            source_field_value = source_record.get(source_field_name, None)
            remote_field_value = remote_record["fields"].get(field_config.field_name, None)
            if source_field_value == remote_field_value:
                continue

            is_qualifying_override = field_config.if_empty_overrides and source_field_value in field_config.if_empty_overrides
            is_empty_should_update = remote_field_value is None or is_qualifying_override
            should_update = (
                field_config.update_type == FieldUpdateType.always or
                field_config.update_type == FieldUpdateType.if_empty and is_empty_should_update)

            if should_update:
                updated_fields[field_config.field_name] = source_field_value

        return updated_fields

    def populate_foreign_keys(self, table_config: TableConfig, source_records: SourceRecordList):
        """
        Populates the foreign key fields in the source_records with the corresponding record IDs from the foreign tables.
        :param table_config: The configuration of the table.
        :param source_records: The list of records to sync with the Airtable table.
        :return: 
        """
        for field_config in [f for f in table_config.fields if f.foreign_table_id]:
            for record in source_records:
                self.populate_foreign_key(field_config, record)

    def populate_foreign_key(self, field_config: FieldConfig, source_record: dict):
        """
        Populates the foreign key field in the source_record with the corresponding record ID from the foreign table.
        :param field_config: The configuration of the field.
        :param source_record: The source record to update.
        :return: 
        """
        source_field_name = field_config.source_field_name or field_config.field_name
        source_record_foreign_value = source_record.get(source_field_name, None)
        if not source_record_foreign_value:
            return

        foreign_field_name = field_config.foreign_field_name

        foreign_record = next(
            (
                foreign_record for foreign_record in self.cache[field_config.foreign_table_id].values()
                if foreign_record["fields"].get(foreign_field_name) == source_record_foreign_value
            ),
            None
        )

        if not foreign_record and field_config.foreign_update_type == ForeignKeyUpdateType.insert:
            # If the foreign record does not exist and the update type is insert, create the foreign record
            foreign_table_config = self.get_table_config(field_config.foreign_table_id)
            foreign_table = self.api.table(foreign_table_config.base_id, foreign_table_config.table_id)
            foreign_record = foreign_table.create(
                {field_config.foreign_field_name: source_record[source_field_name]})
            # Insert the new foreign record into the cache
            self.cache[field_config.foreign_table_id][foreign_record["id"]] = foreign_record

        # Update the source_record with the id of the foreign record, Airtable expects an array of record ids
        source_record[source_field_name] = [foreign_record["id"]]

    @staticmethod
    def clean_date_formats(table_config: TableConfig, source_records: SourceRecordList):
        """
        Populates the date fields in the source_records with the corresponding date formats.
        :param table_config: The configuration of the table.
        :param source_records: The list of records to sync with the Airtable table.
        :return:
        """
        for source_record in source_records:
            for field_config in table_config.fields:

                source_field_name = field_config.source_field_name or field_config.field_name
                source_field_value = source_record.get(source_field_name, None)

                if not source_field_value:
                    continue

                if not isinstance(source_field_value, dt.datetime) or not isinstance(source_field_value, dt.date):
                    continue

                source_field_timezone = tz.timezone(field_config.timezone or 'UTC')

                # Remove the timezone information from the source field value if it exists
                if source_field_value.tzinfo:
                    source_field_value = source_field_value.replace(tzinfo=None)

                # Apply the source field timezone to the source field value
                source_field_value = source_field_timezone.localize(source_field_value).astimezone(tz.UTC)

                source_record[source_field_name] = source_field_value.strftime("%Y-%m-%dT%H:%M:%S") + '.000Z'
