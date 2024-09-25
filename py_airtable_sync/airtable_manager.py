import datetime as dt
import pytz as tz
from enum import Enum
from logging import getLogger
from typing import List, Dict, Optional, Any

from pyairtable import Api
from pyairtable.api.types import RecordDict, Fields, UpdateRecordDict, CreateRecordDict
from pydantic import BaseModel, ConfigDict

logger = getLogger(__name__)


class FieldUpdateType(Enum):
    """
    Enum representing the different types of field updates.

    Attributes:
        never (str): The remote field should never be updated.
        always (str): The remote field should always be updated.
        if_empty (str): The remote field should be updated only if it is empty.
    """
    never = "never"
    always = "always"
    if_empty = "if_empty"


class ForeignKeyUpdateType(Enum):
    """
    Enum representing the different types of foreign key updates.

    Attributes:
        insert (str): Insert the foreign key if it does not exist.
        ignore (str): Ignore the foreign key if it does not exist.
    """
    insert = "insert"
    ignore = "ignore"


class FieldConfig(BaseModel):
    """
    Configuration for a field in an Airtable table.

    Attributes:
        model_config (ConfigDict): Configuration dictionary allowing extra fields.
        field_name (str): The name of the field.
        is_primary (bool): Indicates if the field is a primary key. Defaults to False.
        update_type (Optional[FieldUpdateType]): The type of update allowed for the field. Defaults to FieldUpdateType.never.
        foreign_table_id (Optional[str]): The ID of the foreign table if the field is a foreign key. Defaults to None.
        foreign_field_name (Optional[str]): The name of the field in the foreign table that this field references. Defaults to None.
        foreign_update_type (Optional[ForeignKeyUpdateType]): The type of update allowed for the foreign key. Defaults to ForeignKeyUpdateType.ignore.
        source_field_name (Optional[str]): The name of the field in the source record that corresponds to this field. Defaults to None.
    """
    model_config = ConfigDict(extra="allow")

    field_name: str
    is_primary: bool = False
    update_type: Optional[FieldUpdateType] = FieldUpdateType.never
    foreign_table_id: Optional[str] = None
    foreign_field_name: Optional[str] = None  # The name of the field in the foreign table that this field references
    foreign_update_type: Optional[ForeignKeyUpdateType] = ForeignKeyUpdateType.ignore
    source_field_name: Optional[str] = None
    timezone: Optional[str] = None


class TableConfig(BaseModel):
    """
    Configuration for an Airtable table.

    Attributes:
        model_config (ConfigDict): Configuration dictionary allowing extra fields.
        base_id (str): The ID of the Airtable base.
        table_id (str): The ID of the Airtable table.
        table_name (Optional[str]): The name of the Airtable table. Defaults to None.
        fields (List[FieldConfig]): The list of field configurations for the table.
        primary_key_fields (List[str]): The list of primary key field names.
    """
    model_config = ConfigDict(extra="allow")

    base_id: str
    table_id: str
    table_name: Optional[str] = None
    fields: List[FieldConfig]

    primary_key_fields: List[str] = []

    def __init__(self, **data: any):
        """
        Initializes the TableConfig instance.

        Args:
            **data: Arbitrary keyword arguments.
        """
        super().__init__(**data)
        self.primary_key_fields = [field.field_name for field in self.fields if field.is_primary]


class AirtableBaseConfig(BaseModel):
    """
    Configuration for an Airtable base.

    Attributes:
        model_config (ConfigDict): Configuration dictionary allowing extra fields.
        api_key (str): The API key for accessing the Airtable base.
        tables (List[TableConfig]): The list of table configurations for the base.
    """
    model_config = ConfigDict(extra="allow")

    api_key: str
    verify_ssl: Optional[bool] = True

    tables: List[TableConfig]


# Type alias for the cache of Airtable tables.
# The cache is a dictionary where the key is the table ID and the value is another dictionary.
# The inner dictionary's key is the record ID and the value is the record data.
type TableCache = Dict[str, Dict[str, RecordDict]]

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

    def __init__(self, config: AirtableBaseConfig):
        self.config = config
        self.api = Api(config.api_key)
        self.api.session.verify = config.verify_ssl

    def get_table_config(self, table_id_or_name: str) -> TableConfig:
        """
        Returns the TableConfig for the table with the specified table id or name.
        :param table_id_or_name: The id or name of the table.
        :return: The TableConfig for the table.
        """
        return next(table for table in self.config.tables if
                    table.table_id == table_id_or_name or table.table_name == table_id_or_name)

    def refresh_cache(self):
        """
        Refreshes the cache by clearing the existing cache and repopulating it with
        the latest records from the Airtable tables specified in the configuration.
        """
        self.cache.clear()
        
        

        for table in self.config.tables:
            logger.info(f"Refreshing cache for table {table.table_name if table.table_name else table.table_id}")
            table_obj = self.api.table(table.base_id, table.table_id)
            records = table_obj.all(fields=[field.field_name for field in table.fields])
            self.cache[table.table_id] = {record['id']: record for record in records}
            logger.info(f"Loaded {len(records)} records into cache")

    def sync_records(self, table_name_or_id: str, source_records: SourceRecordList) -> AirtableManagerSyncResult:
        """
        Synchronizes the source_records with the records in the Airtable table specified by table_id.
        :param table_name_or_id: The ID of the Airtable table to sync the records with.
        :param source_records: The list of records to sync with the Airtable table. Records might be mutated in place.
        :return: 
        """
        self.refresh_cache()

        table_config = self.get_table_config(table_name_or_id)
        table_id = table_config.table_id
        self.populate_foreign_keys(table_config, source_records)
        AirtableManager.clean_date_formats(source_records)

        # Insert new records
        records_to_insert = self.get_new_records(table_config, source_records)
        created_records = self.api.table(table_config.base_id, table_config.table_id).batch_create(records_to_insert)
        logger.info(f"Found {len(created_records)} new records to insert")

        # Update existing records
        records_to_update = self.get_updated_records(table_config, source_records)
        updated_records = self.api.table(table_config.base_id, table_config.table_id).batch_update(records_to_update)
        logger.info(f"Found {len(updated_records)} existing records to update")

        # Update the cache
        for record in created_records + updated_records:
            self.cache[table_id][record["id"]] = record

        return AirtableManagerSyncResult(
            inserted_records=len(created_records),
            updated_records=len(updated_records)
        )

    def get_remote_record(self, table_config: TableConfig, source_record: SourceRecord) -> RecordDict:
        # Get the remote record with matching primary key fields
        return next(
            (
                remote_record for remote_record in self.cache[table_config.table_id].values()
                if all(primary_key_field in remote_record["fields"] and
                       source_record[primary_key_field] == remote_record["fields"][primary_key_field]
                       for primary_key_field in table_config.primary_key_fields)
            ),
            None
        )

    def get_new_records(self, table_config: TableConfig, source_records: SourceRecordList) -> List[CreateRecordDict]:
        """
        Returns the records that are new in the source_records compared to the records in the cache.
        :param table_config: The configuration of the table for which to find new records.
        :param source_records: The list of records to sync with the Airtable table. 
        :return: The records that are new in the source_records.
        """
        source_field_name = lambda fc: fc.source_field_name or fc.field_name

        new_records = []
        for source_record in source_records:
            if self.get_remote_record(table_config, source_record):
                continue

            new_record = {
                field_config.field_name: source_record[source_field_name(field_config)]
                for field_config in table_config.fields
                if source_field_name(field_config) in source_record
            }
            new_records.append(new_record)

        return new_records

    def get_updated_records(self, table_config: TableConfig, source_records: SourceRecordList) -> List[
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
            if not remote_record:
                continue
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
            if field_config.update_type == FieldUpdateType.never:
                continue

            source_field_name = field_config.source_field_name or field_config.field_name
            source_field_value = source_record.get(source_field_name, None)
            remote_field_value = remote_record["fields"].get(field_config.field_name, None)
            if source_field_value == remote_field_value:
                continue

            if field_config.update_type == FieldUpdateType.always or (
                    field_config.update_type == FieldUpdateType.if_empty and remote_field_value is None):
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
        :param source_records: The list of records to sync with the Airtable table.
        :return:
        """
        for source_record in source_records:
            for field_config in table_config.fields:

                source_field_name = field_config.source_field_name or field_config.field_name
                source_field_value = source_record.get(source_field_name, None)
                
                if not source_field_value:
                    continue
                    
                if not isinstance(source_field_value, dt.datetime):
                    continue
                    
                source_field_timezone = tz.timezone(field_config.timezone or 'UTC')
                # Apply the source field timezone to the source field value
                source_field_value = source_field_timezone.localize(source_field_value).astimezone(tz.UTC)
                source_record[source_field_name] = source_field_value.strftime("%Y-%m-%dT%H:%M:%S") + '.000Z'                
            