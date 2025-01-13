from enum import Enum
from typing import Optional, List

from pydantic import BaseModel, ConfigDict


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
        if_empty_overrides (list[str] | None): The list of values that should always be updated. Defaults to None.
        foreign_table_id (Optional[str]): The ID of the foreign table if the field is a foreign key. Only works when update_type is `is_empty`. Defaults to None.
        foreign_field_name (Optional[str]): The name of the field in the foreign table that this field references. Defaults to None.
        foreign_update_type (Optional[ForeignKeyUpdateType]): The type of update allowed for the foreign key. Defaults to ForeignKeyUpdateType.ignore.
        source_field_name (Optional[str]): The name of the field in the source record that corresponds to this field. Defaults to None.
    """
    model_config = ConfigDict(extra="allow")

    field_name: str
    is_primary: bool = False
    update_type: Optional[FieldUpdateType] = FieldUpdateType.never
    if_empty_overrides: list[str] | None = None
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
        tables (List[TableConfig]): The list of table configurations for the base.
        verify_ssl (Optional[bool]): Indicates if SSL verification should be performed. Defaults to True.
        cache_max_age (int): The maximum age of cache entries in seconds. Defaults to 3600 (1-hour).
    """
    model_config = ConfigDict(extra="allow")

    verify_ssl: Optional[bool] = True

    tables: List[TableConfig]
    
    cache_max_age: int = 3600
