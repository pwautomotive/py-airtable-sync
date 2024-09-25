import datetime as dt
import unittest
from time import timezone
from typing import List
from unittest.mock import MagicMock, patch

from pyairtable.api.types import RecordDict

from py_airtable_sync import AirtableManager, AirtableBaseConfig, TableConfig, FieldConfig, FieldUpdateType, \
    ForeignKeyUpdateType, SourceRecordList, SourceRecord

SOURCE_TEST_TABLE_RECORD_NO_UPDATE: SourceRecord = {
    "primary_field": "p1",
    "never_field": "a",
    "always_field": "a",
    "if_empty_field": "a",
    "foreign_field": "fk1"
}

SOURCE_TEST_TABLE_RECORD_UPDATE_NEVER: SourceRecord = {
    "primary_field": "p2",
    "never_field": "b",
    "always_field": "a",
    "if_empty_field": "a",
    "foreign_field": "fk1"
}

SOURCE_TEST_TABLE_RECORD_UPDATE_ALWAYS: SourceRecord = {
    "primary_field": "p3",
    "never_field": "a",
    "always_field": "b",
    "if_empty_field": "a",
    "foreign_field": "fk1"
}

SOURCE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_EMPTY: SourceRecord = {
    "primary_field": "p4",
    "never_field": "a",
    "always_field": "a",
    "if_empty_field": "b",
    "foreign_field": "fk1"
}

SOURCE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_NOT_EMPTY: SourceRecord = {
    "primary_field": "p5",
    "never_field": "a",
    "always_field": "a",
    "if_empty_field": "b",
    "foreign_field": "fk1"
}

SOURCE_TEST_TABLE_RECORD_UPDATE_FOREIGN_EXISTS: SourceRecord = {
    "primary_field": "p6",
    "never_field": "a",
    "always_field": "a",
    "if_empty_field": "b",
    "foreign_field": "fk2"
}

SOURCE_TEST_TABLE_RECORD_UPDATE_FOREIGN_NOT_EXISTS: SourceRecord = {
    "primary_field": "p7",
    "never_field": "a",
    "always_field": "a",
    "if_empty_field": "b",
    "foreign_field": "fk3"
}

SOURCE_TEST_TABLE_RECORD_INSERT: SourceRecord = {
    "primary_field": "p8",
    "never_field": "a",
    "always_field": "a",
    "if_empty_field": "a",
    "foreign_field": "fk1"
}

SOURCE_TEST_TABLE_RECORDS: SourceRecordList = [
    SOURCE_TEST_TABLE_RECORD_NO_UPDATE,
    SOURCE_TEST_TABLE_RECORD_UPDATE_NEVER,
    SOURCE_TEST_TABLE_RECORD_UPDATE_ALWAYS,
    SOURCE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_EMPTY,
    SOURCE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_NOT_EMPTY,
    SOURCE_TEST_TABLE_RECORD_UPDATE_FOREIGN_EXISTS,
    SOURCE_TEST_TABLE_RECORD_UPDATE_FOREIGN_NOT_EXISTS,
    SOURCE_TEST_TABLE_RECORD_INSERT,
]

REMOTE_TEST_TABLE_RECORD_NO_UPDATE: RecordDict = {
    "id": "r1",
    "createdTime": "2021-01-01T00:00:00Z",
    "fields": {
        "primary_field": "p1",
        "never_field": "a",
        "always_field": "a",
        "if_empty_field": "a",
        "foreign_field": ["fr1"]
    }
}

REMOTE_TEST_TABLE_RECORD_UPDATE_NEVER: RecordDict = {
    "id": "r2",
    "createdTime": "2021-01-01T00:00:00Z",
    "fields": {
        "primary_field": "p2",
        "never_field": "a",
        "always_field": "a",
        "if_empty_field": "a",
        "foreign_field": ["fr1"]
    }
}

REMOTE_TEST_TABLE_RECORD_UPDATE_ALWAYS: RecordDict = {
    "id": "r3",
    "createdTime": "2021-01-01T00:00:00Z",
    "fields": {
        "primary_field": "p3",
        "never_field": "a",
        "always_field": "a",
        "if_empty_field": "a",
        "foreign_field": ["fr1"]
    }
}

REMOTE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_EMPTY: RecordDict = {
    "id": "r4",
    "createdTime": "2021-01-01T00:00:00Z",
    "fields": {
        "primary_field": "p4",
        "never_field": "a",
        "always_field": "a",
        "foreign_field": ["fr1"]
    }
}

REMOTE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_NOT_EMPTY: RecordDict = {
    "id": "r5",
    "createdTime": "2021-01-01T00:00:00Z",
    "fields": {
        "primary_field": "p5",
        "never_field": "a",
        "always_field": "a",
        "if_empty_field": "a",
        "foreign_field": ["fr1"]
    }
}

REMOTE_TEST_TABLE_RECORD_UPDATE_FOREIGN_EXISTS: RecordDict = {
    "id": "r6",
    "createdTime": "2021-01-01T00:00:00Z",
    "fields": {
        "primary_field": "p6",
        "never_field": "a",
        "always_field": "a",
        "if_empty_field": "a",
        "foreign_field": ["fr1"]
    }
}

REMOTE_TEST_TABLE_RECORD_UPDATE_FOREIGN_NOT_EXISTS: RecordDict = {
    "id": "r7",
    "createdTime": "2021-01-01T00:00:00Z",
    "fields": {
        "primary_field": "p7",
        "never_field": "a",
        "always_field": "a",
        "if_empty_field": "a",
        "foreign_field": ["fr1"]
    }
}

REMOTE_TEST_TABLE_RECORDS: List[RecordDict] = [
    REMOTE_TEST_TABLE_RECORD_NO_UPDATE,
    REMOTE_TEST_TABLE_RECORD_UPDATE_NEVER,
    REMOTE_TEST_TABLE_RECORD_UPDATE_ALWAYS,
    REMOTE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_EMPTY,
    REMOTE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_NOT_EMPTY,
    REMOTE_TEST_TABLE_RECORD_UPDATE_FOREIGN_EXISTS,
    REMOTE_TEST_TABLE_RECORD_UPDATE_FOREIGN_NOT_EXISTS,
]

REMOTE_TEST_FOREIGN_TABLE_RECORDS = [
    {
        "id": "fr1",
        "createdTime": "2021-01-01T00:00:00Z",
        "fields": {
            "primary_field": "p1",
            "foreign_key_field": "fk1",
            "foreign_data_field": "a"
        }
    },
    {
        "id": "fr2",
        "createdTime": "2021-01-01T00:00:00Z",
        "fields": {
            "primary_field": "p2",
            "foreign_key_field": "fk2",
            "foreign_data_field": "a"
        }
    },
]


class TestAirtableManager(unittest.TestCase):

    def setUp(self):
        # Mock the Api class
        self.mock_api = MagicMock()

        # Create a sample configuration
        self.config = AirtableBaseConfig(
            api_key="fake_api_key",
            tables=[
                TableConfig(
                    base_id="test_base",
                    table_id="test_table",
                    table_name="Test Table",
                    fields=[
                        FieldConfig(
                            field_name="primary_field",
                            is_primary=True,
                        ),
                        FieldConfig(
                            field_name="never_field",
                            is_primary=False,
                            update_type=FieldUpdateType.never,
                        ),
                        FieldConfig(
                            field_name="always_field",
                            is_primary=False,
                            update_type=FieldUpdateType.always,
                        ),
                        FieldConfig(
                            field_name="if_empty_field",
                            is_primary=False,
                            update_type=FieldUpdateType.if_empty,
                        ),
                        FieldConfig(
                            field_name="foreign_field",
                            is_primary=False,
                            update_type=FieldUpdateType.always,
                            foreign_table_id="test_foreign_table",
                            foreign_field_name="foreign_key_field",
                            foreign_update_type=ForeignKeyUpdateType.insert,
                        )
                    ]
                ),
                TableConfig(
                    base_id="test_base",
                    table_id="test_foreign_table",
                    table_name="Test Foreign Table",
                    fields=[
                        FieldConfig(
                            field_name="primary_field",
                            is_primary=True,
                        ),
                        FieldConfig(
                            field_name="foreign_key_field",
                            is_primary=False,
                        ),
                        FieldConfig(
                            field_name="foreign_data_field",
                            is_primary=False,
                        )
                    ]
                )
            ]
        )

        # Patch the Api class in the AirtableManager module
        patcher = patch('py_airtable_sync.airtable_manager.Api', return_value=self.mock_api)
        self.addCleanup(patcher.stop)
        self.mock_api_class = patcher.start()

        # Mock table().batch_update()
        def batch_update_side_effect(records):
            self.manager.cache["test_table"].update({record["id"]: record for record in records})

        # Mock table().batch_create()
        def batch_create_side_effect(records):
            new_records = {
                f"r{n + 8}": {
                    "id": f"r{n + 8}",
                    "createdTime": "2021-01-01T00:00:00Z",
                    "fields": record,
                }
                for n, record in enumerate(records)
            }
            self.manager.cache["test_table"].update(new_records)
            print(self.manager.cache["test_table"])

        # Mock table().create()
        def create_side_effect(record):
            new_record_id = "fr3"
            new_record = {
                "id": new_record_id,
                "createdTime": "2021-01-01T00:00:00Z",
                "fields": record,
            }
            new_remote_record = {new_record_id: new_record}
            self.manager.cache["test_foreign_table"].update(new_remote_record)
            return new_record

        # Mock the response from table.all()
        def table_side_effects(base_id, table_id):
            if table_id == "test_table":
                return MagicMock(
                    all=MagicMock(return_value=REMOTE_TEST_TABLE_RECORDS),
                    create=MagicMock(return_value={"id": "new_r"}),
                    batch_update=MagicMock(side_effect=batch_update_side_effect),
                    batch_create=MagicMock(side_effect=batch_create_side_effect))
            elif table_id == "test_foreign_table":
                return MagicMock(
                    all=MagicMock(return_value=REMOTE_TEST_FOREIGN_TABLE_RECORDS),
                    create=MagicMock(side_effect=create_side_effect))
            return MagicMock(all=MagicMock(return_value=[]))

        self.mock_api.table.side_effect = lambda base_id, table_id: table_side_effects(base_id, table_id)

        # Initialize the AirtableManager with the mocked Api
        self.manager = AirtableManager(self.config)

        # Reset the cache to ensure isolation
        self.manager.cache = {
            "test_table": {},
            "test_foreign_table": {}
        }

    def test_get_table_config(self):
        table_config = self.manager.get_table_config("test_table")
        self.assertEqual(table_config.table_id, "test_table")
    

    # def test_get_new_records(self):
    #     # Arrange
    #     source_records = SOURCE_TEST_TABLE_RECORDS
    #     table_config = self.manager.get_table_config("test_table")
    #     self.manager.cache["test_table"] = {record["id"]: record for record in REMOTE_TEST_TABLE_RECORDS}
    #     self.manager.cache["test_foreign_table"] = {record["id"]: record for record in
    #                                                 REMOTE_TEST_FOREIGN_TABLE_RECORDS}
    #     self.manager.populate_foreign_keys(table_config, source_records)
    # 
    #     # Act
    #     new_records = self.manager.get_new_records(table_config, source_records)
    # 
    #     # Assert
    #     self.assertEqual(1, len(new_records))
    # 
    # def test_get_updated_records(self):
    #     # Arrange
    #     source_records = SOURCE_TEST_TABLE_RECORDS
    #     table_config = self.manager.get_table_config("test_table")
    #     self.manager.cache["test_table"] = {record["id"]: record for record in REMOTE_TEST_TABLE_RECORDS}
    #     self.manager.cache["test_foreign_table"] = {record["id"]: record for record in
    #                                                 REMOTE_TEST_FOREIGN_TABLE_RECORDS}
    #     self.manager.populate_foreign_keys(table_config, source_records)
    # 
    #     # Act
    #     updated_record = self.manager.get_updated_records(table_config, source_records)
    # 
    #     # Assert
    #     self.assertEqual(4, len(updated_record))

    def test_get_updated_fields_uses_source_alias(self):
        # Arrange
        table_config = TableConfig(
            base_id="test_base",
            table_id="test_table",
            fields=[
                FieldConfig(
                    field_name="test_field",
                    source_field_name="src_test_field",
                    update_type=FieldUpdateType.always,
                ),
            ]
        )
        source_record = {
            "src_test_field": "b",
        }
        remote_record = {
            "id": "r",
            "createdTime": "2021-01-01T00:00:00Z",
            "fields": {
                "test_field": "a",
            }
        }

        # Act
        updated_fields = self.manager.get_updated_fields(table_config, source_record, remote_record)

        # Assert
        expected_fields = {
            "test_field": "b",
        }
        self.assertEqual(expected_fields, updated_fields)

    def test_get_updated_fields_returns_empty_dict_when_no_fields_require_updating(self):
        # Arrange
        source_record = SOURCE_TEST_TABLE_RECORD_NO_UPDATE
        source_record["foreign_field"] = ["fr1"]
        remote_record = REMOTE_TEST_TABLE_RECORD_NO_UPDATE

        # Act
        table_config = self.manager.get_table_config("test_table")
        updated_fields = self.manager.get_updated_fields(table_config, source_record, remote_record)

        # Assert
        self.assertEqual({}, updated_fields)

    def test_get_updated_fields_does_not_include_never_update_field(self):
        # Arrange
        source_record = SOURCE_TEST_TABLE_RECORD_UPDATE_NEVER
        source_record["foreign_field"] = ["fr1"]
        remote_record = REMOTE_TEST_TABLE_RECORD_UPDATE_NEVER

        # Act
        table_config = self.manager.get_table_config("test_table")
        updated_fields = self.manager.get_updated_fields(table_config, source_record, remote_record)

        # Assert
        self.assertEqual({}, updated_fields)

    def test_get_updated_fields_includes_always_update_field(self):
        # Arrange
        source_record = SOURCE_TEST_TABLE_RECORD_UPDATE_ALWAYS
        source_record["foreign_field"] = ["fr1"]
        remote_record = REMOTE_TEST_TABLE_RECORD_UPDATE_ALWAYS

        # Act
        table_config = self.manager.get_table_config("test_table")
        updated_fields = self.manager.get_updated_fields(table_config, source_record, remote_record)

        # Assert
        self.assertEqual({"always_field": "b"}, updated_fields)

    def test_get_updated_fields_does_not_include_if_empty_when_not_empty(self):
        # Arrange
        source_record = SOURCE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_NOT_EMPTY
        source_record["foreign_field"] = ["fr1"]
        remote_record = REMOTE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_NOT_EMPTY

        # Act
        table_config = self.manager.get_table_config("test_table")
        updated_fields = self.manager.get_updated_fields(table_config, source_record, remote_record)

        # Assert
        self.assertEqual({}, updated_fields)

    def test_get_updated_fields_includes_if_empty_when_empty(self):
        # Arrange
        source_record = SOURCE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_EMPTY
        source_record["foreign_field"] = ["fr1"]
        remote_record = REMOTE_TEST_TABLE_RECORD_UPDATE_IF_EMPTY_IS_EMPTY

        # Act
        table_config = self.manager.get_table_config("test_table")
        updated_fields = self.manager.get_updated_fields(table_config, source_record, remote_record)

        # Assert
        self.assertEqual({"if_empty_field": "b"}, updated_fields)

    def test_populate_foreign_keys(self):
        # Use test source records 2 and 3
        source_records = [
            {"id": "rec1", "fields": {"field1": "value1"}},
            {"id": "rec2", "fields": {"field1": "value2"}}
        ]

        self.manager.populate_foreign_key = MagicMock()
        self.manager.populate_foreign_keys(self.config.tables[0], source_records)
        self.assertEqual(self.manager.populate_foreign_key.call_count, 2)

    # def test_populate_foreign_key_existing(self):
    #     # Arrange
    #     field_config = self.config.tables[0].fields[4]
    #     source_record = SOURCE_TEST_TABLE_RECORDS[1]
    #     self.manager.cache["test_foreign_table"] = {record["id"]: record for record in
    #                                                 REMOTE_TEST_FOREIGN_TABLE_RECORDS}
    # 
    #     # Act
    #     self.manager.populate_foreign_key(field_config, source_record)
    # 
    #     # Assert
    #     self.assertEqual(["fr1"], source_record["foreign_field"])

    def test_populate_foreign_key_insert(self):
        # Arrange
        field_config = self.config.tables[0].fields[4]
        source_record = SOURCE_TEST_TABLE_RECORD_UPDATE_FOREIGN_NOT_EXISTS
        self.manager.cache["test_foreign_table"] = {record["id"]: record for record in
                                                    REMOTE_TEST_FOREIGN_TABLE_RECORDS}

        # Act
        self.manager.populate_foreign_key(field_config, source_record)

        # Assert
        self.assertEqual(["fr3"], source_record["foreign_field"])

    def test_clean_date_formats_returns_expected_format(self):
        # Arrange
        table_config = TableConfig(
            base_id="test_base",
            table_id="test_table",
            fields=[
                FieldConfig(
                    field_name="date_field",
                )
            ]
        )
        test_source_records = [
            {"date_field": dt.datetime(2021, 1, 1, 10, 0, 0)}
        ]

        # Act
        AirtableManager.clean_date_formats(table_config, test_source_records)

        # Assert
        self.assertEqual("2021-01-01T10:00:00.000Z", test_source_records[0]["date_field"])
        
    def test_clean_date_formats_returns_expected_format_with_timezone(self):
        # Arrange
        table_config = TableConfig(
            base_id="test_base",
            table_id="test_table",
            fields=[
                FieldConfig(
                    field_name="date_field",
                    timezone="Pacific/Guam"
                )
            ]
        )
        test_source_records = [
            {"date_field": dt.datetime(2021, 1, 1, 20, 0, 0)}
        ]

        # Act
        AirtableManager.clean_date_formats(table_config, test_source_records)

        # Assert
        self.assertEqual("2021-01-01T10:00:00.000Z", test_source_records[0]["date_field"])
        


if __name__ == "__main__":
    unittest.main()
