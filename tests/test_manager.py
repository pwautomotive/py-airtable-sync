import datetime as dt

from py_airtable_sync import TableConfig, FieldConfig, AirtableManager


def test_clean_date_formats_returns_expected_format():
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
    assert "2021-01-01T10:00:00.000Z" == test_source_records[0]["date_field"]


def test_clean_date_formats_returns_expected_format_with_timezone():
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
    assert "2021-01-01T10:00:00.000Z" == test_source_records[0]["date_field"]
