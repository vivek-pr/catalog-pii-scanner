from __future__ import annotations

from typing import Any

from catalog_pii_scanner.connectors.glue import _table_to_input


def test_table_to_input_strips_read_only_and_unknown_keys() -> None:
    tbl: dict[str, Any] = {
        "Name": "users",
        "DatabaseName": "demo",
        "CreateTime": "2024-08-01T00:00:00Z",
        "UpdateTime": "2024-08-02T00:00:00Z",
        "LastAccessTime": "2024-08-02T00:00:00Z",
        "LastAnalyzedTime": "2024-08-02T00:00:00Z",
        "TableArn": "arn:aws:glue:...",
        "TableId": "abc-123",
        "FederatedTable": {"Identifier": "x"},
        "CatalogId": "111111111111",
        "VersionId": "1",
        "Parameters": {"a": "b"},
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": [
                {"Name": "email", "Type": "string", "Comment": "user email", "Foo": "bar"},
                {"Name": "age", "Type": "int", "Baz": 1},
            ],
            "Location": "s3://bucket/prefix",
            "SerdeInfo": {"Name": "hive", "Foo": "x", "SerializationLibrary": "org..."},
            "SortColumns": [{"Column": "age", "SortOrder": 1, "Extra": 9}],
            "SkewedInfo": {"SkewedColumnNames": ["x"], "Unknown": True},
            "UnknownKey": True,
        },
    }

    ti = _table_to_input(tbl)

    # Disallowed top-level keys should be removed
    for k in [
        "DatabaseName",
        "CreateTime",
        "UpdateTime",
        "LastAccessTime",
        "LastAnalyzedTime",
        "TableArn",
        "TableId",
        "FederatedTable",
        "CatalogId",
        "VersionId",
    ]:
        assert k not in ti

    # Allowed basic fields should remain
    assert ti["Name"] == "users"
    assert ti["TableType"] == "EXTERNAL_TABLE"
    assert ti["Parameters"] == {"a": "b"}

    sd = ti["StorageDescriptor"]
    # Unknown keys removed
    assert "UnknownKey" not in sd
    # Columns sanitized
    cols = sd["Columns"]
    assert cols[0] == {"Name": "email", "Type": "string", "Comment": "user email"}
    assert cols[1] == {"Name": "age", "Type": "int"}
    # Serde sanitized
    assert set(sd["SerdeInfo"].keys()) <= {"Name", "SerializationLibrary", "Parameters"}
    # SortColumns sanitized
    assert sd["SortColumns"][0] == {"Column": "age", "SortOrder": 1}
    # SkewedInfo sanitized
    assert set(sd["SkewedInfo"].keys()) <= {
        "SkewedColumnNames",
        "SkewedColumnValues",
        "SkewedColumnValueLocationMaps",
    }
