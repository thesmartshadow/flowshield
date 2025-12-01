from flowshield.schema import FeatureSchema, FeatureColumn
from flowshield.exceptions import SchemaError


def test_schema_from_columns_unique():
    schema = FeatureSchema.from_columns(["a", "b"], defaults={"dtype": "float"})
    assert schema.column_names() == ["a", "b"]


def test_schema_min_gt_max():
    try:
        FeatureColumn(name="a", dtype="float", minimum=5, maximum=1)
    except SchemaError:
        assert True
    else:
        assert False
