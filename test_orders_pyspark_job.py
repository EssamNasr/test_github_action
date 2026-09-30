import pytest
from pyspark.sql import SparkSession
from orders import clean_data


@pytest.fixture(scope="module")
def spark_local():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-customer-orders")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_clean_data_removes_zero_and_negative_values(spark_local):
    data = [
        ("1001", "2026-09-01", "Alice", "150.00"),
        ("1002", "2026-09-02", "Bob", "-20.00"),
        ("1003", "2026-09-03", "Sara", "0"),
    ]

    df = spark_local.createDataFrame(
        data,
        ["order_id", "order_Date", "customer_name", "value"]
    )

    transformed_df = clean_data(df)

    results = transformed_df.collect()

    assert len(results) == 1
    assert results[0]["order_id"] == "1001"
    assert results[0]["customer_name"] == "Alice"
    assert results[0]["value"] == 150.0