from jobs.user_activity_job import do_user_activity_transformation
from collections import namedtuple
from datetime import datetime

UserEvent = namedtuple("UserEvent", "user_id event_time host url")


def test_user_activity_aggregation(spark):
    # Create test input data
    input_data = [
        UserEvent(user_id=1, event_time=datetime(2023, 1, 15, 10, 0, 0), host="example.com", url="/"),
        UserEvent(user_id=1, event_time=datetime(2023, 1, 20, 11, 0, 0), host="example.com", url="/about"),
        UserEvent(user_id=1, event_time=datetime(2023, 1, 25, 12, 0, 0), host="other.com", url="/"),
        UserEvent(user_id=2, event_time=datetime(2023, 1, 10, 13, 0, 0), host="example.com", url="/contact"),
        UserEvent(user_id=2, event_time=datetime(2023, 2, 5, 14, 0, 0), host="example.com", url="/"),
        # Test NULL user_id gets filtered out
        UserEvent(user_id=None, event_time=datetime(2023, 1, 1, 15, 0, 0), host="example.com", url="/"),
    ]

    source_df = spark.createDataFrame(input_data)
    actual_df = do_user_activity_transformation(spark, source_df)

    print("✅ Query executed successfully!")
    actual_df.show()

    # Collect results for validation
    results = actual_df.collect()

    # Basic validations
    assert len(results) == 3, f"Expected 3 rows, got {len(results)}"

    # Find user 1's result for January
    user1_jan = [r for r in results if r.user_id == 1 and r.month_start.month == 1][0]
    assert user1_jan.total_events == 3, f"User 1 should have 3 events, got {user1_jan.total_events}"
    assert user1_jan.unique_hosts == 2, f"User 1 should have 2 unique hosts, got {user1_jan.unique_hosts}"

    # Validate homepage ratio is approximately correct (2/3 ≈ 0.66667)
    assert abs(float(
        user1_jan.homepage_ratio) - 0.66667) < 0.001, f"Homepage ratio should be ~0.66667, got {user1_jan.homepage_ratio}"

    # Find user 2's results
    user2_jan = [r for r in results if r.user_id == 2 and r.month_start.month == 1][0]
    user2_feb = [r for r in results if r.user_id == 2 and r.month_start.month == 2][0]

    # Validate user 2 January
    assert user2_jan.total_events == 1, f"User 2 Jan should have 1 event, got {user2_jan.total_events}"
    assert user2_jan.unique_hosts == 1, f"User 2 Jan should have 1 unique host, got {user2_jan.unique_hosts}"
    assert abs(float(
        user2_jan.homepage_ratio) - 0.0) < 0.001, f"User 2 Jan homepage ratio should be 0, got {user2_jan.homepage_ratio}"

    # Validate user 2 February
    assert user2_feb.total_events == 1, f"User 2 Feb should have 1 event, got {user2_feb.total_events}"
    assert user2_feb.unique_hosts == 1, f"User 2 Feb should have 1 unique host, got {user2_feb.unique_hosts}"
    assert abs(float(
        user2_feb.homepage_ratio) - 1.0) < 0.001, f"User 2 Feb homepage ratio should be 1, got {user2_feb.homepage_ratio}"

    # Verify required columns exist
    expected_columns = ["user_id", "month_start", "total_events", "unique_hosts", "homepage_ratio"]
    actual_columns = actual_df.columns

    for col in expected_columns:
        assert col in actual_columns, f"Missing column: {col}"

    print("✅ All validations passed!")
    print(
        f"✅ User 1 January: {user1_jan.total_events} events, {user1_jan.unique_hosts} hosts, {user1_jan.homepage_ratio} ratio")
    print(
        f"✅ User 2 January: {user2_jan.total_events} events, {user2_jan.unique_hosts} hosts, {user2_jan.homepage_ratio} ratio")
    print(
        f"✅ User 2 February: {user2_feb.total_events} events, {user2_feb.unique_hosts} hosts, {user2_feb.homepage_ratio} ratio")
    print("✅ PostgreSQL to PySpark conversion test PASSED!")