from datetime import datetime
from pandas import DataFrame, Series
import pytest
import filters


def sample_dataframe():
    return DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [10, 20, 30],
        }
    )


# Test exception_handler
def test_exception_handler():
    # Arrange
    test_error = ValueError("test error")

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        filters.exception_handler(test_error)

    error_dict = str(exc_info.value)
    assert "function" in error_dict
    assert "inner" in error_dict


# Test epoch_to_datetime
@pytest.mark.parametrize(
    "epoch,expected",
    [
        (1609459200000, datetime(2020, 12, 31, 19, 0)),
        (0, datetime(1969, 12, 31, 19, 0)),
    ],
)
def test_epoch_to_local_datetime(epoch, expected):
    # Act
    result = filters.epoch_to_local_datetime(epoch)

    # Assert
    assert result == expected


# Test to_json
def test_to_json_dataframe():
    # Arrange
    dataframe = sample_dataframe()

    # Act
    result = filters.to_json(dataframe)

    # Assert
    assert isinstance(result, str)
    assert all(f'"{k}"' in result for k in ["id", "name", "value"])
    assert '"id":1' in result


def test_to_json_dict():
    # Arrange
    test_dict = {"key": "value"}

    # Act
    result = filters.to_json(test_dict)

    # Assert
    assert result == '{"key": "value"}'


# Test join
@pytest.mark.parametrize(
    "input_data,with_quotes,expected",
    [
        ([1, 2, 3], False, "1,2,3"),
        ([1, 2, 3], True, "'1','2','3'"),
        (["a", "b", "c"], False, "a,b,c"),
        (Series([1, 2, 3]), False, "1,2,3"),
    ],
)
def test_join(input_data, with_quotes, expected):
    # Act
    result = filters.join(input_data, with_quotes)

    # Assert
    assert result == expected


def test_join_empty_list():
    # Arrange
    empty_list = []

    # Act
    result = filters.join(empty_list)

    # Assert
    assert result == ""


def test_join_mixed_types():
    # Arrange
    mixed_list = [1, "two", 3.0, True]

    # Act
    result = filters.join(mixed_list)

    # Assert
    assert result == "1,two,3.0,True"


# Test filter_Nones
def test_filter_Nones():
    # Arrange
    test_df = DataFrame({"id": [1, 2, None, 4], "value": [10, 20, 30, 40]})

    # Act
    result = filters.filter_Nones(test_df, "id")

    # Assert
    assert len(result) == 3
    assert result["id"].notna().all()


# Test update_df
def test_update_df():
    # Arrange
    dest = sample_dataframe()
    source = DataFrame({"id": [1, 3], "new_value": [100, 300]})

    # Act
    result = filters.update_df(dest, source, "id", {"value": "new_value"})

    # Assert
    assert result.at[0, "value"] == 100
    assert result.at[1, "value"] == 20
    assert result.at[2, "value"] == 300


def test_update_df_with_array_key():
    # Arrange
    dest = DataFrame({"key": [[1, 2], [3, 4]], "value": ["A", "B"]})
    source = DataFrame({"key": [[1, 2], [5, 6]], "new_value": ["X", "Y"]})

    # Act
    result = filters.update_df(dest, source, "key", {"value": "new_value"})

    # Assert
    assert result.at[0, "value"] == "X"


def test_update_df_empty_source_error():
    # Arrange
    dest = DataFrame({"id": [1, 2], "value": ["A", "B"]})
    source = DataFrame({"id": [], "new_value": []})

    # Act
    with pytest.raises(Exception) as exc_info:
        filters.update_df(dest, source, "id", {"value": "new_value"})

    # Assert
    assert isinstance(exc_info.value, KeyError)


def test_update_df_all_columns_mapped():
    # Arrange
    dest = DataFrame({"id": [1, 2], "col1": ["A", "B"], "col2": [10, 20]})
    source = DataFrame({"id": [1], "new_col1": ["X"], "new_col2": [30]})

    # Act
    result = filters.update_df(
        dest, source, "id", {"col1": "new_col1", "col2": "new_col2"}
    )

    # Assert
    assert result.at[0, "col1"] == "X"
    assert result.at[0, "col2"] == 30
    assert result.at[1, "col1"] == "B"
    assert result.at[1, "col2"] == 20


# Test pipeline
def test_pipeline_success():
    # Arrange
    def step1(ctx):
        ctx["step1"] = True
        return ctx

    def step2(ctx):
        ctx["step2"] = True
        return ctx

    initial_context = {"initial": True}

    # Act
    result = filters.pipeline(initial_context, step1, step2)

    # Assert
    assert all(key in result for key in ["initial", "step1", "step2"])
    assert all(result[key] is True for key in result)


def test_pipeline_early_termination():
    # Arrange
    def failing_step(ctx):
        return None

    def unreached_step(ctx):
        pytest.fail("Should not be called")

    initial_context = {"initial": True}

    # Act
    result = filters.pipeline(initial_context, failing_step, unreached_step)

    # Assert
    assert result["initial"] is True


def test_pipeline_no_functions():
    # Arrange
    context = {"test": "data"}

    # Act & Assert
    with pytest.raises(ValueError) as exc_info:
        filters.pipeline(context)
    assert "At least one function must be provided" in str(exc_info.value)


def test_pipeline_invalid_function():
    # Arrange
    context = {"test": "data"}
    invalid_func = "not_a_function"

    # Act
    with pytest.raises(TypeError) as exc_info:
        filters.pipeline(context, invalid_func)

    # Assert
    assert "Expected a callable" in str(exc_info.value)
