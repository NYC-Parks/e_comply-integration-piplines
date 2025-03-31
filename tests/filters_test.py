import pytest
import filters
from datetime import datetime
from pandas import DataFrame, Series


# Fixtures
@pytest.fixture
def sample_dataframe():
    return DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [10, 20, 30],
        }
    )


@pytest.fixture
def sample_series():
    return Series([1, 2, 3])


# Test exception_handler
def test_exception_handler():
    with pytest.raises(Exception) as exc_info:
        filters.exception_handler(ValueError("test error"))
    assert "function" in str(exc_info.value)
    assert "inner" in str(exc_info.value)


# Test epoch_to_datetime
@pytest.mark.parametrize(
    "epoch,expected",
    [
        (1609459200000, datetime(2020, 12, 31, 19, 0)),
        (0, datetime(1969, 12, 31, 19, 0)),
    ],
)
def test_epoch_to_local_datetime(epoch, expected):
    result = filters.epoch_to_local_datetime(epoch)
    assert result == expected


# Test to_json
def test_to_json_dataframe(sample_dataframe):
    result = filters.to_json(sample_dataframe)
    assert isinstance(result, str)
    assert '"id":1' in result
    assert '"name":"A"' in result


def test_to_json_dict():
    data = {"key": "value"}
    result = filters.to_json(data)
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
    result = filters.join(input_data, with_quotes)
    assert result == expected


# Test filter_Nones
def test_filter_Nones():
    df = DataFrame({"id": [1, 2, None, 4], "value": [10, 20, 30, 40]})
    result = filters.filter_Nones(df, "id")
    assert len(result) == 3
    assert None not in result["id"].values


# Test update_df
def test_update_df(sample_dataframe):
    source = DataFrame({"id": [1, 3], "new_value": [100, 300]})

    result = filters.update_df(sample_dataframe, source, "id", {"value": "new_value"})
    
    assert result.at[0, "value"] == 100
    assert result.at[1, "value"] == 20
    assert result.at[2, "value"] == 300


def test_update_df_with_array_key():
    dest = DataFrame({"key": [[1, 2], [3, 4]], "value": ["A", "B"]})
    source = DataFrame({"key": [[1, 2], [5, 6]], "new_value": ["X", "Y"]})

    result = filters.update_df(dest, source, "key", {"value": "new_value"})

    assert result.at[0, "value"] == "X"


# Test pipeline
def test_pipeline_success():
    def func1(ctx):
        ctx["step1"] = True
        return ctx

    def func2(ctx):
        ctx["step2"] = True
        return ctx

    context = {"initial": True}
    result = filters.pipeline(context, func1, func2)

    assert result["initial"] is True
    assert result["step1"] is True
    assert result["step2"] is True


def test_pipeline_early_termination():
    def func1(ctx):
        return None

    def func2(ctx):
        pytest.fail("Should not be called")

    context = {"initial": True}
    result = filters.pipeline(context, func1, func2)

    assert result["initial"] is True
