from unittest.mock import MagicMock, patch
import pytest
import filters
from datetime import datetime
from pandas import DataFrame, Series


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


# Common Function Tests
def test_apply_edits():
    # Arrange
    repo = MagicMock()
    repo.apply_edits.return_value = {}
    test_data = DataFrame({"id": [1, 2], "value": ["A", "B"]})
    context = {"repo": repo, "deltas": {1: {"adds": test_data}}}

    # Act
    result = filters.apply_edits(context)

    # Assert
    repo.apply_edits.assert_called_once()
    assert result == context


def test_apply_edits_with_no_deltas():
    # Arrange
    repo = MagicMock()
    context = {"repo": repo, "deltas": {}}

    # Act
    result = filters.apply_edits(context)

    # Assert
    assert result == context
    repo.apply_edits.assert_not_called()


@patch("filters.extract_changes")
def test_extract_changes_success(mock_extract):
    # Arrange
    server = MagicMock()
    expected_changes = {"changes": DataFrame({"id": [1, 2]}), "server_gen": 12345}
    mock_extract.return_value = expected_changes

    # Act
    result = filters.extract_changes(
        server, layer_id=1, server_gen=1000, out_fields=["field1", "field2"]
    )

    # Assert
    assert result == expected_changes
    mock_extract.assert_called_once()


@patch("filters.extract_changes")
def test_extract_changes_no_changes(mock_extract):
    # Arrange
    server = MagicMock()
    mock_extract.return_value = {"changes": None, "server_gen": 12345}

    # Act
    result = filters.extract_changes(
        server, layer_id=1, server_gen=1000, out_fields=["field1"]
    )

    # Assert
    assert result["changes"] is None
    assert result["server_gen"] == 12345


@patch("filters.Server")
def test_extract_changes_empty_changes(mock_server):
    # Arrange
    mock_server.extract_changes.return_value = {
        "layerServerGens": [{"serverGen": 1000}],
        "edits": [{"objectIds": {"adds": [], "updates": []}}],
    }

    # Act
    result = filters.extract_changes(mock_server, 1, 500, ["field1"])

    # Assert
    assert result["changes"] is None
    assert result["server_gen"] == 1000


@patch("filters.Server")
def test_extract_changes_error_handling(mock_server):
    # Arrange
    mock_server.extract_changes.side_effect = Exception("Network error")

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        filters.extract_changes(mock_server, 1, 500, ["field1"])
    assert "extract_changes: Network error" in str(exc_info.value)


# Test seperate_changes
def test_seperate_changes():
    # Arrange
    test_changes = [
        {"objectId": None, "value": "A"},
        {"objectId": 1, "value": "B"},
        {"objectId": 2, "value": "C"},
    ]

    # Act
    result = filters.seperate_changes(test_changes)

    # Assert
    assert isinstance(result, dict)
    assert all(key in result for key in ["adds", "updates"])
    assert len(result["adds"]) == 1
    assert len(result["updates"]) == 2
    assert "objectId" not in result["adds"].columns


def test_seperate_changes_empty_list():
    # Arrange
    changes = []

    # Act & Assert
    with pytest.raises(IndexError):
        filters.seperate_changes(changes)


def test_seperate_changes_all_adds():
    # Arrange
    changes = [
        {"objectId": None, "value": "A"},
        {"objectId": None, "value": "B"},
    ]

    # Act
    result = filters.seperate_changes(changes)

    # Assert
    assert "adds" in result
    assert "updates" not in result
    assert len(result["adds"]) == 2


def test_seperate_changes_all_updates():
    # Arrange
    changes = [
        {"objectId": 1, "value": "A"},
        {"objectId": 2, "value": "B"},
    ]

    # Act
    result = filters.seperate_changes(changes)

    # Assert
    assert "updates" in result
    assert "adds" not in result
    assert len(result["updates"]) == 2


# Test set_deltas error cases
def test_set_deltas_missing_layer_id():
    # Arrange
    context = {}
    data = DataFrame({"col1": [1, 2]})

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        filters.set_deltas(context, data)
    assert "layer_id is required" in str(exc_info.value)


# Test query_domains
def test_query_domains():
    # Arrange
    repo = MagicMock()
    test_context = {
        "repo": repo,
        "layerId": 1,
        "domainNames": ["domain1", "domain2"],
    }
    expected_response = [
        {"name": "domain1", "codedValues": [{"code": 1, "name": "Value1"}]}
    ]
    repo.query_domains.return_value = expected_response

    # Act
    result = filters.query_domains(test_context)

    # Assert
    assert "domainValues" in result
    assert result["domainValues"] == expected_response
    repo.query_domains.assert_called_once()


# Test post_domains
def test_post_domains(mock_service):
    # Arrange
    test_context = {
        "service": mock_service,
        "domainValues": [
            {"name": "domain1", "codedValues": [{"code": 1, "name": "Value1"}]}
        ],
    }
    mock_service.post_domain_values.return_value = "Success"

    # Act
    result = filters.post_domains(test_context)

    # Assert
    assert result["output"] == "Domain Values result: Success"
    mock_service.post_domain_values.assert_called_once()
