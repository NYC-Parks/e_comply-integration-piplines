from json import dumps, loads
from unittest.mock import MagicMock
from numpy import NaN
from pandas import DataFrame
import pytest
import filters


@pytest.fixture
def mock_repo():
    return MagicMock()


@pytest.fixture
def mock_service():
    return MagicMock()


# Common Function Tests
def test_apply_edits(mock_repo):
    # Arrange
    mock_repo.apply_edits.return_value = {}
    test_data = DataFrame({"id": [1, 2], "value": ["A", "B"]})
    context = {"repo": mock_repo, "deltas": {1: {"adds": test_data}}}

    # Act
    result = filters.apply_edits(context)

    # Assert
    mock_repo.apply_edits.assert_called_once()
    assert result == context


def test_apply_edits_with_no_deltas(mock_repo):
    # Arrange
    context = {"repo": mock_repo, "deltas": {}}

    # Act
    result = filters.apply_edits(context)

    # Assert
    assert result == context
    mock_repo.apply_edits.assert_not_called()


def test_extract_changes_success(mock_repo):
    # Arrange
    mock_repo.extract_changes.return_value = {
        "edits": [{"objectIds": {"adds": [1, 2], "updates": [3]}}],
        "layerServerGens": [{"serverGen": 12345}],
    }
    changes = {
        "changes": DataFrame(
            [
                {"objectID": 1, "field1": "A", "field2": "B"},
                {"objectID": 2, "field1": "A", "field2": "B"},
                {"objectID": 3, "field1": "A", "field2": "B"},
            ]
        )
    }
    mock_repo.query.return_value = {1: changes}

    # Act
    result = filters.extract_changes(
        mock_repo,
        layer_id=1,
        server_gen=1000,
        out_fields=["field1", "field2"],
    )

    # Assert
    mock_repo.extract_changes.assert_called_once()
    mock_repo.query.assert_called_once()
    assert result == {"changes": changes, "server_gen": 12345}


def test_extract_changes_no_changes(mock_repo):
    # Arrange
    mock_repo.extract_changes.return_value = {
        "edits": [{"objectIds": {"adds": [], "updates": []}}],
        "layerServerGens": [{"serverGen": 12345}],
    }

    # Act
    result = filters.extract_changes(
        mock_repo,
        layer_id=1,
        server_gen=1000,
        out_fields=["field1"],
    )

    # Assert
    mock_repo.extract_changes.assert_called_once()
    mock_repo.query.assert_not_called()
    assert result == {"changes": None, "server_gen": 12345}


def test_extract_changes_empty_changes(mock_repo):
    # Arrange
    mock_repo.extract_changes.return_value = {
        "layerServerGens": [{"serverGen": 1000}],
        "edits": [{"objectIds": {"adds": [], "updates": []}}],
    }

    # Act
    result = filters.extract_changes(mock_repo, 1, 500, ["field1"])

    # Assert
    assert result["changes"] is None
    assert result["server_gen"] == 1000


def test_extract_changes_error_handling(mock_repo):
    # Arrange
    mock_repo.extract_changes.side_effect = Exception("Network error")

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        filters.extract_changes(mock_repo, 1, 500, ["field1"])
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
    result = filters._seperate_changes(DataFrame(test_changes))

    # Assert
    assert isinstance(result, dict)
    assert all(key in result for key in ["adds", "updates"])
    assert len(result["adds"]) == 1
    assert len(result["updates"]) == 2
    assert "objectId" not in result["adds"].columns


def test_seperate_changes_all_adds():
    # Arrange
    changes = [
        {"objectId": NaN, "value": "A"},
        {"objectId": NaN, "value": "B"},
    ]

    # Act
    result = filters._seperate_changes(DataFrame(changes))

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
    result = filters._seperate_changes(DataFrame(changes))

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
def test_query_domains(mock_repo):
    # Arrange
    test_context = {
        "repo": mock_repo,
        "layerId": 1,
        "domainNames": ["domain1", "domain2"],
    }
    expected_response = [
        {"name": "domain1", "codedValues": [{"code": 1, "name": "Value1"}]}
    ]
    mock_repo.query_domains.return_value = expected_response

    # Act
    result = filters.query_domains(test_context)

    # Assert
    assert "domainValues" in result
    assert result["domainValues"] == expected_response
    mock_repo.query_domains.assert_called_once()


# Test post_domains
def test_post_domains(mock_repo):
    # Arrange
    test_context = {
        "service": mock_repo,
        "domainValues": [
            {"name": "domain1", "codedValues": [{"code": 1, "name": "Value1"}]}
        ],
    }
    mock_repo.post_domain_values.return_value = "Success"

    # Act
    result = filters.post_domains(test_context)

    # Assert
    assert result["output"] == "Domain Values result: Success"
    mock_repo.post_domain_values.assert_called_once()
