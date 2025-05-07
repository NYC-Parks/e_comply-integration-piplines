from unittest.mock import MagicMock, patch
from pandas import DataFrame
import pytest
import filters


@pytest.fixture
def mock_repo():
    return MagicMock()


@pytest.fixture
def mock_service():
    return MagicMock()


# Contract Tests
# def test_contract_get_edits(mock_service):
#     # Arrange
#     mock_service.get_contracts.return_value = [
#         {"objectId": 1, "value": "A"},
#         {"objectId": 2, "value": "B"},
#     ]
#     context = {
#         "service": mock_service,
#         "server_gens": DataFrame({"Contract": [1609459200000]}),
#     }

#     # Act
#     result = filters.contract_get_edits(context)

#     # Assert
#     assert "deltas" in result
#     assert 1 in result["deltas"]  # layer_id = 1
#     mock_service.get_contracts.assert_called_once()


def test_contract_get_edits_no_changes(mock_service):
    # Arrange
    mock_service.get_contracts.return_value = []
    context = {
        "service": mock_service,
        "server_gens": DataFrame({"Contract": [1609459200000]}),
    }

    # Act
    result = filters.contract_get_edits(context)

    # Assert
    assert result is None
    assert context["output"] == "No Contract Changes."


# Work Order Tests
# @patch("filters.extract_changes")
# def test_work_order_extract_changes(stub_extract_changes, mock_repo):
#     # Arrange
#     mock_repo.query.return_value = {1: DataFrame({"ContractName": ["C1", "C2"]})}
#     context = {
#         "repo": mock_repo,
#         "server_gens_repo": mock_repo,
#         "server_gens": DataFrame({"WorkOrder": [1609459200000]}),
#         "layer_id": 1,
#     }
#     stub_extract_changes.return_value = {
#         "changes": DataFrame({"id": [1, 2]}),
#         "server_gen": 12345,
#     }

#     # Act
#     result = filters.work_order_extract_changes(context)

#     # Assert
#     assert "deltas" in result
#     assert 0 in result["deltas"]  # layer_id = 0
#     assert context["server_gens"].at[0, "WorkOrder"] == 12345


@patch("filters.extract_changes")
def test_work_order_extract_changes_no_changes(stub_extract_changes, mock_repo):
    # Arrange
    mock_repo.query.return_value = {1: DataFrame({"ContractName": ["C1", "C2"]})}
    context = {
        "repo": mock_repo,
        "server_gens_repo": mock_repo,
        "server_gens": DataFrame({"WorkOrder": [1609459200000]}),
    }
    stub_extract_changes.return_value = {"changes": None, "server_gen": 12345}

    # Act
    result = filters.work_order_extract_changes(context)

    # Assert
    assert result is None
    assert context["output"] == "No Work Order changes."


# def test_wo_query_associated_planting_space_globalid(mock_repo):
#     # Arrange
#     mock_repo.query.return_value = {
#         4: DataFrame(
#             {
#                 "PlantingSpaceGlobalID": ["PS1", "PS2"],
#                 "GlobalID": ["G1", "G2"],
#             }
#         )
#     }
#     context = {
#         "repo": mock_repo,
#         "deltas": {
#             0: DataFrame({"InspectionGlobalID": ["G1", "G2"], "value": ["A", "B"]})
#         },
#         "layer_id": 0,
#     }

#     # Act
#     result = filters.wo_query_associated_planting_space_globalid(context)

#     # Assert
#     assert "PlantingSpaceGlobalID" in result["deltas"][0].columns
#     mock_repo.query.assert_called_once()


# def test_wo_query_associated_planting_space(mock_repo):
#     # Arrange
#     mock_repo.query.return_value = {
#         2: DataFrame(
#             {
#                 "GlobalID": ["PS1", "PS2"],
#                 "ParkName": ["Park1", "Park2"],
#                 "Borough": ["B1", "B2"],
#             }
#         )
#     }
#     context = {
#         "repo": mock_repo,
#         "deltas": {
#             0: DataFrame({"PlantingSpaceGlobalID": ["PS1", "PS2"], "value": ["A", "B"]})
#         },
#         "layer_id": 0,
#     }

#     # Act
#     result = filters.wo_query_associated_planting_space(context)

#     # Assert
#     assert "ParkName" in result["deltas"][0].columns
#     assert "Borough" in result["deltas"][0].columns
#     mock_repo.query.assert_called_once()


# def test_work_order_post_changes(mock_repo):
#     # Arrange
#     mock_service.post_work_orders.return_value = "Success"
#     context = {
#         "service": mock_service,
#         "deltas": {0: DataFrame({"id": [1, 2], "value": ["A", "B"]})},
#         "layer_id": 0,
#     }

#     # Act
#     result = filters.work_order_post_changes(context)

#     # Assert
#     assert result["output"] == "Work Orders result: Success"
#     mock_service.post_work_orders.assert_called_once()


# Work Order Line Items Tests
# def test_work_order_line_items_get_edits(mock_repo):
#     # Arrange
#     mock_service.get_work_order_line_items.return_value = [
#         {"objectId": 1, "value": "A"},
#         {"objectId": 2, "value": "B"},
#     ]
#     context = {
#         "service": mock_service,
#         "server_gens": DataFrame({"WorkOrder": [1609459200000]}),
#     }

#     # Act
#     result = filters.work_order_line_items_get_edits(context)

#     # Assert
#     assert "deltas" in result
#     assert 2 in result["deltas"]  # layer_id = 2
#     mock_service.get_work_order_line_items.assert_called_once()


def test_work_order_line_items_get_edits_no_changes(mock_service):
    # Arrange
    mock_service.get_work_order_line_items.return_value = []
    context = {
        "service": mock_service,
        "server_gens": DataFrame({"WorkOrder": [1609459200000]}),
    }

    # Act
    result = filters.work_order_line_items_get_edits(context)

    # Assert
    assert result is None
    assert context["output"] == "No Work Order Line Items Changes."


# def test_wo_update_associated_inspection(mock_repo):
#     # Arrange
#     mock_repo.query.return_value = {
#         4: DataFrame(
#             {
#                 "OBJECTID": [1, 2],
#                 "HasActiveWorkOrder": [1, 1],
#             }
#         )
#     }
#     context = {
#         "repo": mock_repo,
#         "layer_id": 0,
#         "deltas": {
#             0: {
#                 "updates": DataFrame(
#                     {
#                         "plantingSpaceGlobalId": ["PS1", "PS2"],
#                         "Status": ["Closed", "Active"],
#                     }
#                 )
#             }
#         },
#     }

#     # Act
#     result = filters.wo_update_associated_inspection(context)

#     # Assert
#     assert 4 in result["deltas"]
#     assert "updates" in result["deltas"][4]
#     assert result["deltas"][4]["updates"]["HasActiveWorkOrder"].iloc[0] == 0
#     mock_repo.query.assert_called_once()


def test_wo_update_associated_platingSpace(mock_repo):
    # Arrange
    mock_repo.query.return_value = {
        2: DataFrame(
            {
                "GlobalID": ["PS1", "PS2"],
                "BuildingNumber": ["123", "456"],
                "StreetName": ["Main St", "Broadway"],
            }
        )
    }
    context = {
        "repo": mock_repo,
        "layer_id": 0,
        "deltas": {
            0: {
                "updates": DataFrame(
                    {"plantingSpaceGlobalId": ["PS1", "PS2"], "value": ["A", "B"]}
                )
            }
        },
    }

    # Act
    result = filters.wo_update_associated_plantingSpace(context)

    # Assert
    assert 2 in result["deltas"]
    assert "updates" in result["deltas"][2]
    mock_repo.query.assert_called_once()
