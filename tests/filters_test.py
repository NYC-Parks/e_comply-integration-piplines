from unittest.mock import MagicMock, patch
from filters import apply_edits, LayerEdits


def test_apply_edits_with_valid_deltas():
    mock_repo = MagicMock()
    mock_repo.apply_edits.return_value = "mock_result"

    context = {
        "deltas": {
            1: {"adds": [1, 2], "updates": [3, 4]},
            2: {"adds": [5], "updates": []},
        },
        "repo": mock_repo,
    }

    with patch("filters.filter_logger.debug") as mock_debug, patch(
        "filters.to_json", return_value="mock_json"
    ):

        result = apply_edits(context)

        # Verify repo.apply_edits was called with the expected LayerEdits
        expected_layer_edits = [
            LayerEdits(1, adds=[1, 2], updates=[3, 4]),
            LayerEdits(2, adds=[5], updates=[]),
        ]
        mock_repo.apply_edits.assert_called_once_with(expected_layer_edits)

        # Verify logging was called
        assert mock_debug.call_count > 0

        # Ensure context is returned unchanged
        assert result == context


def test_apply_edits_with_no_deltas():
    context = {"deltas": {}, "repo": MagicMock()}
    result = apply_edits(context)
    assert result == context  # Function should return unchanged context


def test_apply_edits_with_exception():
    mock_repo = MagicMock()
    mock_repo.apply_edits.side_effect = Exception("Test Exception")

    context = {
        "deltas": {"layer1": {"adds": [1]}},
        "repo": mock_repo,
    }

    with patch("your_module.exception_handler") as mock_exception_handler:
        result = apply_edits(context)

        # Verify exception handler was called
        mock_exception_handler.assert_called_once()

        # Ensure context is returned unchanged
        assert result == context
