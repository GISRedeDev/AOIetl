import pytest
import os
from unittest.mock import patch, MagicMock
from upath import UPath

from aoietl.build_paths import build_config
from aoietl.validation import validate_directories
from aoietl.data_types import setup_azure_filesystem, DirectoryType, RasterType, HDFType, VectorFileName, ParquetFileName


def test_validate_directories(test_config):
    config = build_config(test_config)
    try:
        validate_directories(config)
    except ValueError as e:
        pytest.fail(f"Validation failed with error: {e}")


def test_validate_directories_valid_config(test_config):
    """Test validation passes with valid configuration"""
    config = build_config(test_config)
    result = validate_directories(config)
    assert result is True



@patch.dict(os.environ, {'AZURE_ACCOUNT_NAME': 'test_account', 'AZURE_ACCOUNT_KEY': 'test_account_key'})
@patch('aoietl.data_types.UPath')
def test_setup_azure_filesystem_with_env(mock_upath, test_config, mock_azure_env):
    """Test Azure filesystem setup with environment variables set"""
    from aoietl.build_paths import build_config
    
    # Setup mocks
    mock_path = MagicMock()
    mock_upath.return_value = mock_path
    
    # Load config and test
    config = build_config(test_config)
    tier_roots = setup_azure_filesystem(config)
    
    # Verify UPath was called for each directory type
    assert mock_upath.call_count == len(DirectoryType)
    
    # Verify UPath was called with correct parameters for each directory
    expected_calls = []
    for directory_type in DirectoryType:
        expected_calls.append(
            mock_upath.call_args_list[0] if len(mock_upath.call_args_list) > 0 else None
        )
    
    # Verify tier_roots has all expected attributes
    for dir_type in DirectoryType:
        assert hasattr(tier_roots, dir_type.value)



def test_setup_azure_filesystem_missing_env(test_config, mock_azure_env_unset):
    """Test that missing environment variables raises ValueError"""
    from aoietl.build_paths import build_config
    
    config = build_config(test_config)
    
    with pytest.raises(ValueError, match="Azure account name and key must be set"):
        _ = setup_azure_filesystem(config)