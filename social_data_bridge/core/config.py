"""
Unified configuration loader for social_data_bridge.

Supports profile-based configuration with user overrides.
Each profile can have a user.yaml that overrides base config values.
Shared configs also support a user.yaml.

User overrides are scoped by filename:
    user.yaml:
        pipeline:           # Overrides pipeline.yaml
            processing:
                workers: 16
        gpu_classifiers:    # Overrides gpu_classifiers.yaml
            batch_size: 1000000

List values in user.yaml fully replace base values (no merging).

No hardcoded defaults - missing required config values will raise errors.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
from copy import deepcopy


class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid."""
    pass


def deep_merge(base: Dict, override: Dict, replace_lists: bool = True) -> Dict:
    """
    Deep merge two dictionaries. Override values take precedence.
    
    Args:
        base: Base dictionary
        override: Dictionary with override values
        replace_lists: If True, lists in override fully replace base lists.
                      If False, lists would be merged (not recommended for config).
        
    Returns:
        Merged dictionary (new copy, originals unchanged)
    """
    result = deepcopy(base)
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value, replace_lists)
        elif replace_lists and isinstance(value, list):
            # Lists fully replace, not merge
            result[key] = deepcopy(value)
        else:
            result[key] = deepcopy(value)
    
    return result


def load_yaml_file(file_path: Path) -> Optional[Dict]:
    """
    Load a single YAML file.
    
    Args:
        file_path: Path to the YAML file
        
    Returns:
        Parsed YAML content, or None if file doesn't exist
        
    Raises:
        ConfigurationError: If file exists but cannot be parsed
    """
    if not file_path.exists():
        return None
    
    with open(file_path, 'r') as f:
        try:
            return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Failed to parse {file_path}: {e}")


def get_config_key(filename: str) -> str:
    """
    Get the user.yaml key for a config filename.
    
    Strips the .yaml extension to get the key name.
    e.g., 'pipeline.yaml' -> 'pipeline'
         'gpu_classifiers.yaml' -> 'gpu_classifiers'
    """
    return filename.replace('.yaml', '')


def load_profile_config(
    profile: str,
    config_dir: str = "/app/config",
    quiet: bool = False
) -> Dict[str, Any]:
    """
    Load configuration for a profile with user.yaml overrides.
    
    Loads all base config files for the profile, then applies
    user.yaml overrides scoped by filename key.
    
    user.yaml structure:
        pipeline:           # Overrides pipeline.yaml
            processing:
                workers: 16
        gpu_classifiers:    # Overrides gpu_classifiers.yaml
            batch_size: 1000000
    
    List values in user.yaml fully replace base values (no merging).
    
    Args:
        profile: Profile name ('parse', 'ml_cpu', 'ml', 'postgres_ingest', 'postgres_ml')
        config_dir: Base configuration directory
        quiet: If True, suppress informational output
        
    Returns:
        Merged configuration dictionary
        
    Raises:
        ConfigurationError: If required config files are missing
    """
    # Map profile names to config folder names
    profile_folders = {
        'postgres_ingest': 'postgres',
        'mongo_ingest': 'mongo',
    }
    folder_name = profile_folders.get(profile, profile)
    config_path = Path(config_dir) / folder_name
    
    if not config_path.exists():
        raise ConfigurationError(f"Config directory not found: {config_path}")
    
    # Define base config files per profile
    profile_configs = {
        'parse': ['pipeline.yaml'],
        'ml_cpu': ['pipeline.yaml', 'cpu_classifiers.yaml'],
        'ml': ['pipeline.yaml', 'gpu_classifiers.yaml'],
        'postgres_ingest': ['pipeline.yaml'],
        'postgres_ml': ['pipeline.yaml', 'services.yaml'],
        'mongo_ingest': ['pipeline.yaml'],
    }
    
    if profile not in profile_configs:
        raise ConfigurationError(f"Unknown profile: {profile}")
    
    # Load user.yaml if it exists
    user_config_path = config_path / 'user.yaml'
    user_config = load_yaml_file(user_config_path)
    has_user_config = user_config is not None
    
    if has_user_config and not quiet:
        print(f"[sdb] Using user override: {profile}/user.yaml")
    
    # Load each base config file and apply user overrides
    merged_config = {}
    for config_file in profile_configs[profile]:
        file_path = config_path / config_file
        config = load_yaml_file(file_path)
        
        if config is None:
            raise ConfigurationError(f"Required config file not found: {file_path}")
        
        # Apply user overrides for this specific file
        if has_user_config:
            config_key = get_config_key(config_file)
            if config_key in user_config:
                config = deep_merge(config, user_config[config_key])
        
        # Merge into final config
        merged_config = deep_merge(merged_config, config)
    
    return merged_config


def get_required(config: Dict, *keys: str, error_msg: str = None) -> Any:
    """
    Get a required configuration value, raising error if missing.
    
    Args:
        config: Configuration dictionary
        *keys: Path of keys to traverse (e.g., 'processing', 'data_types')
        error_msg: Custom error message (optional)
        
    Returns:
        Configuration value
        
    Raises:
        ConfigurationError: If value is missing
    """
    value = config
    path = []
    
    for key in keys:
        path.append(key)
        if not isinstance(value, dict) or key not in value:
            key_path = '.'.join(path)
            msg = error_msg or f"Required configuration missing: {key_path}"
            raise ConfigurationError(msg)
        value = value[key]
    
    return value


def get_optional(config: Dict, *keys: str, default: Any = None) -> Any:
    """
    Get an optional configuration value with a default.
    
    Args:
        config: Configuration dictionary
        *keys: Path of keys to traverse
        default: Default value if not found
        
    Returns:
        Configuration value or default
    """
    value = config
    
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    
    return value


def validate_processing_config(config: Dict, profile: str) -> None:
    """
    Validate that required processing config exists.
    
    Args:
        config: Configuration dictionary
        profile: Profile name for error messages
        
    Raises:
        ConfigurationError: If required config is missing
    """
    required_keys = ['data_types']
    
    for key in required_keys:
        if 'processing' not in config or key not in config['processing']:
            raise ConfigurationError(
                f"[{profile}] Required config missing: processing.{key}"
            )


def validate_database_config(config: Dict) -> None:
    """
    Validate that required database config exists for postgres profiles.
    
    Args:
        config: Configuration dictionary
        
    Raises:
        ConfigurationError: If required config is missing
    """
    required_keys = ['host', 'port', 'name', 'schema', 'user']
    
    for key in required_keys:
        if 'database' not in config or key not in config['database']:
            raise ConfigurationError(
                f"[postgres] Required config missing: database.{key}"
            )


def validate_mongo_config(config: Dict) -> None:
    """
    Validate that required MongoDB config exists for mongo_ingest profile.

    Args:
        config: Configuration dictionary

    Raises:
        ConfigurationError: If required config is missing
    """
    required_keys = ['host', 'port']

    for key in required_keys:
        if 'database' not in config or key not in config['database']:
            raise ConfigurationError(
                f"[mongo] Required config missing: database.{key}"
            )


def validate_classifier_config(config: Dict, classifier_name: str, profile: str) -> None:
    """
    Validate that required classifier config exists.
    
    Args:
        config: Classifier configuration dictionary
        classifier_name: Name of the classifier
        profile: Profile name for error messages
        
    Raises:
        ConfigurationError: If required config is missing
    """
    if profile == 'ml_cpu' and classifier_name == 'lingua':
        required_keys = ['suffix', 'languages']
    else:
        # GPU classifiers
        required_keys = ['suffix', 'model']
    
    for key in required_keys:
        if key not in config:
            raise ConfigurationError(
                f"[{profile}] Required config missing for {classifier_name}: {key}"
            )


def load_platform_config(
    config_dir: str = "/app/config",
    platform: str = None
) -> Dict[str, Any]:
    """
    Load platform-specific configuration.

    Built-in platforms (e.g., 'reddit'):
        Loads config/platforms/{platform}/platform.yaml as the base,
        then deep-merges user.yaml on top if it exists.

    Custom platforms (e.g., 'custom/twitter'):
        Loads config/platforms/custom/{name}.yaml directly.
        No user.yaml support — the file is self-contained.

    Args:
        config_dir: Base configuration directory
        platform: Platform name. If None, reads from PLATFORM env var (default: reddit)

    Returns:
        Platform configuration dictionary

    Raises:
        ConfigurationError: If config file is not found
    """
    if platform is None:
        platform = os.environ.get('PLATFORM', 'reddit')

    if platform.startswith('custom/'):
        # Custom platform: single self-contained file
        name = platform.split('/', 1)[1]
        if not name:
            raise ConfigurationError("Custom platform name is empty. Use PLATFORM=custom/<name>")
        config_path = Path(config_dir) / "platforms" / "custom" / f"{name}.yaml"
        config = load_yaml_file(config_path)
        if config is None:
            raise ConfigurationError(f"Custom platform config not found: {config_path}")
        return config

    # Built-in platform: platform.yaml + optional user.yaml merge
    platform_dir = Path(config_dir) / "platforms" / platform

    base_path = platform_dir / "platform.yaml"
    config = load_yaml_file(base_path)
    if config is None:
        raise ConfigurationError(f"Platform config not found: {base_path}")

    # Apply user.yaml overrides if present
    user_path = platform_dir / "user.yaml"
    user_config = load_yaml_file(user_path)
    if user_config is not None:
        config = deep_merge(config, user_config)

    return config


def get_platform_fields(platform_config: Dict, data_type: str) -> List[str]:
    """
    Get the field list for a data type from platform config.

    Args:
        platform_config: Loaded platform configuration
        data_type: Data type key (e.g., 'submissions', 'comments')

    Returns:
        List of field names

    Raises:
        ConfigurationError: If no fields are configured for the data type
    """
    fields = platform_config.get('fields', {}).get(data_type, [])
    if not fields:
        raise ConfigurationError(f"No fields configured for data type: {data_type}")
    return fields


def get_platform_field_types(platform_config: Dict) -> Dict[str, Any]:
    """
    Get the field type definitions from platform config.

    Args:
        platform_config: Loaded platform configuration

    Returns:
        Dictionary mapping field names to type definitions

    Raises:
        ConfigurationError: If no field_types are configured
    """
    field_types = platform_config.get('field_types', {})
    if not field_types:
        raise ConfigurationError("No field_types configured in platform config")
    return field_types


def apply_env_overrides(config: Dict, profile: str) -> Dict:
    """
    Apply environment variable overrides to configuration.
    
    For postgres profiles, environment variables override database settings.
    
    Args:
        config: Configuration dictionary
        profile: Profile name
        
    Returns:
        Configuration with env overrides applied
    """
    result = deepcopy(config)
    
    if profile in ('postgres_ingest', 'postgres_ml'):
        if 'database' not in result:
            result['database'] = {}

        if 'POSTGRES_PORT' in os.environ:
            result['database']['port'] = int(os.environ['POSTGRES_PORT'])
        if 'DB_NAME' in os.environ:
            result['database']['name'] = os.environ['DB_NAME']
        if 'DB_SCHEMA' in os.environ:
            result['database']['schema'] = os.environ['DB_SCHEMA']

    if profile == 'mongo_ingest':
        if 'database' not in result:
            result['database'] = {}

        if 'MONGO_PORT' in os.environ:
            result['database']['port'] = int(os.environ['MONGO_PORT'])

    return result
