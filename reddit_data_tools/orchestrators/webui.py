"""
WebUI profile orchestrator for reddit_data_tools.

Handles configuration generation for the LibreChat + Redash stack:
- Generates librechat.yaml from template
- Creates Redash secrets if not provided
- Creates read-only PostgreSQL user for Redash
- Validates LLM provider configuration

Run this before starting the webui profile:
    docker compose run --rm webui-setup
"""

import os
import re
import sys
import secrets
import string
from pathlib import Path
from typing import Dict, Any, Optional

from ..core.config import (
    load_profile_config,
    get_required,
    get_optional,
    ConfigurationError,
    load_yaml_file,
)


def generate_secret(length: int = 32) -> str:
    """Generate a cryptographically secure random string."""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def load_config(config_dir: str = "/app/config", quiet: bool = False) -> Dict:
    """
    Load webui profile configuration.
    
    Args:
        config_dir: Base configuration directory
        quiet: If True, suppress informational output
        
    Returns:
        Merged configuration dictionary
        
    Raises:
        ConfigurationError: If required config is missing
    """
    config = load_profile_config('webui', config_dir, quiet)
    return config


def validate_llm_config(config: Dict) -> None:
    """
    Validate LLM provider configuration.
    
    Args:
        config: Configuration dictionary
        
    Raises:
        ConfigurationError: If LLM config is invalid
    """
    llm = get_required(config, 'llm')
    provider = get_required(config, 'llm', 'provider')
    model = get_required(config, 'llm', 'model')
    
    valid_providers = ['local', 'openai', 'anthropic', 'openrouter']
    if provider not in valid_providers:
        raise ConfigurationError(
            f"Invalid LLM provider: {provider}. Must be one of: {valid_providers}"
        )
    
    if model == 'local-model':
        print("[WARNING] LLM model is set to 'local-model'. Please update config/webui/pipeline.yaml")
    
    # Remote providers require API key
    if provider != 'local':
        api_key = get_optional(config, 'llm', 'api_key')
        if not api_key:
            raise ConfigurationError(
                f"API key required for provider '{provider}'. "
                f"Set llm.api_key in config/webui/user.yaml or pipeline.yaml"
            )


def get_llm_base_url(config: Dict) -> str:
    """
    Get the LLM base URL based on provider configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Base URL for the LLM API
    """
    provider = get_required(config, 'llm', 'provider')
    
    # Check for explicit override
    base_url = get_optional(config, 'llm', 'base_url')
    if base_url:
        return base_url
    
    # Provider-specific defaults
    if provider == 'local':
        port = get_optional(config, 'llm', 'port', default=1234)
        return f"http://host.docker.internal:{port}/v1"
    elif provider == 'openai':
        return "https://api.openai.com/v1"
    elif provider == 'anthropic':
        return "https://api.anthropic.com/v1"
    elif provider == 'openrouter':
        return "https://openrouter.ai/api/v1"
    else:
        raise ConfigurationError(f"Unknown provider: {provider}")


def get_endpoint_name(config: Dict) -> str:
    """Get a display name for the LLM endpoint."""
    provider = get_required(config, 'llm', 'provider')
    
    provider_names = {
        'local': 'Local LLM',
        'openai': 'OpenAI',
        'anthropic': 'Anthropic',
        'openrouter': 'OpenRouter',
    }
    return provider_names.get(provider, provider.title())


def generate_librechat_yaml(config: Dict, prompts: Dict, output_path: Path) -> None:
    """
    Generate librechat.yaml from template and configuration.
    
    Args:
        config: Pipeline configuration
        prompts: Prompts configuration
        output_path: Path to write the generated file
    """
    template_path = Path("/app/config/webui/librechat.yaml.template")
    
    if not template_path.exists():
        raise ConfigurationError(f"Template not found: {template_path}")
    
    with open(template_path, 'r') as f:
        template = f.read()
    
    # Prepare substitution values
    llm_model = get_required(config, 'llm', 'model')
    llm_api_key = get_optional(config, 'llm', 'api_key') or 'dummy-key-for-local'
    llm_base_url = get_llm_base_url(config)
    endpoint_name = get_endpoint_name(config)
    
    # Get MCP instructions from prompts
    mcp_instructions = get_optional(prompts, 'mcp_instructions', default='')
    # Escape for YAML multiline string
    mcp_instructions = mcp_instructions.replace('\\', '\\\\').replace('"', '\\"')
    
    # Substitutions for LLM config only
    # MCP environment variables (REDASH_URL, REDASH_API_KEY, REDASH_TIMEOUT)
    # are left as ${VAR} for LibreChat to substitute from container environment
    substitutions = {
        '${LLM_ENDPOINT_NAME}': endpoint_name,
        '${LLM_API_KEY}': llm_api_key,
        '${LLM_BASE_URL}': llm_base_url,
        '${LLM_MODEL}': llm_model,
        '${MCP_INSTRUCTIONS}': mcp_instructions,
    }
    
    result = template
    for key, value in substitutions.items():
        result = result.replace(key, value)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write(result)
    
    print(f"[WEBUI] Generated: {output_path}")


def generate_secrets(output_dir: Path) -> Dict[str, str]:
    """
    Generate Redash secrets if not already set.
    
    Returns:
        Dictionary of secret name -> value
    """
    secrets_file = output_dir / 'secrets.env'
    secrets_dict = {}
    
    # Load existing secrets if present
    if secrets_file.exists():
        with open(secrets_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    secrets_dict[key] = value
    
    # Generate missing secrets
    changed = False
    
    if not secrets_dict.get('REDASH_SECRET_KEY'):
        secrets_dict['REDASH_SECRET_KEY'] = generate_secret(64)
        changed = True
        print("[WEBUI] Generated REDASH_SECRET_KEY")
    
    if not secrets_dict.get('REDASH_COOKIE_SECRET'):
        secrets_dict['REDASH_COOKIE_SECRET'] = generate_secret(32)
        changed = True
        print("[WEBUI] Generated REDASH_COOKIE_SECRET")
    
    if not secrets_dict.get('REDASH_API_KEY'):
        # API key will be generated after Redash is running
        # For now, generate a placeholder
        secrets_dict['REDASH_API_KEY'] = ''
        print("[WEBUI] REDASH_API_KEY will be configured after first Redash startup")
    
    if not secrets_dict.get('READONLY_PASSWORD'):
        secrets_dict['READONLY_PASSWORD'] = generate_secret(24)
        changed = True
        print("[WEBUI] Generated READONLY_PASSWORD for database user")
    
    # Save secrets
    if changed:
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(secrets_file, 'w') as f:
            f.write("# WebUI secrets - auto-generated\n")
            f.write("# Copy these to your .env file if needed\n\n")
            for key, value in secrets_dict.items():
                f.write(f"{key}={value}\n")
        print(f"[WEBUI] Saved secrets to: {secrets_file}")
    
    return secrets_dict


def create_readonly_user_sql(config: Dict, secrets: Dict) -> str:
    """
    Generate SQL to create a read-only PostgreSQL user for Redash.
    
    Args:
        config: Configuration dictionary
        secrets: Secrets dictionary
        
    Returns:
        SQL commands as a string
    """
    db_config = get_required(config, 'database')
    schema = get_optional(db_config, 'schema', default='reddit')
    readonly_user = get_optional(db_config, 'readonly_user', default='redash_readonly')
    readonly_password = secrets.get('READONLY_PASSWORD', generate_secret(24))
    
    sql = f"""
-- Create read-only user for Redash
-- Run this on the data PostgreSQL database (not Redash's database)

-- Create user if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{readonly_user}') THEN
        CREATE ROLE {readonly_user} WITH LOGIN PASSWORD '{readonly_password}';
    ELSE
        ALTER ROLE {readonly_user} WITH PASSWORD '{readonly_password}';
    END IF;
END
$$;

-- Grant connect to database
GRANT CONNECT ON DATABASE {os.environ.get('DB_NAME', 'datasets')} TO {readonly_user};

-- Grant usage on schema
GRANT USAGE ON SCHEMA {schema} TO {readonly_user};

-- Grant SELECT on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {readonly_user};

-- Grant SELECT on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO {readonly_user};

-- Verify permissions
SELECT 'Read-only user created: {readonly_user}' AS status;
"""
    return sql


def print_setup_instructions(config: Dict, secrets: Dict, output_dir: Path) -> None:
    """Print setup instructions for the user."""
    librechat_port = get_optional(config, 'librechat', 'port', default=3080)
    redash_port = get_optional(config, 'redash', 'port', default=5000)
    
    print("\n" + "=" * 60)
    print("WEBUI SETUP COMPLETE")
    print("=" * 60)
    
    print("\n[1] Copy secrets to your .env file:")
    print(f"    cat {output_dir}/secrets.env >> .env")
    
    print("\n[2] Create the read-only database user:")
    print("    # Connect to your data PostgreSQL and run:")
    print(f"    psql -h localhost -U postgres -d ${{DB_NAME}} -f {output_dir}/create_readonly_user.sql")
    
    print("\n[3] Initialize Redash database (first time only):")
    print("    docker compose run --rm redash-server create_db")
    
    print("\n[4] Start the webui stack:")
    print("    docker compose --profile webui up -d")
    
    print("\n[5] After Redash starts, create an API key:")
    print(f"    a. Open Redash at http://localhost:{redash_port}")
    print("    b. Create admin user (first time)")
    print("    c. Go to Settings -> API Keys -> Create")
    print("    d. Copy the key and add to .env: REDASH_API_KEY=<key>")
    
    print("\n[6] Configure data source in Redash:")
    print("    a. Go to Settings -> Data Sources -> New Data Source")
    print("    b. Choose PostgreSQL")
    print("    c. Use these settings:")
    print("       Host: postgres")
    print(f"       Port: {os.environ.get('POSTGRES_PORT', '5432')}")
    print(f"       Database: {os.environ.get('DB_NAME', 'datasets')}")
    print(f"       User: {get_optional(config, 'database', 'readonly_user', default='redash_readonly')}")
    print(f"       Password: (from secrets.env)")
    
    print("\n[7] Access the WebUI:")
    print(f"    LibreChat: http://localhost:{librechat_port}")
    print(f"    Redash:    http://localhost:{redash_port}")
    
    print("\n" + "=" * 60)


def run_setup(config_dir: str = "/app/config"):
    """
    Run the WebUI setup process.
    
    Generates configuration files and prints setup instructions.
    """
    print("[WEBUI] Starting setup...")
    
    # Load configuration
    config = load_config(config_dir)
    
    # Validate LLM config
    try:
        validate_llm_config(config)
    except ConfigurationError as e:
        print(f"[WARNING] {e}")
        print("[WARNING] You can continue setup but will need to configure LLM before use")
    
    # Load prompts
    prompts_path = Path(config_dir) / 'webui' / 'prompts.yaml'
    prompts = load_yaml_file(prompts_path) or {}
    
    # Output directory
    output_dir = Path("/data/webui")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate secrets
    secrets = generate_secrets(output_dir)
    
    # Generate librechat.yaml
    librechat_yaml_path = output_dir / 'librechat.yaml'
    generate_librechat_yaml(config, prompts, librechat_yaml_path)
    
    # Generate SQL for read-only user
    sql = create_readonly_user_sql(config, secrets)
    sql_path = output_dir / 'create_readonly_user.sql'
    with open(sql_path, 'w') as f:
        f.write(sql)
    print(f"[WEBUI] Generated: {sql_path}")
    
    # Print instructions
    print_setup_instructions(config, secrets, output_dir)


def main():
    """Main entry point."""
    config_dir = "/app/config"
    
    try:
        run_setup(config_dir)
    except ConfigurationError as e:
        print(f"[ERROR] Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Setup failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
