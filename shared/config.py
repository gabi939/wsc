import yaml
from pathlib import Path


def load_config() -> dict:
    """
    Load and parse configuration from config.yaml
    Returns a dictionary with all configuration values
    """
    config_path = Path(__file__).parent.parent / "config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config


config = load_config()
