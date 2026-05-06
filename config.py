import json
import sys
import tomllib
from pathlib import Path


def get_config_local(filename: Path) -> dict:
    with open(filename, 'rb') as f:
        return tomllib.load(f)


def get_config() -> dict:
    try:
        script_dir = Path(__file__).resolve().parent
        config_dict = get_config_local(script_dir / 'config.toml')
        return config_dict
    except FileNotFoundError as e:
        print(f'config.toml: {e=}', file=sys.stderr)
        sys.exit(1)


def get_first_config() -> dict:
    files: list[Path] = [
        Path('/config/config.toml'),
        Path('config.toml'),
    ]
    for file in files:
        if file.exists():
            loaded_config: dict = get_config_local(file)
            break
    else:
        raise FileNotFoundError
    options_files: list[Path] = [
        Path('/data/options.json'),
        Path('/data/options.toml'),
    ]
    for options_file in options_files:
        if options_file.exists():
            if options_file.suffix == '.json':
                with open(options_file) as file:
                    options: dict = json.load(file)
            else:
                options: dict = get_config_local(options_file)
            for key, option in options.items():
                if isinstance(option, str) and option:
                    loaded_config[key] = option
                elif isinstance(option, int) or isinstance(option, bool):
                    loaded_config[key] = option
            break
    return loaded_config
