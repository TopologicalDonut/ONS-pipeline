from pathlib import Path
from dataclasses import dataclass, field

@dataclass(frozen = True)
class PathConfig:
    PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
    DATA_DIR: Path = PROJECT_ROOT / 'data'
    DB_DIR: Path = PROJECT_ROOT / 'database'
    LOG_DIR: Path = PROJECT_ROOT/ 'logs'
    VALIDATION_DIR: Path = DATA_DIR / 'validation_problems'

PATH_CONFIG = PathConfig()
