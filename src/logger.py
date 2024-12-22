import logging
from pathlib import Path

def setup_logger(
    name: str,
    level: int = logging.INFO, 
    log_dir: Path = Path('logs'),
    add_file_handler: bool = False
) -> logging.Logger:
    """
    Parameters
    ----------
    name:
        Logger name. Usually __name__ from module
    level:
        Logging detail level (e.g. logging.DEBUG)
    log_dir:
        Directory to store log files (if enabled)
    add_file_handler:
        Whether to add file output
    """
    
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(level)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        if add_file_handler:
            log_dir
            file_handler = logging.FileHandler(
                log_dir / f'{name}'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger