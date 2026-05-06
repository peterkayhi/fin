import logging
from pathlib import Path

log_dir = Path.home() / "Downloads" / "backtestFiles"
log_dir.mkdir(parents=True, exist_ok=True)

# Setup once (put this at the top of your main file)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',   # <-- timestamp here
    datefmt='%Y-%m-%d %H:%M:%S',                           # control timestamp format
    handlers=[
        logging.FileHandler(log_dir / "app.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Then use anywhere in your code:
logging.info("User logged in")
logging.debug("Processing record debug")
logging.error("Failed to save file error")