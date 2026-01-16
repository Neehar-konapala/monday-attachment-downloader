import os
from pathlib import Path
from typing import Optional

import monday_config


def save(file_name: str, data: bytes, download_dir: Optional[str] = None) -> None:
    """
    Save file to specified directory (or default if not specified)
    """
    if download_dir is None:
        download_dir = monday_config.BASE_DOWNLOAD_DIR
    
    if file_name is None or not file_name.strip():
        import time
        file_name = f"attachment_{int(time.time() * 1000)}"
    
    dir_path = Path(download_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    
    # Handle duplicate filenames
    output_path = dir_path / file_name
    counter = 1
    base_name = file_name
    extension = ""
    
    last_dot = file_name.rfind('.')
    if last_dot > 0:
        base_name = file_name[:last_dot]
        extension = file_name[last_dot:]
    
    while output_path.exists():
        new_name = f"{base_name}_{counter}{extension}"
        output_path = dir_path / new_name
        counter += 1
    
    output_path.write_bytes(data)
    print(f"   Saved: {output_path.absolute()}")
