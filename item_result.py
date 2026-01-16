from enum import Enum

class ItemResult(Enum):
    SUCCESS = (1, 0)
    FAILURE = (0, 1)
    
    def __init__(self, success: int, failed: int):
        self.success = success
        self.failed = failed
