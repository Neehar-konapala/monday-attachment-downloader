class GroupResult:
    def __init__(self, success: int, failed: int, processed: bool):
        self.success = success
        self.failed = failed
        self.processed = processed
    
    @staticmethod
    def processed(success: int, failed: int) -> 'GroupResult':
        return GroupResult(success, failed, True)
    
    @staticmethod
    def not_processed() -> 'GroupResult':
        return GroupResult(0, 0, False)
    
    @staticmethod
    def failed_group() -> 'GroupResult':
        return GroupResult(0, 1, True)
