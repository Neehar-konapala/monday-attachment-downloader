class Asset:
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
    
    def get_name(self) -> str:
        return self.name
    
    def get_url(self) -> str:
        return self.url
