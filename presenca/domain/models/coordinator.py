from functools import total_ordering

@total_ordering
class Coordinator:
    
    def __init__(self, raw_name: str):
        self.name = self._normalize(raw_name)
    
    def _normalize(self, name: str) -> str:
        if not isinstance(name, str):
            return "Indefinido"
        return name.strip().title()

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, Coordinator):
            return self.name == other.name
        return False

    def __lt__(self, other):
        if isinstance(other, Coordinator):
            return self.name < other.name
        return NotImplemented
    
    def __repr__(self):
        return f"Coordinator(name='{self.name}')"