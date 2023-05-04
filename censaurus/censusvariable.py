class CensusVariable:
    def __init__(self, name, variables) -> None:
        self.name = name

    def __repr__(self) -> str:
        return self.name