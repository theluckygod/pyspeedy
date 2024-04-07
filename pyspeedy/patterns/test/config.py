from pyspeedy.patterns.singleton import SingletonMeta


class Config(metaclass=SingletonMeta):
    def __init__(self, value: int):
        self.value = value


conf = Config(value=1)
