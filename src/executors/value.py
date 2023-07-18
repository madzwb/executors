


class Value():

    def __init__(self,_value: int) -> None:
        self._value = _value

    @property
    def value(self) -> int:
        return self._value
    
    @value.setter
    def value(self, value: int) -> None:
        self._value = value
