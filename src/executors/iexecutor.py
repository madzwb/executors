
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, cast



class IExecutor(ABC):

    @classmethod
    def __init_subclass__(cls, /, *args, **kwargs) -> None:
        cls.init(*args, **kwargs)
    
    @classmethod
    @abstractmethod
    def init(cls,*args, **kwargs) -> bool: ...

    @abstractmethod
    def start(self) -> bool: ...

    @abstractmethod
    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool: ...

    @abstractmethod
    def join(self, timeout = None) -> bool: ...

    @abstractmethod
    def shutdown(self, wait = True, * , cancel = False) -> bool: ...

    # @abstractmethod
    # def __bool__(self) -> bool: ...
