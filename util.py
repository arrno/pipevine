from dataclasses import dataclass, field
from typing import TypeVar, Callable, Any, cast, TypeGuard, TypeAlias, ParamSpec
from functools import wraps

@dataclass
class Err:
    message: str
    trace: list["Err"] = field(default_factory=list)

    def wrap(self, other: "Err") -> "Err":
        self.trace = [Err(other.message), *other.trace, *self.trace]
        return self
    
    def __repr__(self) -> str:
        rep = self.message
        if len(self.trace) > 0:
            rep += "\n"
            rep += "\n".join([repr(x) for x in self.trace])
        return rep

P = ParamSpec("P")
T = TypeVar("T")
Result: TypeAlias = T | Err

def is_err(res: Result[T]) -> bool:
    return isinstance(res, Err)

def get_err(res: Result[T]) -> str:
    if isinstance(res, Err):
        return res.message
    return ""

def is_ok(res: Result[T]) -> TypeGuard[T]:
    return not is_err(res)

def unwrap(res: Result[T]) -> T:
    if isinstance(res, Err):
        raise RuntimeError("unwrap on Err: "+res.message)
    return res

def unwrap_or(res: Result[T], default: T) -> T:
    if isinstance(res, Err):
        return default
    return res

def err_as_value(func: Callable[P, T]) -> Callable[P, Result[T]]:
    @wraps(func)
    def wrap(*args: P.args, **kwargs: P.kwargs) -> Result[T]:
        try:
            res = func(*args, **kwargs)
            return res
        except Exception as e:
            return Err(repr(e))
    return wrap


def with_retry(retry: int) -> Callable[[Callable[P, T]], Callable[P, Result[T]]]:
    def decorator(func: Callable[P, T]) -> Callable[P, Result[T]]:
        # Wrap the original function with error-to-value behavior
        safe_do: Callable[P, Result[T]] = err_as_value(func)

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[T]:
            if retry < 1:
                return Err("retry without positive try value")

            last_err: Err | None = None
            for _ in range(retry):
                res = safe_do(*args, **kwargs)
                if is_ok(res):
                    return res
                last_err = cast(Err, res)

            # last_err is set if we never returned an Ok
            return Err("failed with retry").wrap(cast(Err, last_err))
        return wrapper
    return decorator