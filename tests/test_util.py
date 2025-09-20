"""Tests for util module - Result types, error handling, and retry logic."""

from typing import Any

import pytest

from pipevine.util import (
    Err,
    err_as_value,
    get_err,
    is_err,
    is_ok,
    unwrap,
    unwrap_or,
    with_retry,
)


class TestErr:
    """Test the Err class functionality."""
    
    def test_err_creation(self) -> None:
        err = Err("test error")
        assert err.message == "test error"
        assert err.trace == []
    
    def test_err_repr_simple(self) -> None:
        err = Err("simple error")
        assert repr(err) == "simple error"
    
    def test_err_wrap(self) -> None:
        err1 = Err("first error")
        err2 = Err("second error")
        wrapped = err1.wrap(err2)
        
        assert wrapped is err1  # wrap modifies and returns self
        assert len(wrapped.trace) == 1
        assert wrapped.trace[0].message == "second error"
    
    def test_err_wrap_with_nested_trace(self) -> None:
        err1 = Err("error1")
        err2 = Err("error2")
        err3 = Err("error3")
        
        # Create a chain: err2 wraps err3, then err1 wraps err2
        err2.wrap(err3)
        result = err1.wrap(err2)
        
        assert result is err1
        assert len(result.trace) == 2  # err2 + err3
    
    def test_err_repr_with_trace(self) -> None:
        err1 = Err("main error")
        err2 = Err("traced error")
        err1.wrap(err2)
        
        repr_str = repr(err1)
        assert "main error" in repr_str
        assert "traced error" in repr_str
        assert "\n" in repr_str


class TestResultFunctions:
    """Test Result type helper functions."""
    
    def test_is_err_with_err(self) -> None:
        err = Err("test")
        assert is_err(err) is True
    
    def test_is_err_with_value(self) -> None:
        value = "success"
        assert is_err(value) is False
    
    def test_get_err_with_err(self) -> None:
        err = Err("error message")
        assert get_err(err) == "error message"
    
    def test_get_err_with_value(self) -> None:
        value = "success"
        assert get_err(value) == ""
    
    def test_is_ok_with_value(self) -> None:
        value = 42
        assert is_ok(value) is True
    
    def test_is_ok_with_err(self) -> None:
        err = Err("failure")
        assert is_ok(err) is False
    
    def test_unwrap_success(self) -> None:
        value = "test value"
        assert unwrap(value) == "test value"
    
    def test_unwrap_error_raises(self) -> None:
        err = Err("test error")
        with pytest.raises(RuntimeError, match="unwrap on Err: test error"):
            unwrap(err)
    
    def test_unwrap_or_with_value(self) -> None:
        value = "success"
        result = unwrap_or(value, "default")
        assert result == "success"
    
    def test_unwrap_or_with_error(self) -> None:
        err = Err("failure")
        result = unwrap_or(err, "default")
        assert result == "default"


class TestErrAsValue:
    """Test the err_as_value decorator."""
    
    def test_success_case(self) -> None:
        @err_as_value
        def successful_function(x: int) -> int:
            return x * 2
        
        result = successful_function(5)
        assert result == 10
        assert is_ok(result)
    
    def test_exception_case(self) -> None:
        @err_as_value
        def failing_function(x: int) -> int:
            if x < 0:
                raise ValueError("Negative not allowed")
            return x * 2
        
        result = failing_function(-1)
        assert is_err(result)
        assert isinstance(result, Err)
        assert "ValueError" in result.message
        assert "Negative not allowed" in result.message
    
    def test_preserves_function_metadata(self) -> None:
        @err_as_value
        def documented_function(x: int) -> int:
            """A documented function."""
            return x
        
        assert documented_function.__name__ == "documented_function"
        assert documented_function.__doc__ == "A documented function."


class TestWithRetry:
    """Test the with_retry decorator."""
    
    def test_success_on_first_try(self) -> None:
        call_count = 0
        
        @with_retry(3)
        def always_succeeds(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2
        
        result = always_succeeds(5)
        assert is_ok(result)
        assert unwrap(result) == 10
        assert call_count == 1
    
    def test_success_after_retries(self) -> None:
        call_count = 0
        
        @with_retry(3)
        def succeeds_on_third_try(x: int) -> int:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError(f"Attempt {call_count} failed")
            return x * 2
        
        result = succeeds_on_third_try(5)
        assert is_ok(result)
        assert unwrap(result) == 10
        assert call_count == 3
    
    def test_failure_after_all_retries(self) -> None:
        call_count = 0
        
        @with_retry(2)
        def always_fails(x: int) -> int:
            nonlocal call_count
            call_count += 1
            raise ValueError(f"Attempt {call_count} failed")
        
        result = always_fails(5)
        assert is_err(result)
        assert isinstance(result, Err)
        assert call_count == 2
        assert "failed with retry" in result.message
        assert len(result.trace) > 0  # Should have wrapped the last error
    
    def test_zero_retries(self) -> None:
        @with_retry(0)
        def some_function(x: int) -> int:
            return x
        
        result = some_function(5)
        assert is_err(result)
        assert "retry without positive try value" in get_err(result)
    
    def test_negative_retries(self) -> None:
        @with_retry(-1)
        def some_function(x: int) -> int:
            return x
        
        result = some_function(5)
        assert is_err(result)
        assert "retry without positive try value" in get_err(result)
    
    def test_preserves_function_metadata(self) -> None:
        @with_retry(3)
        def documented_function(x: int) -> int:
            """A documented function with retries."""
            return x
        
        assert documented_function.__name__ == "documented_function"
        assert documented_function.__doc__ == "A documented function with retries."
    
    def test_different_exception_types(self) -> None:
        call_count = 0
        
        @with_retry(3)
        def mixed_exceptions(x: int) -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("First error")
            elif call_count == 2:
                raise RuntimeError("Second error")
            return x
        
        result = mixed_exceptions(42)
        assert is_ok(result)
        assert unwrap(result) == 42
        assert call_count == 3


class TestResultIntegration:
    """Integration tests for Result type workflows."""
    
    def test_chained_operations_success(self) -> None:
        @err_as_value
        def double(x: int) -> int:
            return x * 2
        
        @err_as_value
        def add_ten(x: int) -> int:
            return x + 10
        
        # Chain operations
        result1 = double(5)
        assert is_ok(result1)
        
        result2 = add_ten(unwrap(result1))
        assert is_ok(result2)
        assert unwrap(result2) == 20
    
    def test_chained_operations_with_failure(self) -> None:
        @err_as_value
        def divide(x: int, y: int) -> float:
            return x / y
        
        @err_as_value
        def sqrt(x: float) -> float:
            if x < 0:
                raise ValueError("Cannot take sqrt of negative")
            return float(x ** 0.5)
        
        # First operation fails
        result1 = divide(5, 0)
        assert is_err(result1)
        
        # Chain doesn't proceed when first fails
        if is_ok(result1):
            result2 = sqrt(unwrap(result1))
        else:
            result2 = result1
        
        assert is_err(result2)
        assert "ZeroDivisionError" in get_err(result2)
    
    def test_retry_with_result_type(self) -> None:
        attempt = 0
        
        @with_retry(3)
        def flaky_operation() -> str:
            nonlocal attempt
            attempt += 1
            if attempt < 3:
                raise ConnectionError(f"Network failed on attempt {attempt}")
            return "success"
        
        result = flaky_operation()
        assert is_ok(result)
        assert unwrap(result) == "success"
        assert attempt == 3
