"""Provide the ExitScopeContextManager class and end-of-context utility functions."""


class ExitScopeContextManager:
    """A context manager that invokes an operation on a given instance when the control flow leaves the context."""
    def __init__(self, instance, operation):
        self.instance  = instance
        self.operation = operation

    def __enter__(self):
        return self.instance

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.operation(self.instance)


def close_when_done(instance):
    """Returns a context that will call `close` method of the passed instance when leaving the context."""
    return ExitScopeContextManager(instance, lambda x : x.close())


def shutdown_when_done(instance):
    """Returns a context that will call `shutdown` method of the passed instance when leaving the context."""
    return ExitScopeContextManager(instance, lambda x : x.shutdown())
