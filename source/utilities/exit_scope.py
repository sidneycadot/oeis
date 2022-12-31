
class ExitScopeContextManager:

    def __init__(self, instance, operation):
        self.instance  = instance
        self.operation = operation

    def __enter__(self):
        return self.instance

    def __exit__(self, exc_type, exc_value, traceback):
        self.operation(self.instance)


def close_when_done(instance):
    return ExitScopeContextManager(instance, lambda x : x.close())


def shutdown_when_done(instance):
    return ExitScopeContextManager(instance, lambda x : x.shutdown())
