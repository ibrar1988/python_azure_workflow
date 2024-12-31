def handle_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise RuntimeError(f"Error in {func.__name__}: {str(e)}") from e
    return wrapper