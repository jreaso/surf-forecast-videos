import re

def to_snake_case(text: str) -> str:
    """
    Converts a camelCase or PascalCase string to snake_case.

    Args:
        text (str): The input string in camelCase or PascalCase.

    Returns:
        str: The input string converted to snake_case.
    """
    snake_case = re.sub(r'(.)([A-Z][a-z]+)|([a-z0-9])([A-Z])', r'\1\3_\2\4', text)
    return snake_case.lower()