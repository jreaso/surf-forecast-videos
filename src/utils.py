import re
from datetime import datetime


def to_snake_case(text: str) -> str:
    """
    Converts a camelCase or PascalCase string to snake_case.

    :param text: The input string in camelCase or PascalCase.
    :return: The input string converted to snake_case.
    """
    return re.sub(r'(.)([A-Z][a-z]+)|([a-z0-9])([A-Z])', r'\1\3_\2\4', text).lower()


def datetime_serializer(obj):
    """
    Serialize a datetime object to ISO 8601 format.

    This function is intended to be used as a custom serializer with the json.dumps() function. It checks if the given
    object is an instance of the datetime class and converts it to its ISO 8601 representation. If the object is not a
    datetime instance, a TypeError is raised. It can then be the default method in json.dumps().

    :param obj: The object to be serialized.
    :return: A string representing the ISO 8601 format of the datetime object.
    :raises TypeError: If the input object is not a datetime instance.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO 8601 format
    raise TypeError("Type not serializable")
