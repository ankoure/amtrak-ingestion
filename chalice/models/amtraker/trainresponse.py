from typing import Dict, List
from pydantic import RootModel
from .train import Train


class TrainResponse(RootModel[Dict[str, List[Train]]]):
    """
    Response from Amtraker API.
    Maps train numbers (as strings) to lists of Train objects.
    Example: {'1': [Train(...), Train(...)], '2': [Train(...)]}
    """

    pass
