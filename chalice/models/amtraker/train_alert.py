from pydantic import BaseModel


class TrainAlert(BaseModel):
    message: str
