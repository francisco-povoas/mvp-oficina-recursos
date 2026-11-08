# presentation/http/schemas/common.py

from uuid import UUID

from pydantic import BaseModel


class ErrorResponse(BaseModel):
    code: str
    message: str
    correlation_id: UUID
    operation_id: UUID | None = None
