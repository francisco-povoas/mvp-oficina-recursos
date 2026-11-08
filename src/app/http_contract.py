"""Conformidade HTTP comum do serviço com os contratos REST v1."""

from collections.abc import Awaitable, Callable
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

CORRELATION_HEADER = "X-Correlation-ID"

_DEFAULT_CODES = {
    400: "BAD_REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    409: "CONFLICT",
    422: "VALIDATION_ERROR",
    500: "INTERNAL_ERROR",
    501: "NOT_IMPLEMENTED",
    503: "DEPENDENCY_UNAVAILABLE",
}


def require_correlation_id(
    x_correlation_id: Annotated[UUID, Header(alias=CORRELATION_HEADER)],
) -> UUID:
    """Exige um UUID de correlação nas APIs internas contratadas."""
    return x_correlation_id


def _correlation_id(request: Request) -> str:
    return str(getattr(request.state, "correlation_id", uuid4()))


def problem_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    correlation_id = _correlation_id(request)
    response_headers = dict(headers or {})
    response_headers[CORRELATION_HEADER] = correlation_id
    return JSONResponse(
        status_code=status_code,
        content={
            "code": code,
            "message": message,
            "correlation_id": correlation_id,
        },
        headers=response_headers,
    )


def _http_problem(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    code = _DEFAULT_CODES.get(exc.status_code, "HTTP_ERROR")
    message = "Requisição não pôde ser processada"
    if isinstance(exc.detail, dict):
        candidate_code = exc.detail.get("code")
        candidate_message = exc.detail.get("message")
        if isinstance(candidate_code, str):
            code = candidate_code
        if isinstance(candidate_message, str):
            message = candidate_message
    elif isinstance(exc.detail, str) and exc.status_code < 500:
        message = exc.detail
    elif exc.status_code == 501 and isinstance(exc.detail, str):
        message = exc.detail

    return problem_response(
        request,
        status_code=exc.status_code,
        code=code,
        message=message,
        headers=exc.headers,
    )


def _validation_problem(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return problem_response(
        request,
        status_code=422,
        code="VALIDATION_ERROR",
        message="Dados da requisição inválidos",
    )


def _internal_problem(request: Request, exc: Exception) -> JSONResponse:
    return problem_response(
        request,
        status_code=500,
        code="INTERNAL_ERROR",
        message="Erro interno do serviço",
    )


def install_http_contract(app: FastAPI) -> None:
    """Instala correlação e envelopes de erro sem alterar regras de domínio."""

    @app.middleware("http")
    async def correlate(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        raw_correlation_id = request.headers.get(CORRELATION_HEADER)
        try:
            correlation_id = UUID(raw_correlation_id) if raw_correlation_id else uuid4()
        except ValueError:
            correlation_id = uuid4()
        request.state.correlation_id = correlation_id
        response = await call_next(request)
        response.headers[CORRELATION_HEADER] = str(correlation_id)
        return response

    app.add_exception_handler(StarletteHTTPException, _http_problem)
    app.add_exception_handler(RequestValidationError, _validation_problem)
    app.add_exception_handler(Exception, _internal_problem)
