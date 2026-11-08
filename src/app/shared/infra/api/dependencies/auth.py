"""Dependências locais para tokens de usuário e de serviço."""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, OAuth2PasswordBearer

from app.shared.dtos.usuario_autenticado import UsuarioAutenticado
from app.shared.infra.security.jwt import (
    InsufficientScope,
    InvalidToken,
    JWTProvider,
)

_credentials_exception = HTTPException(
    status_code=401,
    detail="Token inválido, expirado ou incompatível",
    headers={"WWW-Authenticate": "Bearer"},
)
_scope_exception = HTTPException(
    status_code=403,
    detail="Escopo de serviço insuficiente",
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")
service_bearer = HTTPBearer(auto_error=False)
Oauth2Token = Annotated[str, Depends(oauth2_scheme)]
ServiceCredential = Annotated[
    HTTPAuthorizationCredentials | None,
    Depends(service_bearer),
]


@dataclass(frozen=True)
class ServicePrincipal:
    client_id: str
    scopes: frozenset[str]


def get_jwt_provider(request: Request) -> JWTProvider:
    settings = request.app.state.settings
    return JWTProvider(
        settings.jwt_secret_key,
        settings.jwt_access_token_expire_minutes,
        settings.jwt_algorithm,
        settings.jwt_issuer,
    )


TokenProvider = Annotated[JWTProvider, Depends(get_jwt_provider)]


def get_usuario_autenticado(
    request: Request,
    token: Oauth2Token,
    token_provider: TokenProvider,
) -> UsuarioAutenticado:
    """Aceita exclusivamente JWT de usuário para a audiência pública."""
    try:
        user_id, perfis = token_provider.get_sub(
            token,
            audience=request.app.state.settings.jwt_user_audience,
        )
        return UsuarioAutenticado(id=user_id, perfis=perfis)
    except InvalidToken:
        raise _credentials_exception


class RequireServiceScopes:
    """Política default-deny para endpoints internos."""

    def __init__(self, *scopes: str) -> None:
        if not scopes:
            raise ValueError("Endpoint interno exige ao menos um escopo")
        self.scopes = frozenset(scopes)

    async def __call__(
        self,
        request: Request,
        credential: ServiceCredential,
        token_provider: TokenProvider,
    ) -> ServicePrincipal:
        if credential is None:
            raise _credentials_exception
        try:
            client_id, granted = token_provider.get_service(
                credential.credentials,
                audience=request.app.state.settings.service_name,
                required_scopes=set(self.scopes),
            )
        except InsufficientScope:
            raise _scope_exception
        except InvalidToken:
            raise _credentials_exception
        return ServicePrincipal(client_id=client_id, scopes=frozenset(granted))


UsuarioAutenticadoDep = Annotated[
    UsuarioAutenticado,
    Depends(get_usuario_autenticado),
]
