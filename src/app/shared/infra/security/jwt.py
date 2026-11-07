from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from jose import JWTError, jwt


class InvalidToken(Exception):
    """Token ausente, inválido ou incompatível com a política local."""


class InsufficientScope(InvalidToken):
    """Token de serviço válido, mas sem todos os escopos exigidos."""


class JWTProvider:
    def __init__(
        self,
        secret_key: str,
        expire_minutes: int,
        algorithm: str,
        issuer: str = "mvp-oficina-ordens",
    ) -> None:
        if algorithm != "HS256":
            raise ValueError("Somente HS256 é suportado nesta fase")
        self._jwt = jwt
        self.secret_key = secret_key
        self.expire_minutes = expire_minutes
        self.algorithm = algorithm
        self.issuer = issuer

    def decode(self, token: str, audience: str | None = None) -> dict[str, Any]:
        try:
            return self._jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm],
                audience=audience,
                issuer=self.issuer,
                options={"verify_aud": audience is not None},
            )
        except JWTError as exc:
            raise InvalidToken from exc

    def create_access_token(
        self,
        data: dict[str, Any],
        *,
        expire_minutes: int | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        expire = now + timedelta(
            minutes=self.expire_minutes if expire_minutes is None else expire_minutes
        )
        to_encode = {
            **data.copy(),
            "iss": self.issuer,
            "iat": now,
            "exp": expire,
            "jti": str(uuid4()),
        }
        access_token = self._jwt.encode(
            to_encode,
            self.secret_key,
            algorithm=self.algorithm,
        )
        return {
            "expire": expire.timestamp(),
            "expires_in": max(0, int((expire - now).total_seconds())),
            "access_token": access_token,
            "token_type": "bearer",
        }

    def create_user_token(
        self,
        *,
        subject: str,
        roles: list[str],
        audience: str,
    ) -> dict[str, Any]:
        return self.create_access_token(
            {
                "sub": subject,
                "perfis": roles,
                "aud": audience,
                "token_use": "user",
            }
        )

    def create_service_token(
        self,
        *,
        client_id: str,
        audience: str,
        scopes: set[str],
        expire_minutes: int,
    ) -> dict[str, Any]:
        if not scopes:
            raise ValueError("Token de serviço exige ao menos um escopo")
        return self.create_access_token(
            {
                "sub": f"service:{client_id}",
                "aud": audience,
                "token_use": "service",
                "scope": " ".join(sorted(scopes)),
            },
            expire_minutes=expire_minutes,
        )

    def get_sub(
        self,
        token: str,
        *,
        audience: str,
    ) -> tuple[str, list[str]]:
        payload = self.decode(token, audience=audience)
        if payload.get("token_use") != "user":
            raise InvalidToken

        sub = payload.get("sub")
        if not isinstance(sub, str) or not sub:
            raise InvalidToken

        if sub.startswith("user_id:"):
            _, sub = sub.split(":", 1)

        roles = payload.get("perfis", [])
        if not isinstance(roles, list) or not all(
            isinstance(role, str) and role for role in roles
        ):
            raise InvalidToken

        try:
            return str(UUID(sub)), roles
        except ValueError as exc:
            raise InvalidToken from exc

    def get_service(
        self,
        token: str,
        *,
        audience: str,
        required_scopes: set[str],
    ) -> tuple[str, set[str]]:
        payload = self.decode(token, audience=audience)
        if payload.get("token_use") != "service":
            raise InvalidToken

        subject = payload.get("sub")
        raw_scope = payload.get("scope")
        if (
            not isinstance(subject, str)
            or not subject.startswith("service:")
            or not subject.removeprefix("service:")
            or not isinstance(raw_scope, str)
        ):
            raise InvalidToken

        scopes = {scope for scope in raw_scope.split() if scope}
        if not required_scopes.issubset(scopes):
            raise InsufficientScope

        return subject.removeprefix("service:"), scopes
