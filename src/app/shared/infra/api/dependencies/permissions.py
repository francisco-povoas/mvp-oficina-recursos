
from typing import Annotated

from fastapi import Depends, HTTPException

from app.security.roles import Role
from app.shared.dtos.usuario_autenticado import UsuarioAutenticado
from app.shared.infra.api.dependencies.auth import get_usuario_autenticado


class RequireRoles:
    def __init__(self, *roles: Role):
        self.roles = {role.value for role in roles}

    async def __call__(
        self,
        usuario: Annotated[UsuarioAutenticado, Depends(get_usuario_autenticado)],
    ) -> UsuarioAutenticado:
        if not self.roles.intersection(usuario.perfis):
            raise HTTPException(status_code=403, detail="Permissão insuficiente")

        return usuario


# Utilizar quando precisar do usuario autenticado
AdminDep = Annotated[
    UsuarioAutenticado,
    Depends(RequireRoles(Role.ADMINISTRADOR)),
]

AtendenteDep = Annotated[
    UsuarioAutenticado,
    Depends(RequireRoles(Role.ATENDENTE)),
]

MecanicoDep = Annotated[
    UsuarioAutenticado,
    Depends(RequireRoles(Role.MECANICO)),
]
