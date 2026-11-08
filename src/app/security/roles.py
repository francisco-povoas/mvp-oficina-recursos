from enum import StrEnum


class Role(StrEnum):
    """Perfis aceitos nos tokens emitidos pelo serviço de Ordens."""

    ATENDENTE = "Atendente"
    MECANICO = "Mecanico"
    ADMINISTRADOR = "Administrador"
