from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.modules.estoque.domain.entities.reserva import Reserva, StatusReserva
from app.modules.estoque.domain.exceptions import ItemEstoqueInvalidoError


def criar_reserva(*, quantidade: int = 2) -> Reserva:
    agora = datetime.now(UTC)
    return Reserva(
        operation_id=uuid4(),
        ordem_servico_id=uuid4(),
        ordem_servico_item_id=uuid4(),
        item_estoque_id=uuid4(),
        quantidade=quantidade,
        status=StatusReserva.PENDENTE,
        criado_em=agora,
        atualizado_em=agora,
    )


def test_reserva_identifica_saldo_suficiente_sem_mutar_original() -> None:
    reserva = criar_reserva()

    processada = reserva.reservar(saldo_disponivel=2)

    assert reserva.status == StatusReserva.PENDENTE
    assert processada.status == StatusReserva.RESERVADA


def test_reserva_identifica_falta_de_estoque() -> None:
    assert criar_reserva(quantidade=3).reservar(2).status == StatusReserva.SEM_ESTOQUE


def test_reserva_nao_aceita_quantidade_invalida() -> None:
    with pytest.raises(ItemEstoqueInvalidoError):
        criar_reserva(quantidade=0)


def test_reserva_confirmada_pode_ser_liberada_ou_consumida() -> None:
    reserva = criar_reserva().reservar(10)

    assert reserva.liberar().status == StatusReserva.LIBERADA
    assert reserva.consumir().status == StatusReserva.CONSUMIDA
