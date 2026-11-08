from app.modules.servicos.infrastructure.db.repositories.servico_repo import ServicoRepo
from app.shared.infra.db import DBSession
from app.shared.infra.db.base_uow import BaseUnitOfWork


class ServicoUnitOfWork(BaseUnitOfWork):
    def __init__(self, session: DBSession) -> None:
        super().__init__(session)
        self.servico_repo = ServicoRepo(session)


def servico_uow_factory(session: DBSession) -> ServicoUnitOfWork:
    return ServicoUnitOfWork(session)
