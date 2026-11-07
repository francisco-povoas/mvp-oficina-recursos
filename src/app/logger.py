import logging


def setup_logger(name: str | None = None) -> logging.Logger:
    """Retorna logger padrão sem adicionar handlers globais no import."""

    return logging.getLogger(name)
