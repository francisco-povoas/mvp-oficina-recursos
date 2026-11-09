from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine


async def test_alembic_aplica_schema_independente_desde_banco_vazio(
    db_engine: AsyncEngine,
) -> None:
    async with db_engine.connect() as connection:
        revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))

        def inspect_schema(sync_connection):
            inspector = inspect(sync_connection)
            return {
                "tables": set(inspector.get_table_names()),
                "item_checks": {
                    constraint["name"]
                    for constraint in inspector.get_check_constraints("item_estoque")
                },
                "reservation_checks": {
                    constraint["name"]
                    for constraint in inspector.get_check_constraints("reserva_estoque")
                },
                "reservation_fks": inspector.get_foreign_keys("reserva_estoque"),
                "reservation_pk": inspector.get_pk_constraint("reserva_estoque"),
                "outbox_indexes": {
                    index["name"] for index in inspector.get_indexes("outbox_message")
                },
                "outbox_checks": {
                    constraint["name"]
                    for constraint in inspector.get_check_constraints("outbox_message")
                },
                "outbox_columns": {
                    column["name"] for column in inspector.get_columns("outbox_message")
                },
                "inbox_message_checks": {
                    constraint["name"]
                    for constraint in inspector.get_check_constraints("inbox_message")
                },
                "inbox_effect_checks": {
                    constraint["name"]
                    for constraint in inspector.get_check_constraints("inbox_effect")
                },
                "inbox_indexes": {
                    index["name"] for index in inspector.get_indexes("inbox_message")
                },
            }

        schema = await connection.run_sync(inspect_schema)

    assert revision == "20261109_04_inbox"
    assert schema["tables"] == {
        "alembic_version",
        "catalogo_servico",
        "item_estoque",
        "reserva_estoque",
        "outbox_message",
        "inbox_message",
        "inbox_effect",
    }
    assert {
        "ck_item_saldo_disponivel",
        "ck_item_saldo_reservado",
        "ck_item_quantidade_minima",
        "ck_item_valor_nao_negativo",
    } <= schema["item_checks"]
    assert {
        "ck_reserva_quantidade_positiva",
        "ck_reserva_status_valido",
    } <= schema["reservation_checks"]
    assert schema["reservation_pk"]["constrained_columns"] == ["operation_id"]
    assert {fk["referred_table"] for fk in schema["reservation_fks"]} == {"item_estoque"}
    assert {"ix_outbox_pending", "ix_outbox_claimable"} <= schema["outbox_indexes"]
    assert {
        "ck_outbox_claim_completo",
        "ck_outbox_claim_expiracao_valida",
        "ck_outbox_falha_com_erro",
    } <= schema["outbox_checks"]
    assert "saga_id" in schema["outbox_columns"]
    assert {
        "ck_inbox_message_version",
        "ck_inbox_receive_count_positivo",
        "ck_inbox_message_status",
        "ck_inbox_message_processing",
    } <= schema["inbox_message_checks"]
    assert {"ck_inbox_effect_status", "ck_inbox_effect_processing"} <= schema[
        "inbox_effect_checks"
    ]
    assert {"ix_inbox_message_business_key", "ix_inbox_message_operation_id"} <= schema[
        "inbox_indexes"
    ]


async def test_schema_nao_possui_tabelas_orm_ou_fk_para_ordens(
    db_engine: AsyncEngine,
) -> None:
    async with db_engine.connect() as connection:
        def inspect_boundaries(sync_connection):
            inspector = inspect(sync_connection)
            tables = set(inspector.get_table_names())
            fks = inspector.get_foreign_keys("reserva_estoque")
            return tables, fks

        tables, foreign_keys = await connection.run_sync(inspect_boundaries)

    assert tables.isdisjoint(
        {"ordem_servico", "ordem_servico_item", "cliente", "veiculo", "usuario"}
    )
    assert all(fk["referred_table"] != "ordem_servico" for fk in foreign_keys)
    assert all("ordem_servico_id" not in fk["constrained_columns"] for fk in foreign_keys)
