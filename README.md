# MVP Oficina — Recursos

Microsserviço responsável pelo catálogo de serviços e pelo estoque de peças e
insumos da oficina.

## Sumário

- [Responsabilidades](#responsabilidades)
- [Tecnologias e persistência](#tecnologias-e-persistência)
- [Estoque e reservas](#estoque-e-reservas)
- [Comunicação assíncrona](#comunicação-assíncrona)
- [Idempotência e concorrência](#idempotência-e-concorrência)
- [Execução local](#execução-local)
- [Swagger](#swagger)
- [Testes e cobertura](#testes-e-cobertura)

## Responsabilidades

| Contexto | Responsabilidade |
|---|---|
| Catálogo de serviços | cadastro, consulta, atualização, ativação e desativação |
| Catálogo de peças | cadastro e manutenção de preço e saldo |
| Estoque | saldos disponível e reservado, sem valores negativos |
| Reservas | reserva por operação, liberação, recebimento de pendência e consumo |
| Integração | consumer de comandos de Ordens, Outbox de resultados e contratos internos de catálogo |

Recursos não altera a OS nem mantém agregados de orçamento ou pagamento. Ele aplica
as invariantes do estoque e devolve o resultado; Ordens decide o estado de seu
próprio agregado.

## Tecnologias e persistência

- Python 3.12, FastAPI, Pydantic e Uvicorn;
- PostgreSQL 16, SQLModel/SQLAlchemy e Alembic;
- boto3 com SQS FIFO ou ElasticMQ local;
- JWT HS256 para APIs públicas e tokens de serviço para rotas internas;
- JSON Schema, OpenTelemetry, Pytest, pytest-cov e Ruff.

O PostgreSQL é exclusivo de Recursos e armazena catálogo, itens, saldos, reservas,
resultados por operação, Outbox e Inbox. Não há acesso ao PostgreSQL de Ordens nem ao
DynamoDB de Financeiro.

## Estoque e reservas

O serviço implementa quatro operações distribuídas:

1. **Reserva:** reduz o saldo disponível e aumenta o reservado. Se não houver saldo,
   registra o resultado de *backorder* sem deixar saldos negativos.
2. **Liberação:** devolve ao disponível uma reserva ativa.
3. **Recebimento:** uma peça recebida especificamente para a OS entra diretamente no
   saldo reservado, sem passar pelo disponível.
4. **Consumo:** reduz o saldo reservado e marca a reserva como consumida.

Reserva inexistente, quantidade inválida e conflitos de payload têm respostas
explícitas. Repetir uma operação terminal não movimenta o estoque novamente.

## Comunicação assíncrona

O consumer lê a FIFO Ordens → Recursos e processa:

- `ReserveItem.v1`;
- `ReleaseReservation.v1`;
- `ReceiveBackorderedItem.v1`;
- `ConsumeReservation.v1`.

Na mesma transação PostgreSQL, ele registra a Inbox, aplica o efeito e grava o
resultado na Outbox. O publisher envia pela FIFO Recursos → Ordens:

- `ResourceReserved.v1`, `ItemBackordered.v1` ou `ReservationRejected.v1`;
- `ReservationReleased.v1`;
- `BackorderedItemReceived.v1`;
- `ReservationConsumed.v1`.

O transporte é *at-least-once*: a mensagem só recebe ACK após o commit local. Falhas
transitórias são repetidas; após a política de redrive, seguem para a DLQ.

## Idempotência e concorrência

- `operation_id` identifica a operação de negócio e impede movimentação duplicada;
- a Inbox deduplica `(consumer_name, message_id)`;
- uma nova mensagem com a mesma operação e o mesmo payload devolve o resultado
  persistido;
- a mesma operação com payload divergente é conflito e não altera o primeiro efeito;
- alterações concorrentes no mesmo item usam lock pessimista
  `SELECT ... FOR UPDATE`;
- Outbox, efeito de estoque e conclusão da Inbox são atômicos na transação local.

## Execução local

Para executar o sistema distribuído completo, use o
[Kubernetes local de Infra](../mvp-oficina-infra/README.md#subir-o-ambiente-no-minikube).

Para preparar o serviço no host:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
make install
```

As configurações estão em `.env.example`. A API requer PostgreSQL migrado; o
container runtime aplica Alembic e inicia o Uvicorn por `scripts/start.sh`. Publisher
e consumer são processos separados:

```bash
python -m app.publisher
python -m app.consumer
```

Para exercitar os fluxos distribuídos sem configurar manualmente a topologia, use o
BDD de Infra:

```bash
cd ../mvp-oficina-infra
make test-bdd
```

## Swagger

Com o Checkpoint A ativo:

```bash
kubectl --context=minikube -n mvp-oficina port-forward service/recursos-api 8002:8002
```

Swagger UI: <http://127.0.0.1:8002/docs>

OpenAPI JSON: <http://127.0.0.1:8002/openapi.json>

O Kong publica apenas `/api/v1/servicos` e `/api/v1/itens-estoque`; ele não
publica Swagger, OpenAPI ou rotas internas.

## Testes e cobertura

```bash
make lint
make test
```

O gate de cobertura de branches, configurado em `pyproject.toml`, é 80%. Para a
suíte isolada com PostgreSQL real, migrations e ElasticMQ quando requerido:

```bash
docker compose -f compose.test.yaml run --rm --build tests
docker compose -f compose.test.yaml down --volumes --remove-orphans
```

## Documentação relacionada

- [Arquitetura distribuída](../mvp-oficina-infra/docs/diagrams/arquitetura.md)
- [Saga e comunicação](../mvp-oficina-infra/docs/diagrams/saga-comunicacao.md)
- [Contratos de Recursos](contracts/README.md)
- [README principal de Infra](../mvp-oficina-infra/README.md)
