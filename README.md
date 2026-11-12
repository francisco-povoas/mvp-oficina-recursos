# MVP Oficina — Recursos

Microsserviço da Fase 4 responsável pelo catálogo de serviços e pelo estoque de peças e
insumos: saldos disponível/reservado, reservas, recebimento, liberação e consumo. Ele
não altera ordens de serviço nem mantém agregados financeiros.

## Estado da Etapa 1

Os módulos `servicos` e `estoque` estão extraídos e organizados por `domain`,
`application`, `infrastructure` e `presentation`. O serviço usa PostgreSQL próprio,
SQLModel e migrations Alembic, sem banco, ORM ou entidade compartilhada com os demais
serviços.

As invariantes impedem saldos negativos. Operações de estoque que disputam o mesmo
item usam lock pessimista (`SELECT ... FOR UPDATE`), e a reserva persistente usa
`operation_id` como chave idempotente. O modelo cobre os estados e saldos necessários
para reservar, receber, liberar e consumir recursos.

A Outbox transacional e o publisher independente da Etapa 2 estão ativos. O publisher
usa claim com lease e `FOR UPDATE SKIP LOCKED`, valida o schema v1 consumido de Ordens
e publica os seis resultados aprovados na FIFO de Ordens. O Checkpoint D acrescenta o
consumer Ordens→Recursos e a Inbox PostgreSQL persistente. Os quatro handlers
funcionais de reserva, recebimento, liberação e consumo estão registrados na composição
padrão e produzem os resultados aprovados pela Outbox transacional.

## APIs e contratos internos

As APIs públicas preservam os perfis locais da oficina. As rotas `/internal/v1` exigem
token de serviço com audiência e escopo corretos. O OpenAPI versionado em `contracts/`
define consultas internas de catálogo e resultado de operação, além dos schemas comuns
de correlação e erro.

Os snapshots internos de serviço e recurso estão ativos e não expõem saldos. A
consulta de resultado de operação ainda planejada retorna explicitamente `501
INTEGRATION_PLANNED`. As operações de estoque assíncronas usam o consumer funcional,
e o publisher envia os resultados já persistidos na Outbox.

## Estrutura

| Caminho | Responsabilidade |
|---|---|
| `src/app/modules/servicos/` | catálogo de serviços em camadas de Clean Architecture |
| `src/app/modules/estoque/` | itens, saldos, reservas e persistência transacional |
| `alembic/` | baseline exclusivo do PostgreSQL de Recursos |
| `contracts/` | OpenAPI e schemas versionados das interfaces internas |
| `tests/` | testes unitários, de contratos e de integração PostgreSQL |
| `.github/workflows/ci.yml` | Ruff, suítes com cobertura mínima de 80% e build da imagem |

Os manifests Kubernetes específicos serão materializados somente na Etapa 4, junto da
preparação do deployment AWS. A Etapa 1 não contém placeholders Kubernetes.

## Execução e validação

Requer Python 3.12. Use os arquivos `.env.example` somente como modelo e não versione
credenciais.

```bash
make install
make lint
make test
make build
docker compose up --build
```

Para a suíte isolada com PostgreSQL e ciclo Alembic:

```bash
docker compose -f compose.test.yaml run --rm --build tests
docker compose -f compose.test.yaml down --volumes --remove-orphans
```

O health check fica em `GET /health` na porta `8002`. O CI executa lint, testes
unitários, contratuais e de integração com PostgreSQL/ElasticMQ, gate global de
cobertura e build sem publicar a imagem.

## Publisher da Outbox

O publisher roda separado do FastAPI por `python -m app.publisher` ou
`scripts/start-publisher.sh`. As variáveis, timeouts, lote, lease, polling e retry
limitado estão em `.env.example`; `SQS_ENDPOINT_URL` é permitido apenas fora de
produção. SIGTERM/SIGINT encerram o loop sem iniciar novo lote.

A semântica é at-least-once. Envio aceito pelo SQS seguido de falha na confirmação
PostgreSQL deixa o lease expirar e permite reenvio do mesmo `message_id`; a futura
Inbox, não a janela de deduplicação FIFO, impedirá efeitos de negócio repetidos.

## Consumer e Inbox

O processo independente inicia por `python -m app.consumer` ou
`scripts/start-consumer.sh`. Ele usa long polling, concorrência limitada, renovação do
visibility timeout e ACK somente após commit. A Inbox separa a deduplicação de
transporte `(consumer_name, message_id)` da idempotência de negócio por chave semântica
(`reserve`, `release`, `receive-backordered` ou `consume` + `operation_id`). Mensagem
nova com a mesma operação e payload não repete o efeito; payload divergente é conflito
e segue o redrive/DLQ.

O handler recebe a mesma sessão PostgreSQL da Inbox, portanto efeito local, eventual
Outbox de resposta e conclusão da Inbox são confirmados atomicamente. Tipos não
aprovados, contrato inválido, conflito de idempotência ou falha técnica permanecem sem
ACK e seguem retry/redrive; não são convertidos em sucesso ou rejeição de negócio.
