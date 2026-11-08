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

## APIs e contratos internos

As APIs públicas preservam os perfis locais da oficina. As rotas `/internal/v1` exigem
token de serviço com audiência e escopo corretos. O OpenAPI versionado em `contracts/`
define consultas internas de catálogo e resultado de operação, além dos schemas comuns
de correlação e erro.

Os endpoints distribuídos ainda planejados retornam explicitamente `501
INTEGRATION_PLANNED`; contrato não equivale a integração ativa. Publicação de eventos,
Outbox/Inbox funcionais, workers, SQS/ElasticMQ e participação na Saga pertencem à
Etapa 2.

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
unitários, contratuais e de integração, gate global de cobertura e build sem publicar a
imagem.

## Próxima etapa funcional

A Etapa 2 ligará os contratos à comunicação real, implementará consumers idempotentes,
Outbox/Inbox, workers e os comandos/eventos de reserva, recebimento, liberação e
consumo. Na Etapa 1 essas integrações permanecem deliberadamente não funcionais.
