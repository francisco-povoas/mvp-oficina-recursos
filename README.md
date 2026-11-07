# MVP Oficina — Recursos

Microsserviço responsável pelo catálogo de serviços, peças e insumos, estoque, reservas, recebimento, liberação e consumo.

## Estado atual

O esqueleto executável independente da Etapa 1 está disponível. A extração de catálogo e estoque será incremental e preservará os limites descritos em `docs/architecture-boundary.md`.

## Estrutura inicial

- `src/app/`: aplicação FastAPI e futuras camadas de domínio;
- `tests/`: testes próprios do serviço;
- `docs/`: contratos e documentação próprios;
- `compose.yaml`: API e PostgreSQL de desenvolvimento;
- `compose.test.yaml`: testes isolados com PostgreSQL próprio.

O serviço terá PostgreSQL próprio e não poderá alterar ordens de serviço nem consultar bancos, modelos ORM ou entidades de domínio dos demais serviços.

## Comandos

```bash
make install
make lint
make test
make build
docker compose up --build
```

O health check fica disponível em `GET /health` na porta `8002`.
