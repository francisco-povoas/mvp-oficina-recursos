# MVP Oficina — Recursos

Microsserviço responsável pelo catálogo de serviços, peças e insumos, estoque, reservas, recebimento, liberação e consumo.

## Estado atual

Este repositório contém somente a estrutura mínima inicial da Etapa 1 da Fase 4. Nenhum módulo do monólito foi segregado ainda.

## Estrutura inicial

- `src/`: código-fonte futuro do serviço;
- `tests/`: testes próprios do serviço;
- `docs/`: contratos e documentação próprios.

O serviço terá PostgreSQL próprio e não poderá alterar ordens de serviço nem consultar bancos, modelos ORM ou entidades de domínio dos demais serviços.
