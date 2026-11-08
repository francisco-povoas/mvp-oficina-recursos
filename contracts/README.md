# Contratos versionados de Recursos

- `openapi/internal-v1.openapi.json`: snapshots de catálogo e consulta autoritativa do resultado de operação.
- `common/`: envelope e erro HTTP v1.

Os endpoints estão protegidos e respondem 501 após autenticação válida porque a consulta funcional pertence à Etapa 2. O contrato das mensagens coordenadas por Ordens está materializado no repositório de Ordens; a centralização imutável para testes distribuídos será feita em Infra na frente própria.

## OpenAPI 1.0.1

- `Problem` foi alinhado ao schema comum, incluindo os limites de `message` e os campos opcionais `operation_id` e `details`.
- respostas de sucesso e erro declaram `X-Correlation-ID`; as rotas internas exigem um UUID nesse header.
- respostas 422 e 500 passaram a ser declaradas explicitamente, sempre no formato `Problem`.

