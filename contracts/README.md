# Contratos versionados de Recursos

- `openapi/internal-v1.openapi.json`: snapshot de serviço implementado na frente REST da Etapa 2; snapshot de recurso e consulta de operação permanecem planejados.
- `common/`: envelope e erro HTTP v1.

Os endpoints estão protegidos por audiência e escopo. Somente a consulta de serviço do catálogo está funcional nesta frente; os demais continuam respondendo 501 após autenticação válida.

## OpenAPI 1.1.0

- `Problem` foi alinhado ao schema comum, incluindo os limites de `message` e os campos opcionais `operation_id` e `details`.
- respostas de sucesso e erro declaram `X-Correlation-ID`; as rotas internas exigem um UUID nesse header.
- respostas 422 e 500 passaram a ser declaradas explicitamente, sempre no formato `Problem`.
- `ServiceSnapshot` inclui preço, tempo estimado e estado de ativação autoritativos.

