# Contratos versionados de Recursos

- `openapi/internal-v1.openapi.json`: snapshots de serviço e recurso implementados na frente REST da Etapa 2; consulta de operação permanece planejada.
- `common/`: envelope e erro HTTP v1.
- `vendor/ordens/saga-stock.v1.schema.json`: cópia semântica imutável do contrato
  produzido por Ordens e fixado na Infra. Recursos a empacota para validar seu próprio
  publisher sem importar código ou acessar outro checkout em runtime.

Os endpoints estão protegidos por audiência e escopo. As duas consultas de catálogo
estão funcionais; a consulta de operação continua respondendo 501 após autenticação
válida.

## OpenAPI 1.1.0

- `Problem` foi alinhado ao schema comum, incluindo os limites de `message` e os campos opcionais `operation_id` e `details`.
- respostas de sucesso e erro declaram `X-Correlation-ID`; as rotas internas exigem um UUID nesse header.
- respostas 422 e 500 passaram a ser declaradas explicitamente, sempre no formato `Problem`.
- `ServiceSnapshot` inclui preço, tempo estimado e estado de ativação autoritativos.

