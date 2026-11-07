# Contratos versionados de Recursos

- `openapi/internal-v1.openapi.json`: snapshots de catálogo e consulta autoritativa do resultado de operação.
- `common/`: envelope e erro HTTP v1.

Os endpoints estão protegidos e respondem 501 após autenticação válida porque a consulta funcional pertence à Etapa 2. O contrato das mensagens coordenadas por Ordens está materializado no repositório de Ordens; a centralização imutável para testes distribuídos será feita em Infra na frente própria.

