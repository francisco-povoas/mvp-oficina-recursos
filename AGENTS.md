# Instruções — mvp-oficina-recursos

- Preserve o monólito original; use-o somente como fonte de regras e testes legados.
- Este repositório é dono do catálogo, estoque, reservas, recebimento, liberação e consumo.
- Preserve as invariantes de saldo e a idempotência por `operation_id`.
- Não altere ordens de serviço nem mantenha agregados de Ordens ou Financeiro.
- Não compartilhe domínio, ORM, migrations ou banco com outros serviços.
- Integrações externas devem usar contratos versionados e transações locais.
- Nunca versione segredos, tokens, credenciais ou arquivos `.env`.
- Use o fluxo `feature/* -> main`; não faça push adicional sem autorização da atividade em curso.
