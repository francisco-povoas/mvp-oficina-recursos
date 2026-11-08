# Limite arquitetural de Recursos

Recursos é dono do catálogo de serviços, peças e insumos, saldos de estoque, reservas, recebimento, liberação e consumo.

O serviço não altera a ordem de serviço nem seus itens operacionais. Ele persiste resultados por `operation_id` e devolve fatos; Ordens decide o estado de seus próprios agregados.

Nesta etapa, estruturas de reserva e Outbox podem ser preparadas. Publicação de eventos, Inbox, workers, SQS/ElasticMQ e participação executável na Saga permanecem na Etapa 2.
