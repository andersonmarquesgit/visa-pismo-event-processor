
# Event Processor

Um processador de eventos rodando como serviço: sempre disponível para receber eventos de tipos diferentes, validar, triar e persistir. A ideia é preparar o evento para ser entregue ao cliente final por outro serviço depois.

Demonstrando essencialmente:

- **Publicação** em um **exchange `topic`** (routing key = tipo do evento).
- **Consumo** com **competing consumers** (múltiplas instâncias do `processor` lendo a mesma fila).
- **ACK/NACK manual** para garantir **at-least-once** e controlar falhas.
- **DLQ (Dead Letter Queue)** para eventos inválidos/falhos, com trilha para inspeção/replay.
- **Idempotência** por **dedupe no Postgres** (`UNIQUE(event_id)` + `ON CONFLICT DO NOTHING`).

## Como executar (rápido)

### Requisitos

- Docker + Docker Compose
- Go **1.26+** (o repo usa `toolchain go1.26.2` em `go.mod`)

### Subir tudo e publicar eventos automaticamente

```bash
make up
```

O que acontece no `make up`:

- Sobe a stack com `docker compose up -d --build` (**Postgres, RabbitMQ e `processor-1/2/3`**)
- Em seguida roda o `producer` **no host** com `go run ./cmd/producer`

Observações:

- O `producer` roda em loop e **mantém o terminal ocupado**. Para parar, use `Ctrl+C`.
- Para rodar o `producer` em outro terminal, prefira subir só a infra (ver abaixo).

### Subir só a infra (sem producer)

```bash
docker compose up -d --build
```

Em outro terminal, rode o producer:

```bash
make producer
```

### Rodar um processor local (sem Docker)

Com RabbitMQ e Postgres rodando via compose, você pode rodar um `processor` local:

```bash
make processor
```

Opcionalmente, configure as variáveis (exemplo):

```bash
export WORKER_ID=processor-local
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/
export DATABASE_URL="postgres://events:events@localhost:5432/events-db?sslmode=disable"
make processor
```

### Acessos úteis

- **RabbitMQ Management UI**: `http://localhost:15672` (user/pass: `guest` / `guest`)
- **Postgres**: `localhost:5432` (user/pass/db: `events` / `events` / `events-db`)

### Logs dos containers

```bash
make logs
```

### Parar a stack

```bash
make down
```

Nota: `make down` não remove volumes. Para reset total (inclui volume do Postgres), veja a seção “Reset completo”.

## Arquitetura

### Componentes

- **Producer** (`cmd/producer`)
    - Publica eventos JSON no exchange `events-exchange` (tipo `topic`).
    - Publica eventos válidos com `routingKey = eventType` e injeta `id` único por evento.
    - Também publica eventos inválidos (payload malformado) para simular falhas.
- **Processor** (`cmd/processor`)
    - Consome da fila `events-processing-queue` com **ACK/NACK manual**.
    - Persiste um registro de “evento processado” no Postgres.
    - Em falha (payload inválido, sem `id`, erro de persistência), **NACK com requeue=false** → DLQ.
- **RabbitMQ**
    - Exchange: `events-exchange` (tipo `topic`).
    - Fila principal: `events-processing-queue`.
    - DLQ: `events-dead-letter-queue` (recebe mensagens rejeitadas/expiradas).

- **PostgreSQL**
    - Tabela `processed_events` para registrar processamento e garantir **idempotência** via `event_id UNIQUE`.

  Principal estratégia:

    ```sql
    UNIQUE(event_id)
    ON CONFLICT DO NOTHING
    ```

### Topologia RabbitMQ (resumo)

- Exchange: `events-exchange` (**`topic`**)
- Binding: fila `events-processing-queue` ligada com **binding key `#`** (consome tudo publicado no exchange)
- DLQ:
    - `events-processing-queue` tem argumentos:
        - `x-dead-letter-exchange = ""` (default exchange)
        - `x-dead-letter-routing-key = events-dead-letter-queue`
    - Quando o consumer faz `NACK(requeue=false)`, a mensagem vai para `events-dead-letter-queue`.

Todos os tipos de eventos são publicados na exchange e consumidos pela mesma fila.
Exemplos de routing keys:

```text
transaction.authorized
user.created
monitoring.alert
```

### Processadores Concorrentes

O projeto executa múltiplas instâncias do Processor consumindo da mesma fila:

- processor-1
- processor-2
- processor-3

RabbitMQ distribui as mensagens entre eles de forma automática.

Exemplo de logs:

```text
2026/05/08 04:13:05 worker=processor-1 | event_id=... | type=transaction.authorized | status=processed | duration_ms=230
2026/05/08 04:13:07 worker=processor-3 | event_id=... | type=transaction.authorized | status=processed | duration_ms=162
2026/05/08 04:13:11 worker=processor-1 | event_id=... | type=monitoring.alert | status=processed | duration_ms=376
2026/05/08 04:13:13 worker=processor-3 | event_id=... | type=user.created | status=processed | duration_ms=269
2026/05/08 04:13:15 worker=processor-2 | event_id=... | type=user.created | status=processed | duration_ms=289
```

Isso demonstra:

- escala horizontal
- distribuição de carga
- processamento em paralelo

## Fluxo (fim a fim)

1. O `producer` lê um JSON de `fake-events/valid/*` ou `fake-events/invalid/*`.
2. Para evento válido:
    - Define `event.id` e `event.timestamp`.
    - Publica no exchange `events-exchange` com `routingKey = event.eventType`.
3. Para evento inválido:
    - Publica payload malformado com `routingKey = malformed.json`.
4. O `processor` consome mensagens da `events-processing-queue`.
5. O handler:
    - Faz `json.Unmarshal`.
    - Valida existência de `event.id`.
    - Persiste em `processed_events` usando `ON CONFLICT (event_id) DO NOTHING` (idempotência).
6. Resultado:
    - Se deu tudo certo: **`ACK`**
    - Se falhou em qualquer ponto: **`NACK(requeue=false)`** → mensagem segue para **DLQ**

## Diagrama (alto nível)

```mermaid
flowchart LR
  P[producer] -->|publish\nroutingKey=eventType| X((events-exchange\ntopic))
  X -->|binding #| Q[events-processing-queue]

  subgraph Processors["processors (competing consumers)"]
    C1[processor-1]
    C2[processor-2]
    C3[processor-3]
  end

  Q --> C1
  Q --> C2
  Q --> C3

  C1 -->|ACK| Q
  C2 -->|ACK| Q
  C3 -->|ACK| Q

  C1 -->|NACK requeue=false| DLQ[events-dead-letter-queue]
  C2 -->|NACK requeue=false| DLQ
  C3 -->|NACK requeue=false| DLQ

  C1 -->|insert ON CONFLICT DO NOTHING| PG[(PostgreSQL\nprocessed_events)]
  C2 -->|insert ON CONFLICT DO NOTHING| PG
  C3 -->|insert ON CONFLICT DO NOTHING| PG
```

```text
+-----------+
| Producer  |
+-----------+
      |
      v
+-------------------+
| RabbitMQ Exchange |
|  events-exchange  |
+-------------------+
      |
      v
+---------------------------+
| events-processing-queue   |
+---------------------------+
      |
      +-------------------+
      |         |         |
      v         v         v
+-------------+ +-------------+ +-------------+
| processor-1 | | processor-2 | | processor-3 |
+-------------+ +-------------+ +-------------+
      |
      v
+------------------+
| PostgreSQL       |
| processed_events |
+------------------+

Invalid messages
      |
      v
+---------------------------+
| events-dead-letter-queue  |
+---------------------------+
```

### Reset completo (inclui volume do Postgres)

O Postgres executa o SQL de init **só na primeira subida com volume vazio**. Para resetar tudo:

```bash
docker compose down -v && docker compose up -d --build
```

## Comandos úteis (Make)

```bash
make up
make producer
make processor
make logs
make down
make test
make coverage
make tidy
```

O que cada um faz:

- **`make up`**: sobe a infra + processors via Docker Compose e depois roda o `producer` localmente
- **`make producer`**: roda `go run ./cmd/producer` (publica eventos de demo a cada ~2s; ~10% malformados)
- **`make processor`**: roda `go run ./cmd/processor` (útil para rodar um worker local extra)
- **`make logs`**: segue logs do Docker Compose (`--tail=200`)
- **`make down`**: derruba containers (sem remover volumes)
- **`make test`**: `go test ./...`
- **`make coverage`**: gera `coverage.out` e imprime o resumo no terminal
- **`make tidy`**: `go mod tidy`

---

## Exemplo de logs

### Processamento com Sucesso

```text
worker=processor-2 | event_id=1e57d3ad-0110-494c-b239-5e8bfa1aeb49 | type=transaction.authorized | status=processed | duration_ms=450
```

### DLQ flow

```text
handler failed, sending to dead-letter queue "events-dead-letter-queue": invalid event payload
```

## Concorrência (Processors concorrentes)

O `docker-compose.yml` sobe **3 instâncias** (`processor-1`, `processor-2`, `processor-3`) consumindo **a mesma fila** `events-processing-queue`.

- Cada instância recebe um `WORKER_ID` para facilitar observação nos logs.
- A distribuição de mensagens entre instâncias é natural do RabbitMQ (modelo **competing consumers**).
- O handler adiciona um pequeno atraso aleatório (100–500ms) para tornar a distribuição mais visível em demos.

### O que isso entrega

- **Escala horizontal**: para aumentar throughput, sobe mais réplicas.
- **Isolamento de falhas**: uma instância travando não para a fila inteira.
- **Simplicidade**: não precisa particionar manualmente por “shard” para distribuir carga.

### Trade-off relevante

- A ordem global de mensagens **não é garantida** (e normalmente não deve ser assumida com competing consumers).

## DLQ (Dead Letter Queue)

### Quando uma mensagem vai para a DLQ?

O `processor` **classifica erros** em dois grupos:

- **`ErrInvalidEvent` (evento inválido / problema de contrato)**:
    - JSON inválido (falha no `unmarshal`)
    - Campos obrigatórios ausentes (ex.: `id`, `tenant_id`, `event_type`)
    - Payload não respeita o contrato esperado do `event_type`
    - **Ação**: vai para a **DLQ**

- **`ErrTransient` (falha transitória de infra)**:
    - Erro ao persistir no Postgres (timeout, indisponibilidade, falhas de rede)
    - **Ação desejada**: **retry** (requeue / política de tentativas)
    - **Comportamento atual**: **também vai para a DLQ** (retry ainda não implementado)

Tecnicamente, a ida para a DLQ ocorre via `NACK(requeue=false)` na mensagem.

### Por que DLQ?

- **Observabilidade e auditoria**: você não “perde” eventos ruins; eles ficam isolados para inspeção.
- **Replay**: dá para corrigir um bug/regra e reprocessar mensagens da DLQ (manual ou automatizado).
- **Proteção da fila principal**: evita loop infinito de mensagens venenosas (poison messages) bloqueando o consumo.

## Idempotência

### Como está implementada

No Postgres:

- `processed_events.event_id` é **`UNIQUE`**.
- O insert usa:
    - `ON CONFLICT (event_id) DO NOTHING`

Ou seja: se o mesmo evento (mesmo `event_id`) for entregue mais de uma vez (o que é esperado em at-least-once), a segunda tentativa não duplica o processamento.

### Por que idempotência?

- **RabbitMQ + ACK manual** tende a ser **at-least-once**: reentregas podem ocorrer por:
    - crash do consumer antes do ACK,
    - rede oscilando,
    - timeouts/transientes.
- Sem idempotência, reentrega vira **duplicidade** (efeitos colaterais repetidos).

### Trade-off relevante

- `ON CONFLICT DO NOTHING` evita duplicar, mas também **não informa** explicitamente se foi “duplicado” (a aplicação não diferencia insert real vs. conflito, a menos que passemos a tratar isso com `RETURNING`/rowcount).

## Decisões e “por quês”

### Por que **topic exchange**?

- **Roteamento por tipo**: usar `routingKey = eventType` permite evoluir para rotas por domínio (ex.: `transaction.*`, `user.*`) sem mudar produtores.
- **Extensibilidade**: amanhã podemos criar novas filas/consumidores com bindings específicos (ex.: `transaction.#`) sem impactar o fluxo atual.
- **Compatível com o demo atual**: mesmo com binding `#` (tudo), mantém o modelo certo para crescer.

### Por que **competing consumers**?

- **Escala linear**: mais instâncias = mais throughput (até os limites de I/O/DB).
- **Modelo operacional simples**: uma fila, N workers; deploy fácil em container/orquestrador.
- **Boa combinação com idempotência**: reduz risco operacional quando existe redelivery/duplicidade.

### Por que **DLQ**?

- **Separar “falhas de dados” de “falhas de sistema”**: mensagens inválidas não devem ficar voltando para a fila principal.
- **Evitar poison message loop**: sem DLQ, requeue infinito pode travar o consumo.
- **Inspeção e replay**: permite corrigir e reprocessar com segurança.

### Por que **ACK/NACK manual**?

- Com `autoAck=false`, o consumer só confirma depois que concluiu o trabalho.
- **ACK depois do commit** (persistência) evita “perder” mensagens caso o processo caia no meio.
- **NACK com requeue=false** é uma decisão explícita: falhas vão para DLQ em vez de retry infinito.

### Por que **PostgreSQL**?

- **Idempotência transacional** com constraint única é simples, robusta e auditável.
- **Fonte de verdade**: mantém histórico do que foi processado (payload, tenant, timestamps).
- **Operação conhecida**: tooling, backups, queries e índices para análise.

### Por que **idempotência** (de novo, na prática)?

- Mensageria com semântica at-least-once é comum; idempotência é o “cinto de segurança”.
- Permite escalar consumidores e lidar com falhas sem medo de efeitos duplicados.