# ADR-001: Why Kappa Architecture Over Lambda

## Status
**Accepted**

## Context
We need to choose between Lambda (separate batch + streaming) and Kappa (streaming-first, batch as special case of streaming) architectures.

## Decision
**Kappa Architecture** — streaming-first approach where:
- All data flows through Kafka as the immutable event log
- Streaming processes are the primary computation layer
- Batch reprocessing is achieved by replaying the event log

## Rationale
1. **Single codebase**: No need to maintain separate batch and streaming logic
2. **Kafka as source of truth**: Immutable event log enables replay and reprocessing
3. **Simpler operations**: One processing framework to monitor and maintain
4. **DataForge integration**: Late data beyond watermarks flows to `dataforge-core` for batch reprocessing, creating a natural hybrid without Lambda's complexity

## Trade-offs
- **Pro**: Simpler architecture, single processing framework
- **Con**: Higher Kafka storage costs for long retention
- **Con**: Complex aggregations may be slower in pure streaming
- **Mitigation**: Use `dataforge-core` for complex historical aggregations (Gold layer)
