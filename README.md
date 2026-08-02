# Case técnico Teachable - Data Engineer
## Descrição 
Criação de pipeline de dados end-to-end desenvolvido como solução para o case técnico de Data Engineer.

---

## Como rodar

```bash
pip install -r requirements.txt

make run       # Popula os eventos CDC do case e reprocessa todos os dias de ingestão
make report    # Linhas de exemplo + queries analíticas
make test      # Pool de testes
make check     # Invariantes de ponta a ponta (imutabilidade, determinismo)

make test-all                       # Inclui os testes marcados como slow
make quality BATCH_DATE=2023-08-10  # gate de qualidade sobre um batch
```

OBS: Roda em Parquet puro.

---

## Entregáveis

| # | Pedido | Onde está |
|---|---|---|
| 1 | DDL da tabela analítica final | [`ddl/fct_purchase_gmv_history.sql`](ddl/fct_purchase_gmv_history.sql) |
| 2 | Pipeline ETL/ELT (PySpark) | [`gmv/`](gmv/) |
| 3 | Exemplo de saída | [`docs/sample_output.md`](docs/sample_output.md) |
| 4 | GMV diário por subsidiary | [`sql/gmv_daily_by_subsidiary.sql`](sql/gmv_daily_by_subsidiary.sql) |
| 5 | Arquitetura e decisões de projeto | |
| — | Bônus: streaming, camada semântica, conciliação com Finance | |

## Estrutura do repositório

```
gmv/
  config.py      premissas e regras de negócio, num lugar só
  storage.py     sessão Spark, formato de armazenamento plugável, watermark
  seed.py        os eventos CDC do enunciado (Bronze)
  silver.py      compactação de estado por chave: incremental + rebuild point-in-time
  assemble.py    três streams CDC -> uma linha fato candidata
  history.py     carga append-only da Gold, diff de versões, motivos de mudança
  pipeline.py    orquestração / CLI
  analytics.py   camada de serving
  report.py      entregáveis 3 e 4
  checks.py      testes de invariante
ddl/  sql/  docs/
```

## A query analítica

Uma tabela, sem joins, sem filtros — `gmv_amount` já é zero para tudo que não foi
liberado ou que foi cancelado depois:

```sql
SELECT gmv_date AS date, subsidiary, SUM(gmv_amount) AS gmv
FROM   vw_purchase_gmv_current
WHERE  gmv_date IS NOT NULL
GROUP  BY gmv_date, subsidiary;
```

## Testes

- `make test` cobre 29 casos em cinco arquivos: regra de negócio, versionamento,
comportamento bitemporal, reprocessamento e contrato de schema. O mais
importante é `test_reprocessing_the_past_does_not_leak_future_state`.

- `make quality` roda o gate de produção: oito checks blockers e quatro
warnings sobre os dados reais do batch.

## Invariantes verificados por `make check`

| | |
|---|---|
| 1 | `(purchase_id, version_number)` é único; não há lacunas em `version_number` |
| 2 | um snapshot "as of" é sempre prefixo estrito de um posterior |
| 3 | nenhuma versão antecede o evento de origem que ela reflete |
| 4 | reprocessar um dia encerrado reproduz todas as linhas inalteradas |
| 5 | reprocessar o pipeline do zero reproduz a Gold byte a byte |

O invariante 5 pegou um bug real durante o desenvolvimento — uma Gold append-only
alimentada por uma Silver mutável ainda produzia história falsificada. O
diagnóstico e a correção estão na documentação presente nesse repositório.
