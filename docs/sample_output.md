# Exemplo de saída (Entregável 3)

Gerado por `make report` sobre os eventos CDC do enunciado. Para reproduzir:

```bash
make run && make report
```

As três compras exercitam todos os pontos da Core Question:

| compra | o que demonstra |
|---|---|
| **55** | dimensão atrasada, correção de valor seis meses depois, e uma retificação de `release_date` que move o GMV de janeiro para março |
| **56** | item de compra ingerido *antes* do evento de compra; pagamento capturado tardiamente, então ela entra no GMV apenas numa versão posterior |
| **69** | subsidiary retificada de `nacional` para `internacional`; depois reembolsada, então o GMV sai da tabela sem que nenhuma linha seja deletada |

---

```text
====================================================================================================
1. fct_purchase_gmv_history - historico completo de versoes (append-only)
====================================================================================================

                                                                                
+-----------+---+-----------+------------+-------------+----------+----------+--------+-----------+--------+-------------------------------------------------+
|purchase_id|v  |ingested_on|version_type|subsidiary   |gmv_date  |gmv_amount|eligible|status     |complete|change_reason                                    |
+-----------+---+-----------+------------+-------------+----------+----------+--------+-----------+--------+-------------------------------------------------+
|55         |1  |2023-01-20 |INITIAL     |UNKNOWN      |2023-01-20|50.00     |true    |APROVADA   |false   |[NEW]                                            |
|55         |2  |2023-01-23 |LATE_ARRIVAL|nacional     |2023-01-20|50.00     |true    |APROVADA   |true    |[SUBSIDIARY_ARRIVED_LATE, LATE_COMPONENT_ARRIVED]|
|55         |3  |2023-02-05 |RESTATEMENT |nacional     |2023-01-20|50.00     |true    |APROVADA   |true    |[BUYER_CHANGED]                                  |
|55         |4  |2023-07-12 |RESTATEMENT |nacional     |2023-01-20|55.00     |true    |APROVADA   |true    |[VALUE_CORRECTED]                                |
|55         |5  |2023-07-15 |RESTATEMENT |nacional     |2023-03-01|55.00     |true    |APROVADA   |true    |[RELEASE_DATE_CHANGED]                           |
|56         |1  |2023-01-26 |INITIAL     |internacional|NULL      |0.00      |false   |INICIADA   |true    |[NEW]                                            |
|56         |2  |2023-02-10 |RESTATEMENT |internacional|2023-02-10|2400.00   |true    |APROVADA   |true    |[RELEASE_DATE_CHANGED, STATUS_CHANGED]           |
|69         |1  |2023-02-26 |INITIAL     |UNKNOWN      |2023-02-28|2000.00   |true    |APROVADA   |false   |[NEW]                                            |
|69         |2  |2023-02-28 |LATE_ARRIVAL|nacional     |2023-02-28|2000.00   |true    |APROVADA   |true    |[SUBSIDIARY_ARRIVED_LATE, LATE_COMPONENT_ARRIVED]|
|69         |3  |2023-03-12 |RESTATEMENT |internacional|2023-02-28|2000.00   |true    |APROVADA   |true    |[SUBSIDIARY_RESTATED]                            |
|69         |4  |2023-08-10 |RESTATEMENT |internacional|2023-02-28|0.00      |false   |REEMBOLSADA|true    |[STATUS_CHANGED]                                 |
+-----------+---+-----------+------------+-------------+----------+----------+--------+-----------+--------+-------------------------------------------------+

====================================================================================================
2. vw_purchase_gmv_current - uma linha por compra, sem necessidade de joins
====================================================================================================

                                                                                
+-----------+---+-------------+----------+----------+-----------+-----------+
|purchase_id|v  |subsidiary   |gmv_date  |gmv_amount|status     |why_not    |
+-----------+---+-------------+----------+----------+-----------+-----------+
|55         |5  |nacional     |2023-03-01|55.00     |APROVADA   |NULL       |
|56         |2  |internacional|2023-02-10|2400.00   |APROVADA   |NULL       |
|69         |4  |internacional|2023-02-28|0.00      |REEMBOLSADA|REEMBOLSADA|
+-----------+---+-------------+----------+----------+-----------+-----------+

====================================================================================================
3. ENTREGAVEL 4 - GMV diario por subsidiary (verdade vigente)
====================================================================================================
+----------+-------------+-------+---------+
|date      |subsidiary   |gmv    |purchases|
+----------+-------------+-------+---------+
|2023-02-10|internacional|2400.00|1        |
|2023-02-28|internacional|0.00   |1        |
|2023-03-01|nacional     |55.00  |1        |
+----------+-------------+-------+---------+

====================================================================================================
4. REQUISITO 4 - consultas as of: janeiro/2023 visto de tres pontos no tempo
====================================================================================================

--- GMV como era conhecido em 2023-01-31 ---
+----------+----------+-----+---------+
|date      |subsidiary|gmv  |purchases|
+----------+----------+-----+---------+
|2023-01-20|nacional  |50.00|1        |
+----------+----------+-----+---------+

--- GMV como era conhecido em 2023-03-31 ---
+----------+----------+-----+---------+
|date      |subsidiary|gmv  |purchases|
+----------+----------+-----+---------+
|2023-01-20|nacional  |50.00|1        |
+----------+----------+-----+---------+

--- GMV como era conhecido em 2023-12-31 ---
+----+----------+---+---------+
|date|subsidiary|gmv|purchases|
+----+----------+---+---------+
+----+----------+---+---------+

====================================================================================================
5. Linhagem diaria / conciliacao - o que mudou, quando e por que
====================================================================================================
+-------------+---------------------+-----------+---+------------+-------------------------------------------------+----------+----------+-------------+
|ingestion_day|batch_id             |purchase_id|v  |version_type|change_reason                                    |gmv_date  |gmv_amount|subsidiary   |
+-------------+---------------------+-----------+---+------------+-------------------------------------------------+----------+----------+-------------+
|2023-01-20   |gmv_daily__2023-01-20|55         |1  |INITIAL     |[NEW]                                            |2023-01-20|50.00     |UNKNOWN      |
|2023-01-23   |gmv_daily__2023-01-23|55         |2  |LATE_ARRIVAL|[SUBSIDIARY_ARRIVED_LATE, LATE_COMPONENT_ARRIVED]|2023-01-20|50.00     |nacional     |
|2023-01-26   |gmv_daily__2023-01-26|56         |1  |INITIAL     |[NEW]                                            |NULL      |0.00      |internacional|
|2023-02-05   |gmv_daily__2023-02-05|55         |3  |RESTATEMENT |[BUYER_CHANGED]                                  |2023-01-20|50.00     |nacional     |
|2023-02-10   |gmv_daily__2023-02-10|56         |2  |RESTATEMENT |[RELEASE_DATE_CHANGED, STATUS_CHANGED]           |2023-02-10|2400.00   |internacional|
|2023-02-26   |gmv_daily__2023-02-26|69         |1  |INITIAL     |[NEW]                                            |2023-02-28|2000.00   |UNKNOWN      |
|2023-02-28   |gmv_daily__2023-02-28|69         |2  |LATE_ARRIVAL|[SUBSIDIARY_ARRIVED_LATE, LATE_COMPONENT_ARRIVED]|2023-02-28|2000.00   |nacional     |
|2023-03-12   |gmv_daily__2023-03-12|69         |3  |RESTATEMENT |[SUBSIDIARY_RESTATED]                            |2023-02-28|2000.00   |internacional|
|2023-07-12   |gmv_daily__2023-07-12|55         |4  |RESTATEMENT |[VALUE_CORRECTED]                                |2023-01-20|55.00     |nacional     |
|2023-07-15   |gmv_daily__2023-07-15|55         |5  |RESTATEMENT |[RELEASE_DATE_CHANGED]                           |2023-03-01|55.00     |nacional     |
|2023-08-10   |gmv_daily__2023-08-10|69         |4  |RESTATEMENT |[STATUS_CHANGED]                                 |2023-02-28|0.00      |internacional|
+-------------+---------------------+-----------+---+------------+-------------------------------------------------+----------+----------+-------------+

====================================================================================================
6. Auditoria de retificacao - o delta de GMV entre duas datas, explicado
====================================================================================================

                                                                                
+-----------+---------------+--------------+-----------------+----------------+----------+---------+--------+----------------------+
|purchase_id|gmv_date_before|gmv_date_after|subsidiary_before|subsidiary_after|gmv_before|gmv_after|delta   |latest_reason         |
+-----------+---------------+--------------+-----------------+----------------+----------+---------+--------+----------------------+
|55         |2023-01-20     |2023-03-01    |nacional         |nacional        |50.00     |55.00    |5.00    |[RELEASE_DATE_CHANGED]|
|69         |2023-02-28     |2023-02-28    |internacional    |internacional   |2000.00   |0.00     |-2000.00|[STATUS_CHANGED]      |
+-----------+---------------+--------------+-----------------+----------------+----------+---------+--------+----------------------+
```
