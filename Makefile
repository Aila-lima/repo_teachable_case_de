.PHONY: install run report check clean

install:	## instala dependencias (rode dentro de um venv)
	pip install -r requirements.txt

run:		## seed Bronze and replay every ingestion day
	python -m gmv.pipeline --reset --seed --replay-all

report:		## deliverables 3 and 4: sample rows and analytical queries
	python -m gmv.report

check:		## invariantes de ponta a ponta (imutabilidade, determinismo)
	python -m gmv.checks

test:		## suite de testes (ciclo rapido)
	python -m pytest

test-all:	## suite completa, incluindo os marcados como slow
	python -m pytest -m ""

quality:	## data quality gate sobre um batch
	python -m gmv.quality --batch-date $(BATCH_DATE)

clean:
	rm -rf warehouse
