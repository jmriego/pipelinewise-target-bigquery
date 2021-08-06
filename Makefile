venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
# TODO use pylintrc
	pylint -d C,W,unexpected-keyword-arg,duplicate-code target_bigquery/

unit_test:
	. ./venv/bin/activate ;\
	pytest --cov=target_bigquery  --cov-fail-under=41 tests/unit -v

integration_test:
	. ./venv/bin/activate ;\
	pytest tests/integration --cov=target_bigquery -v
