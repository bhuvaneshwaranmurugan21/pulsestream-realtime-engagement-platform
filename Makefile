.PHONY: verify evidence refresh-evidence spark-test package-lambdas format terraform-validate

verify:
	ruff check src tests lambdas spark_jobs orchestration scripts
	ruff format --check src tests lambdas spark_jobs orchestration scripts
	mypy src/pulsestream
	SPARK_LOCAL_IP=127.0.0.1 pytest
	python scripts/validate_architecture_contract.py
	python scripts/validate_claim_registry.py

evidence:
	python -m pulsestream evidence --repository-root . --work-dir artifacts/local-run --events 5000

refresh-evidence:
	python scripts/refresh_evidence.py

spark-test:
	SPARK_LOCAL_IP=127.0.0.1 pytest -m spark --no-cov

package-lambdas:
	python scripts/build_lambda_package.py

format:
	ruff check --fix src tests lambdas spark_jobs orchestration scripts
	ruff format src tests lambdas spark_jobs orchestration scripts
	terraform fmt -recursive infrastructure/terraform

terraform-validate:
	terraform -chdir=infrastructure/terraform init -backend=false
	terraform -chdir=infrastructure/terraform validate
