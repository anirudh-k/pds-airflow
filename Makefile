.PHONY: login run-local deps deps-compile deps-sync deps-update

login:
	gcloud auth application-default login

run-local:
	astro dev restart

deps: deps-compile deps-sync ## Install all dependencies

deps-compile: ## Compile requirements.txt from requirements.in
	pip-compile requirements.in

deps-sync: ## Sync venv with requirements.txt
	pip-sync requirements.txt

deps-update: ## Update all dependencies to their latest versions
	pip-compile --upgrade requirements.in
	pip-sync requirements.txt
