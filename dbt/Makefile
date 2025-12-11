# Makefile para gerenciar o projeto DBT

.PHONY: help install install-dev deps clean test run docs format lint

# Default target
help: ## Mostrar ajuda
	@echo "Comandos disponíveis:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Instalar dependências de produção
	pip install -r requirements.txt

install-dev: ## Instalar dependências de desenvolvimento
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pre-commit install

deps: ## Instalar dependências DBT
	dbt deps

clean: ## Limpar arquivos temporários
	dbt clean
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

test: ## Executar testes DBT
	dbt test

run: ## Executar pipeline completo
	dbt run

run-staging: ## Executar apenas modelos staging
	dbt run --select staging

docs: ## Gerar e servir documentação
	dbt docs generate
	dbt docs serve

format: ## Formatar código Python
	black .
	isort .

lint: ## Verificar qualidade do código
	flake8 .
	black --check .
	isort --check-only .

setup: install deps ## Setup completo do projeto
	@echo "✅ Projeto configurado com sucesso!"

dev-setup: install-dev deps ## Setup para desenvolvimento
	@echo "🚀 Ambiente de desenvolvimento pronto!"
