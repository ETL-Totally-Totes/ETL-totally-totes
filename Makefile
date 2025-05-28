#################################################################################
#
# Makefile to build the project
#
#################################################################################

REGION = eu-west-2
PYTHON_INTERPRETER = python
SHELL := /bin/bash
PYTHONPATH=$(shell pwd)

## Create python interpreter environment.
create-environment:
	@if [ ! -d "venv" ]; then \
		echo ">>> Creating virtual environment..."; \
		$(PYTHON_INTERPRETER) -m venv venv; \
	else \
		echo ">>> venv already exists, skipping creation."; \
	fi
# Create-environment only exists if venv/ doesn't already exist
# So we can safely keep it within requirements block for CI/CD

##set python path
set-pythonpath:
	export PYTHONPATH=$(PYTHONPATH)
## Build the environment requirements
requirements: create-environment
	source venv/bin/activate && pip install -r requirements.txt

################################################################################################################
# Set Up
## Install bandit
bandit:
	source venv/bin/activate && pip install bandit

## Install black
black:
	source venv/bin/activate && pip install black

## Install coverage
coverage:
	source venv/bin/activate && pip install coverage

## Set up dev requirements (bandit, black & coverage)
dev-setup: bandit black coverage

## Run the security test (bandit)
security-test:
	source venv/bin/activate && bandit -lll */*.py *c/*/*.py

## Run the black code check
run-black:
	source venv/bin/activate && black ./src ./tests

## Run the unit tests
unit-test:
	source venv/bin/activate && PYTHONPATH=${PYTHONPATH} pytest || true

## Run the coverage check
# check-coverage:
# 	source venv/bin/activate && pytest --cov=src tests/ || true

# check-coverage:
#     $(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run --omit 'venv/*' -m pytest && coverage report -m)

## Run all checks
run-checks: security-test run-black unit-test #check-coverage

## Run everything
run-all: requirements dev-setup run-checks
	echo 'Makefile completed!'
