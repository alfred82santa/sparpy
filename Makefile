PACKAGE_NAME = sparpy

help:
	@echo "Options"
	@echo "---------------------------------------------------------------"
	@echo "help:                     This help"
	@echo "requirements:             Download requirements"
	@echo "requirements-dev:         Download requirements for development"
	@echo "requirements-docs:        Download requirements for docs"
	@echo "run-tests:                Run tests with coverage"
	@echo "clean:                    Clean compiled files"
	@echo "flake:                    Run Flake8"
	@echo "prepush:                  Helper to run before to push to repo"
	@echo "autopep:                  Reformat code using PEP8"
	@echo "beautify:                 Alias of autopep"
	@echo "publish:                  Publish package on Pypi repository"
	@echo "---------------------------------------------------------------"

requirements:
	@echo "Installing $(PACKAGE_NAME) requirements..."
	pip3 install -r requirements.txt

requirements-dev: requirements
	@echo "Installing $(PACKAGE_NAME) development requirements..."
	pip3 install -r requirements-dev.txt

requirements-docs: requirements
	@echo "Installing $(PACKAGE_NAME) docs requirements..."
	pip3 install -r requirements-docs.txt

run-tests:
	@echo "Running tests..."
	nosetests --with-coverage -d --cover-package=$(PACKAGE_NAME) --cover-erase -x


clean:
	@echo "Cleaning compiled files..."
	find . | grep -E "(__pycache__|\.pyc|\.pyo)$ " | xargs rm -rf
	@echo "Cleaning coverage files..."
	rm -f .coverage
	@echo "Cleaning build files..."
	rm -rf build
	@echo "Cleaning egg files..."
	rm -rf *.egg-info
	@echo "Cleaning distribution files..."
	rm -rf dist
	@echo "Cleaning spark distribution files..."
	rm -rf spark_dist


flake:
	@echo "Running flake8 tests..."
	flake8 $(PACKAGE_NAME) --exclude=scripts
	flake8 tests
	isort -rc -y -c $(PACKAGE_NAME)
	isort -rc -y -c tests

autopep:
	autopep8 --max-line-length 120 -r -j 8 -i -a --exclude ./docs/source/conf.py .

sort-imports:
	isort -rc -y $(PACKAGE_NAME)
	isort -rc -y tests

beautify: autopep sort-imports

prepush: flake run-tests

build:
	python3 setup.py bdist_wheel

publish: clean build
	@echo "Publishing new version on Pypi..."
	twine upload dist/*
