=======================================
Sparpy: An Spark entry point for python
=======================================

---------
Changelog
---------

......
v0.5.2
......

* Fix ignoring all packages when exclude packages list is empty.

......
v0.5.1
......

* Fix Python package regex.
* Fix download script.

......
v0.5.0
......

* Added `--exclude-python-packages` option in order to exclude python packages.
* Better parsing plugins names.
* Added `--exclude-packages` option in order to exclude spark packages.

......
v0.4.5
......

* Fix isparpy entrypoint. Allows `--class` parameter.
* Allow to set constraints files.


......
v0.4.4
......

* Don't set `master` and `deploy_mode` default values.

......
v0.4.3
......

* Fix sparpy-submit entrypoint.
* Fix `--property-file` option.
* Fix `--class` option.

......
v0.4.2
......

* Able to use environment variables for the most of options.

......
v0.4.1
......

* Support to set pip options as configuration using `--conf sparpy.config-key=value` in order to allow to
  use `sparpy-submit` in EMR-on-EKS images.

* Allows `--class` in order to allow to use `sparpy-submit` in EMR-on-EKS images.
* Allows `--property-file` in order to allow to use `sparpy-submit` in EMR-on-EKS images.

......
v0.4.0
......

* Added `--pre` option in order to allow pre-release packages.
* Added `--env` option in order to set environment variables for spark process.
* Added `spark-env` config section in order to set environment variables for spark process.
* Write pip output when it fails.
* Fixed problems with interactive sparpy.
* Fixed `no-self` option in config file.

* Allow to use plugins that don't use `click`. They must be callable with one argument of type `Sequence[str]`
  in order to pass arguments to it.

* Added `--version` option in order to print sparpy version.
* Fixed error when a plugin requires a package which is already installed but version does not satisfy requirement.
* `Sparpy` does not print error traceback when subprocess fails.

......
v0.3.0
......

* Enable `--force-download` option.
* Added `--find-links` option in order to use a directory as package repository.
* Added `--no-index` option in order to avoid to use external package repositories.
* Added `--queue` option in order to set yarn queue.
* Ensure driver's python executable is same python as `sparpy`.
* Added new entry point `sparpy-download` just to download packages to specific directory.
* Added new entry point `isparpy` in order to start an interactive session.

......
v0.2.1
......

* Force `pyspark` python executable to same as `sparpy`.
* Fix unrecognized options.
* Fix default configuration file names.

......
v0.2.0
......

* Added configuration file option.
* Added `--debug` option.

----------------------------
How to build a Sparpy plugin
----------------------------

On package `setup.py` an entry point should be configured for Sparpy:

.. code-block:: python

    setup(
        name='yourpackage',
        ...

        entry_points={
            ...
            'sparpy.cli_plugins': [
                'my_command_1=yourpackage.module:command_1',
                'my_command_2=yourpackage.module:command_2',
            ]
        }
    )

.. note::

    Avoid to use PySpark as requirement in order to not download package from pypi.

-------
Install
-------

It must be installed on a Spark edge node.

.. code-block:: bash

    $  pip install sparpy[base]


----------
How to use
----------

Using default Spark submit parameters:

.. code-block:: bash

    $ sparpy --plugin "mypackage>=0.1" my_plugin_command --myparam 1


-------------------
Configuration files
-------------------

`sparpy` and `sparpu-submit` accept the parameter `--config` that allow to set a configuration file. If it is not set
it will try to use configuration file `$HOME/.sparpyrc`. It if does not exist it will try to use `/etc/sparpy.conf`.

Format:

.. code-block:: ini

    [spark]

    master=yarn
    deploy-mode=client

    queue=my_queue

    spark-executable=/path/to/my-spark-submit
    conf=
        spark.conf.1=value1
        spark.conf.2=value2

    packages=
        maven:package_1:0.1.1
        maven:package_2:0.6.1

    repositories=
        https://my-maven-repository-1.com/mvn
        https://my-maven-repository-2.com/mvn

    reqs_paths=
        /path/to/dir/with/python/packages_1
        /path/to/dir/with/python/packages_2

    [spark-env]

    MY_ENV_VAR=value

    [plugins]

    extra-index-urls=
        https://my-pypi-repository-1.com/simple
        https://my-pypi-repository-2.com/simple

    cache-dir=/path/to/cache/dir

    plugins=
        my-package1
        my-package2==0.1.2

    requirements-files=
        /path/to/requirement-1.txt
        /path/to/requirement-2.txt

    find-links=
        /path/to/directory/with/packages_1
        /path/to/directory/with/packages_2

    download-dir-prefix=my_prefix_

    no-index=false
    no-self=false
    force-download=true

    [interactive]

    pyspark-executable=/path/to/pyspark
    python-interactive-driver=/path/to/interactive/driver
