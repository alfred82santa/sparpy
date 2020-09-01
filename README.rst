======================================
Sparpy: A Spark entry point for python
======================================

---------
Changelog
---------

......
v0.2.0
......

* Added configuration file option.
* Added `--debug` option.

----------------------------
How to build a Sparpy plugin
----------------------------

On package `setup.py` a entry point must be configured for Sparpy:

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

    $  pip install sparpy


----------
How to use
----------

Using default Spark submit parameters:

.. code-block:: bash

    $ sparpy-submit --plugin "mypackage>=0.1" my_command_1 --myparam 1


-------------------
Configuration files
-------------------

`sparpy` and `sparpu-submit` accept the parameter `--config` that allow to set a configuration file. If it is not set
it will try to use configuration file `$HOME/.sparpyrc`. It if does not exist it will try to use `/etc/sparkpy.conf`.

Format:

.. code-block:: ini

    [spark]

    master=yarn
    deploy-mode=client

    spark-executable=/path/to/my-spark-submit
    conf=
        spark.conf.1=value1
        spark.conf.2=value2

    packages=
        maven:package_1:0.1.1
        maven:package_1:0.1.1

    repositories=
        http://my-maven-repository-1.com/simple
        http://my-maven-repository-2.com/simple

    reqs_paths=
        /path/to/dir/with/python/packages_1
        /path/to/dir/with/python/packages_2

    [plugins]

    extra-index-urls=
        http://my-pypi-repository-1.com/simple
        http://my-pypi-repository-2.com/simple

    cache-dir=/path/to/cache/dir

    plugins=
        my-package1
        my-package2==0.1.2

    requirements-files=
        /path/to/requirement-1.txt
        /path/to/requirement-2.txt

    download-dir-prefix=my_prefix_

    no-self=false
    force-download=true