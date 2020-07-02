======================================
Sparpy: A Spark entry point for python
======================================


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