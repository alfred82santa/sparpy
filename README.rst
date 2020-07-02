======================================
Sparpy: A spark entry point for python
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
                'name_of_plugin_command_1=yourpackage.module:command_1',
                'name_of_plugin_command_2=yourpackage.module:command_2',
            ]
        }
    )