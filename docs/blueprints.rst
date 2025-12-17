Blueprints
==========

Example
-------

Schema details
--------------

Checking validity
-----------------

CLI
^^^

.. code::

    cstar blueprint check my_blueprint.yaml

In Python
^^^^^^^^^

.. code:: python

    from cstar.orchestration.models import RomsMarblBlueprint
    from cstar.orchestration.serialization import deserialize
    model = deserialize(path_to_your_blueprint_yaml, RomsMarblBlueprint)  # will raise error if invalid

Execution
---------

CLI
^^^

.. code::

    cstar blueprint run my_blueprint.yaml



In Python
^^^^^^^^^

.. code:: python

