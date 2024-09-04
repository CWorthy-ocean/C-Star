{{ objname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
   :special-members:

   {% block methods %}
   {% if methods %}
   .. rubric:: Methods
   .. autosummary::
      :toctree:
   {% for item in all_methods %}
      {%- if not item.startswith('_') or item in ['__call__',
                                                  ] %}
      {{ name }}.{{ item }}

      {% endif %}
   {% endfor %}
   {% endif %}
   {% endblock %}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: Attributes
   .. autosummary::
   {% for item in attributes %}
      {{ name }}.{{ item }}
   {% endfor %}
   {% endif %}
   {% endblock %}
