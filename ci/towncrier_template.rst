{#-
  towncrier template for docs/releases/<version>.rst (see [tool.towncrier] in
  pyproject.toml). It writes the ".. _<version>:" anchor and the title, then a
  "~"-underlined block for each fragment type that has fragments, and one "- "
  bullet per fragment, as in the releases already in docs/releases/.

  A fragment's lead is its first paragraph: the lines up to the first blank
  line or the first "- " sub-bullet. The PR link goes at the end of the lead.
  Whatever follows (sub-bullets, further paragraphs, a code block) is set under
  the bullet, after the blank line RST needs. A fragment written as a bullet
  ("- Fixed X", with its sub-bullets indented under it) loses its marker and
  that extra indentation, so it does not render as a list nested in the bullet.
  A lead ending in "::" introduces a literal block, so its link goes after that
  block instead of breaking the "::".
-#}
{% if render_title %}
.. _{{ versiondata.version }}:

{{ versiondata.version }}
{{ top_underline * (versiondata.version | length) }}
{% endif %}
{% for section, _ in sections.items() %}
{% if sections[section] %}
{% for category, val in definitions.items() if category in sections[section] %}

{{ definitions[category]["name"] }}
{{ underlines[0] * (definitions[category]["name"] | length) }}

{% set block = namespace(after_rest=false) %}
{% for text, values in sections[section][category].items() %}
{% set entry = namespace(lead=[], rest=[], lead_done=false, bullet=false, indent=1000) %}
{% for line in text.split("\n") %}
{% if loop.first %}
{% set entry.bullet = line[:2] in ("* ", "- ") %}
{% set entry.lead = [line[2:] if entry.bullet else line] %}
{% elif entry.rest %}
{% set entry.rest = entry.rest + [line] %}
{% elif not line.strip() %}
{% set entry.lead_done = true %}
{% elif entry.lead_done or line.lstrip()[:2] in ("* ", "- ") %}
{% set entry.rest = [line] %}
{% else %}
{% set entry.lead = entry.lead + ["  " + line.strip()] %}
{% endif %}
{% endfor %}
{% for line in entry.rest if line.strip() %}
{% set entry.indent = [entry.indent, (line | length) - (line.lstrip() | length)] | min %}
{% endfor %}
{% set shift = 2 if entry.bullet and entry.indent >= 4 else 0 %}
{% set links = "(" + (values | join(", ")) + ")" if values else "" %}
{% set link_last = links and entry.rest and entry.lead[-1].rstrip()[-2:] == "::" %}
{% if block.after_rest %}

{% endif %}
- {{ entry.lead | join("\n") }}{% if links and not link_last %} {{ links }}{% endif %}

{% if entry.rest %}

{% for line in (entry.rest | join("\n")).rstrip().split("\n") %}
{{ line[shift:] if line.strip() else "" }}
{% endfor %}
{% if link_last %}

  {{ links }}
{% endif %}
{% endif %}
{% set block.after_rest = entry.rest | length > 0 %}
{% endfor %}
{% endfor %}
{% else %}

No significant changes.
{% endif %}
{% endfor %}
