{# HIPAA-safe one-way mask. Default is SHA-256 of the input — sufficient for
   joining across marts on a stable token without ever exposing the raw value.
   Production would salt this with a per-environment secret; we leave that as
   an env-var swap so the demo is self-contained.

   Usage:  {{ hipaa_mask('member_id') }}  →  sha256(member_id)
#}
{% macro hipaa_mask(column_name, salt_env_var='HIPAA_MASK_SALT') -%}
    sha256(cast({{ column_name }} as varchar) || coalesce('{{ env_var(salt_env_var, "") }}', ''))
{%- endmacro %}
