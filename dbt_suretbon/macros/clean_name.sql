{% macro clean_text(input_text) %}
    (
      REGEXP_REPLACE(
        LOWER(
            REGEXP_REPLACE(
                -- Supprimer les accents en utilisant la normalisation Unicode
                NORMALIZE({{ input_text }}, NFD),
                '[^a-zA-Z0-9]', ''  -- Supprimer les caractères non alphanumériques
            )
        )
        , r'(sarl|sas|eurl|scs|scop|sasu|scic)', '')
    )
{% endmacro %}
