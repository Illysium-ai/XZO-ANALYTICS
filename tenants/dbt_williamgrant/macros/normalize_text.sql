{# Macro to normalize text fields for better matching #}
{# 
    Args:
        column_name: The column or expression containing the text to normalize.

    Returns:
        A normalized string:
        - Leading 'The ' removed (case-insensitive)
        - Common apostrophes (e.g., ', ‘, ’) removed
        - All periods (.) removed
        - Other non-alphanumeric characters (excluding spaces AND '&') replaced with a single space
        - Multiple spaces collapsed into single spaces
        - Leading/trailing whitespace trimmed
        - Converted to Title Case (using initcap) at the end
#}
{% macro normalize_text(column_name) %}
  initcap( -- 7. Apply Title Case last
    trim( -- 6. Final Trim
      regexp_replace( -- 5. Collapse multiple spaces to single space
        regexp_replace( -- 4. Replace OTHER unwanted chars with space (keep A-Z, a-z, 0-9, &, space)
          regexp_replace( -- 3. Remove all periods
            regexp_replace( -- 2. Remove common apostrophes (ASCII ', Left Single ‘, Right Single ’)
              regexp_replace( -- 1. Remove leading "The " (case-insensitive)
                {{ column_name }}, -- 0. Input column/expression
                '^[Tt][Hh][Ee]\\\s+', '' -- Case-insensitive "The " + space
              ),
              '[''‘’]', ''  -- Pattern to match ASCII ', Left Single ‘, Right Single ’
            ),
            '\\\.', '' -- Remove all periods (escaped dot)
          ),
          '[^A-Za-z0-9&\\\s]', ' ' -- Replace non (letter, number, ampersand, space) with space
        ),
        '\\\s+', ' ' -- Collapse consecutive spaces
      )
    )
  )
{% endmacro %} 