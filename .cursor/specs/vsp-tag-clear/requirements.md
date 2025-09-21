### Requirements (EARS)

- R-101: WHEN a batch item includes `tag_names` as an empty array, THE SYSTEM SHALL set `APOLLO_VARIANT_SIZE_PACK_TAG.TAG_IDS` and `TAG_NAMES` to empty arrays for that `variant_size_pack_id`.
  - Acceptance: After execution, `TAG_IDS = []` and `TAG_NAMES = []` for each specified VSPID.
- R-102: WHEN a batch item omits the `tag_names` field entirely, THE SYSTEM SHALL leave existing `TAG_IDS` and `TAG_NAMES` unchanged for that `variant_size_pack_id`.
  - Acceptance: No mutation to tag fields when `tag_names` key is not present in the JSON object.
- R-103: WHEN `tag_names` is provided as a non-empty array, THE SYSTEM SHALL normalize, create any missing tags, and update both arrays accordingly.
  - Acceptance: New tags are inserted into `APOLLO_PRODUCT_TAGS`, and VSP row reflects normalized names and IDs.
- R-104: WHEN `tag_names` is provided but its value is not an array, THE SYSTEM SHALL ignore tag updates for that item without error and proceed with any other fields.
  - Acceptance: Tag fields remain unchanged; other fields (e.g., `is_planned`) still update.

### Example Acceptance Test

Input:
- `[{"variant_size_pack_id":"RK001-96-50","tag_names":[]},{"variant_size_pack_id":"BV007-6-750","tag_names":[]}]`

Result:
- Both VSPIDs have `TAG_IDS = []` and `TAG_NAMES = []` after execution, and procedure returns success.
