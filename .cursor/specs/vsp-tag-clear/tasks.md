### Tasks / Plan

- T-101 (completed): Update input extraction to leave `V_TAG_NAMES_ARRAY` as NULL when `tag_names` is absent (R-102)
  - Notes: Added `V_TAG_NAMES_PRESENT_AND_ARRAY` flag; set `V_TAG_NAMES_ARRAY := NULL` when absent/non-array in `sp_batch_update_apollo_variant_size_pack_tags.sql`.
- T-102 (completed): Update UPDATE statement logic to condition on presence of `tag_names` and its array type (R-101, R-103)
  - Notes: UPDATE now uses presence flag to overwrite `TAG_IDS`/`TAG_NAMES`, enabling empty array clears.
- T-103 (completed): Validate compile/apply in dev and run sample calls (R-101..R-104)
  - Notes: Compiled in `APOLLO_DEVELOPMENT`; executed clear via `[]`, add via `["Core"]`, and omission (only `is_planned:true`) — verified tags clear/set/unchanged as expected.
- T-104 (won't-do): Add regression tests/examples to repo docs (R-101..R-104)
  - Notes: Skipped per product decision (no regression artifacts requested).
- T-105 (completed): CHANGELOG and PR description
  - Notes: Added entry to `CHANGELOG.md` documenting behavior change and example call; PR notes prepared inline.
