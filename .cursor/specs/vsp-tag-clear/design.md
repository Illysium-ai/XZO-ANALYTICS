### Design Overview

- Distinguish between "field omitted" and "provided as empty array" for `tag_names`.
- Only update tag arrays when `tag_names` is present AND is an array. If provided empty (`[]`), explicitly set both arrays to empty; if non-empty, upsert and set accordingly; if omitted, leave unchanged (R-101, R-102, R-103).
- Preserve existing transactional and realtime sync behavior; tag updates do not affect sync trigger conditions (R-103).

### Changes to Stored Procedure

- In `SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS`:
  - Input parsing: Do not default `V_TAG_NAMES_ARRAY` to `ARRAY_CONSTRUCT()` when `tag_names` is absent; leave it `NULL` to represent "omitted" (R-102).
  - Update logic: Replace `CASE WHEN ARRAY_SIZE(:V_PROCESSED_TAG_IDS) > 0 THEN ... ELSE TAG_IDS END` with a presence check on the input record: when `tag_names` is present and is an array, set `TAG_IDS`/`TAG_NAMES` to the processed arrays (which may be empty), otherwise leave unchanged (R-101, R-103).

### Data Contracts

- Input JSON item fields used:
  - `variant_size_pack_id: string` (required)
  - `tag_names: array<string>` (optional; when present, drives tag updates)
  - Other optional fields unchanged (e.g., `is_planned`, exclusions, `is_custom_product`).

### Diagram

```mermaid
flowchart TD
    A[Batch JSON Input] --> B{item.tag_names present?}
    B -- no --> C[Leave TAG_IDS/TAG_NAMES unchanged]
    B -- yes --> D{Is array?}
    D -- no --> C
    D -- yes --> E{Empty array?}
    E -- yes --> F[Set TAG_IDS=[] and TAG_NAMES=[]]
    E -- no --> G[Normalize/insert tags; set TAG_IDS/TAG_NAMES]
    F --> H[Commit per-batch]
    G --> H
```

### Traceability

- R-101: Explicit empty array clears tags via presence-checked update
- R-102: Omissions preserved by leaving `V_TAG_NAMES_ARRAY` as NULL and skipping tag updates
- R-103: Non-empty arrays follow existing create-and-set flow
- R-104: Non-array values ignored for tags; other fields continue to update
