# Archive: Snowflake Stored Procedure Debugging Pattern

- **Date:** 2024-07-25
- **Task:** Snowflake Stored Procedure Migration - `product_tagging_workflow`
- **Associated Task in `tasks.md`:** `[x] Translate product_tagging_workflow`
- **Result:** Two key stored procedures, `sp_add_product_tags_to_vsp` and `sp_batch_update_apollo_variant_size_pack_tags`, were successfully debugged and are now fully functional.

---

### 1. Architectural Decision Record & Implementation Summary

This session focused on resolving a recurring syntax error in Snowflake stored procedures that perform dynamic SQL.

- **Problem:** The `EXECUTE IMMEDIATE ... USING` statement in Snowflake SQL Scripting is not robust enough to handle dynamic identifiers (e.g., `IDENTIFIER(?)`) and array data types simultaneously. This limitation caused syntax errors in procedures that needed to insert data into dynamically-named temporary tables.

- **Decision & Solution:** The standard parameterized query approach was abandoned in favor of building the SQL command as a string. This provides a reliable and repeatable pattern for similar procedures.

- **Implementation Pattern:**
  1.  **Remove `USING` Clause:** Eliminate the `USING` clause entirely for the problematic `EXECUTE IMMEDIATE` calls.
  2.  **Concatenate Dynamic Identifiers:** Directly concatenate table and variable names into the SQL string.
        - `... 'INSERT INTO ' || TEMP_TABLE_NAME || '...'`
  3.  **Serialize Array Parameters:** Convert array variables into a valid SQL representation by using `TO_JSON` and `PARSE_JSON`.
        - `... PARSE_JSON(' || TO_JSON(array_variable) || ') ...`
  4.  **Escape String Literals:** Use `CHR(39)` to safely wrap string literals within the concatenated SQL command.

### 2. Key Artifacts

- **`tenants/pg_func_migration/product_tagging_workflow/sp_add_product_tags_to_vsp.sql`**: The core procedure for adding tags to a single product.
- **`tenants/pg_func_migration/product_tagging_workflow/sp_batch_update_apollo_variant_size_pack_tags.sql`**: The batch procedure that iterates a JSON input and calls the core procedure.
- **`memory-bank/reflection.md`**: The detailed reflection document that was generated during the `REFLECT` phase.

### 3. Lessons Learned & Impact

- **Primary Lesson:** The limitations of Snowflake's `EXECUTE IMMEDIATE ... USING` clause are now a known constraint. The string concatenation pattern is the official workaround for dynamic SQL involving array parameters or dynamic identifiers.
- **Impact:** This solution unblocks the migration of all PostgreSQL functions that follow a similar pattern, providing a clear path forward for the remainder of Phase 4. It establishes a key technical precedent for future Snowflake development in this project. 