import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text, inspect

# ==========================================
# 1. CONSOLIDATION FUNCTION (Defined First)
# ==========================================
def consolidate_tables(engine, schema_name, table_suffix):
    """
    Looks for cleaned tables in the schema, groups them by identical column structure,
    merges them into a MASTER table, and removes the fragments.
    """
    print(f"--- Starting Auto-Consolidation in {schema_name} ---")
    
    try:
        inspector = inspect(engine)
        all_tables = inspector.get_table_names(schema=schema_name)    
        
        # Filter for tables that have the suffix (e.g., '_cleaned')
        # AND exclude tables that are already MASTER tables
        tables = [
            t for t in all_tables 
            if t.endswith(table_suffix) and not t.startswith('MASTER_')
        ]
        
        if not tables:
            print("No cleaned tables found to consolidate.")
            return

        # Group tables by Column Signature
        schema_groups = {}
        for table_name in tables:
            columns = [col['name'] for col in inspector.get_columns(table_name, schema=schema_name)]
            col_signature = tuple(sorted(columns))
            
            if col_signature not in schema_groups:
                schema_groups[col_signature] = []
            schema_groups[col_signature].append(table_name)

        # Merge groups
        for signature, table_list in schema_groups.items():
            if len(table_list) > 1:
                print(f"\n🔗 Found {len(table_list)} compatible tables: {table_list}")
                
                dfs_to_merge = []
                for t in table_list:
                    print(f"   > Reading {t}...")
                    df = pd.read_sql(f'SELECT * FROM "{schema_name}"."{t}"', engine)
                    df['original_table_name'] = t 
                    dfs_to_merge.append(df)
                
                merged_df = pd.concat(dfs_to_merge, axis=0, ignore_index=True)
                
                # Naming: Strip suffix and numbers to get base name
                base_name = table_list[0].replace(table_suffix, '')
                base_name = ''.join([i for i in base_name if not i.isdigit()]).rstrip('_')
                
                merged_table_name = f"{base_name}_concat"
                
                print(f"   > 💾 Saving merged data to: {schema_name}.{merged_table_name}")
                merged_df.to_sql(
                    name=merged_table_name,
                    con=engine,
                    schema=schema_name,
                    if_exists='replace',
                    index=False
                )
                print("   > ✅ Concatenation Complete!")

                # --- CLEANUP STEP (Fixed) ---
                print(f"   > 🗑️ Cleaning up {len(table_list)} fragment tables...")
                
                # Use engine.begin() to ensure auto-commit of the DROP statements
                with engine.begin() as conn:
                    for t in table_list:
                        try:
                            drop_query = text(f'DROP TABLE IF EXISTS "{schema_name}"."{t}"')
                            conn.execute(drop_query)
                            print(f"     - Deleted: {t}")
                        except Exception as del_e:
                            print(f"     - ❌ Failed to delete {t}: {del_e}")
                
            else:
                print(f"⚠️ Skipping {table_list[0]} (No other tables match its columns)")

    except Exception as e:
        print(f"❌ Error during consolidation: {e}")