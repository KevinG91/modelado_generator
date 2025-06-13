#!/bin/bash

# List of table names
tables=(
  "ow_g20_ind_mud_check"
  "ow_g20_bha_componentes"
  "ow_g18_kpi_cementing"
  "ow_g11_pozos_survey"
  "ow_g27_bit_summary"
  "ow_casing_components"
  "ow_casing_general"
  "ow_cement_general"
  "ow_cement_stage"
  "ow_cement_test"
  "ow_g11_oper_perfil_oh"
  "ow_g11_log_las_files"
)

database="pae_dataplatform_standarized"

# Get the directory where the script is located
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output_dir="${script_dir}/../outputs/table-schemas"

mkdir -p "$output_dir"

for table in "${tables[@]}"; do
  echo "Processing $table..."
  
  aws glue get-table \
    --database-name "$database" \
    --name "$table" \
    --query "Table.StorageDescriptor.Columns[*].[Name, Type]" \
    --output text > "${output_dir}/${table}.txt"
  
  if [ $? -ne 0 ]; then
    echo "❌ Failed to get schema for table: $table"
    rm -f "${output_dir}/${table}.txt"
  else
    echo "✅ Schema saved to ${output_dir}/${table}.txt"
  fi
done

echo "All tables processed."
