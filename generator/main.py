import argparse
import logging
import os
from tqdm import tqdm
from typing import Any, Dict, List, Tuple

import pandas as pd

import yaml

from utils import normalizer, sorter

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory (secretaria)
PARENT_DIR = os.path.dirname(SCRIPT_DIR)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate SQL files for Glue catalog and S3 populators"
    )
    # Change default for the name of YOUR document
    parser.add_argument(
        "--excel-file",
        help="Path to the Excel file with ingestion details",
        default=os.path.join(SCRIPT_DIR, "Solicitud de ingesta - Secretaria.xlsx"),
    )
    parser.add_argument(
        "--schemas-path",
        help="Path to the directory containing schema files",
        default=os.path.join(PARENT_DIR, "outputs", "table-schemas"),
    )
    parser.add_argument(
        "--catalog-output",
        help="Path to the directory for Glue catalog SQL files",
        default=os.path.join(PARENT_DIR, "outputs", "glue-catalog-populators"),
    )
    parser.add_argument(
        "--s3-output",
        help="Path to the directory for S3 populator SQL files",
        default=os.path.join(PARENT_DIR, "outputs", "s3-populator"),
    )
    parser.add_argument(
        "--config",
        help="Path to configuration file",
        default=os.path.join(PARENT_DIR, "config.yaml"),
    )
    return parser.parse_args()


def generate_glue_catalog_populators(
    origin: List[str],
    field: List[str],
    schema: str,
    lakehouse_table_names: pd.DataFrame,
    replace: Dict[str, str],
    destination: str,
) -> None:
    """Generate Glue catalog SQL files for table creation."""
    try:
        # Fixed audit fields (you can tweak the types if needed)
        audit_fields = [
            "fecha_audit_create TIMESTAMP",
            "proceso_audit_create STRING",
            "fecha_audit_update TIMESTAMP",
            "proceso_audit_update STRING",
        ]

        # Combine fields into SQL-formatted strings
        field_lines = [f"    `{name}` {ftype}," for name, ftype in zip(origin, field)]

        # Add audit fields
        field_lines.extend([f"    {field}" for field in audit_fields])

        # Join fields
        all_fields = "\n".join(field_lines)

        # Get lakehouse table name
        lakehouse_naming = (
            lakehouse_table_names.loc[
                lakehouse_table_names["Nombre en Origen"] == replace[schema]
            ]["Nombre Interface en Lakehouse"].values[0]
            + "_th"
        )

        # SQL template
        sql = f"""CREATE EXTERNAL TABLE IF NOT EXISTS pae_dataplatform_lakehouse.{lakehouse_naming} (
        {all_fields}
        )
        STORED AS PARQUET
        LOCATION 's3://$LAKEHOUSE_BUCKET/intervencionesdepozo/eventos/structured/{lakehouse_naming}';"""

        # Output path
        output_path = os.path.join(destination, f"{lakehouse_naming}.sql")

        # Write to .sql file
        with open(output_path, "w", encoding="utf-8") as out_file:
            out_file.write(sql)

        logger.info(f"Generated Glue catalog populator for {schema} at {output_path}")
    except Exception as e:
        logger.error(f"Error generating Glue catalog populator for {schema}: {str(e)}")
        raise


def generate_s3_populator(
    origin: List[str],
    alias: List[str],
    schema: str,
    lakehouse_table_names: pd.DataFrame,
    replace: Dict[str, str],
    destination: str,
) -> None:
    """Generate S3 populator SQL files for data transformation."""
    try:
        audit_fields = [
            "current_timestamp AS fecha_audit_create,",
            "'drilling-standarized2lakehouse-pipeline' AS proceso_audit_create,",
            "null AS fecha_audit_update,",
            "null AS proceso_audit_update",
        ]

        s3p = [f"`{orig}` AS {alias_val}," for orig, alias_val in zip(origin, alias)]

        # Combine fields into SQL-formatted strings
        field_lines = [f"    {field}" for field in s3p]

        # Add audit fields
        field_lines.extend([f"    {field}" for field in audit_fields])

        # Join fields
        all_fields = "\n".join(field_lines)

        # Get lakehouse table name
        lakehouse_naming = (
            lakehouse_table_names.loc[
                lakehouse_table_names["Nombre en Origen"] == replace[schema]
            ]["Nombre Interface en Lakehouse"].values[0]
            + "_th"
        )

        # SQL template
        sql = f"""SELECT
        {all_fields}
        FROM $SOURCE_DATABASE.{schema};"""

        # Output path
        output_path = os.path.join(destination, f"{lakehouse_naming}_lh_sec.sql")

        # Write to .sql file
        with open(output_path, "w", encoding="utf-8") as out_file:
            out_file.write(sql)

        logger.info(f"Generated S3 populator for {schema} at {output_path}")
    except Exception as e:
        logger.error(f"Error generating S3 populator for {schema}: {str(e)}")
        raise


def load_schema_data(schema_path: str, schema_name: str) -> Tuple[List[str], List[str]]:
    """Load field names and types from a schema file."""
    origin_fields = []
    field_types = []

    input_path = os.path.join(schema_path, f"{schema_name}.txt")

    try:
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) == 2:
                    origin_fields.append(parts[0])
                    field_types.append(parts[1])
        return origin_fields, field_types
    except Exception as e:
        logger.error(f"Error loading schema data for {schema_name}: {str(e)}")
        raise


def load_config_file(config_path):
    """Load configuration from YAML file."""
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading configuration file: {str(e)}")
        return {}


def get_config() -> Dict[str, Any]:
    """Return configuration parameters for the script."""
    args = parse_args()

    # Load config from file
    file_config = load_config_file(args.config)

    # Combine file config with command line arguments
    config = {
        "tablas_solicitud_ingesta": file_config.get("tablas_solicitud_ingesta", []),
        "replacements": file_config.get("replacements", {}),
        "excel_file": args.excel_file,
        "schemas_path": args.schemas_path,
        "catalog_output_folder": args.catalog_output,
        "s3_output_folder": args.s3_output,
    }

    return config


def main():
    """Main execution function."""
    try:
        # Load configuration
        config = get_config()

        # Create output directories
        os.makedirs(config["catalog_output_folder"], exist_ok=True)
        os.makedirs(config["s3_output_folder"], exist_ok=True)

        # Load Excel data
        logger.info(f"Loading data from {config['excel_file']}")
        ingesta_detalle_campos = pd.read_excel(
            config["excel_file"], sheet_name="Campos Lakehouse", skiprows=2
        )
        ingesta_tablas_lakehouse = pd.read_excel(
            config["excel_file"], sheet_name="Tablas Lakehouse", skiprows=2
        )

        # Get schema list
        schemas = [
            os.path.splitext(f)[0]
            for f in os.listdir(config["schemas_path"])
            if os.path.isfile(os.path.join(config["schemas_path"], f))
        ]
        logger.info(f"Found {len(schemas)} schemas to process")

        # Process each schema
        for current_schema in tqdm(schemas, desc="Processing schemas"):
            logger.info(f"Processing schema: {current_schema}")

            # Skip schemas not in replacements
            if current_schema not in config["replacements"]:
                logger.warning(
                    f"Schema {current_schema} not found in replacements, skipping"
                )
                continue

            # Get aliases for current schema
            try:
                aliases = list(
                    ingesta_detalle_campos[
                        ingesta_detalle_campos["Nombre Interface en Origen"]
                        == config["replacements"][current_schema]
                    ]["Campo (Nombre Lakehouse)"]
                )
            except Exception as e:
                logger.error(f"Error getting aliases for {current_schema}: {str(e)}")
                continue

            # Load schema data
            try:
                origin_fields, field_types = load_schema_data(
                    config["schemas_path"], current_schema
                )
            except Exception as e:
                logger.error(f"Skipping schema {current_schema} due to error: {str(e)}")
                continue

            # Process fields
            normalized_origin_fields = [normalizer(f) for f in origin_fields]
            aligned_aliases = sorter(
                origin=origin_fields, normalized=normalized_origin_fields, alias=aliases
            )

            # Generate output files
            generate_glue_catalog_populators(
                origin=origin_fields,
                field=field_types,
                schema=current_schema,
                lakehouse_table_names=ingesta_tablas_lakehouse,
                replace=config["replacements"],
                destination=config["catalog_output_folder"],
            )

            generate_s3_populator(
                origin=origin_fields,
                alias=aligned_aliases,
                schema=current_schema,
                lakehouse_table_names=ingesta_tablas_lakehouse,
                replace=config["replacements"],
                destination=config["s3_output_folder"],
            )

        logger.info("Processing completed successfully")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise


if __name__ == "__main__":
    main()
