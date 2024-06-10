"""Loads SIC codes from the ONS to BigQuery.

SIC (Standard Industrial Classification) codes are used to classify companies data in
the Companies House records. They are published by the ONS here:
https://www.ons.gov.uk/methodology/classificationsandstandards/ukstandardindustrialclassificationofeconomicactivities
"""

from io import BytesIO
from typing import Literal, get_args

import httpx
import polars as pl

from companies_house.save_polars_to_bq import save_dataframe_to_bq

# Define the SIC Code grouping levels (above "class").
LevelNames = Literal["section", "division", "group"]

# The URI for the latest revision (2007) of the SIC codes.
SIC_CODES_URI = "https://www.ons.gov.uk/file?uri=/methodology/classificationsandstandards/ukstandardindustrialclassificationofeconomicactivities/uksic2007/publisheduksicsummaryofstructureworksheet.xlsx"


def load_sic_codes_to_bigquery(table_name: str = "sic_codes") -> None:
    """Extract, transform then load SIC codes from the ONS URI to a BigQuery table.

    Args:
        table_name: The table name to save to (combined with BQ_DATASET setting to
          create the table ID). Defaults to "sic_codes".
    """
    load_sic_codes()
    # Load data to BigQuery table (with default replace=True).
    save_dataframe_to_bq(load_sic_codes(), table_name)


def load_sic_codes() -> pl.DataFrame:
    """Extract, transform then load SIC codes from the ONS URI to a polars Dataframe.

    Args:
        table_name: The table name to save to (combined with BQ_DATASET setting to
          create the table ID). Defaults to "sic_codes".
    """
    # polars read_excel requires a Byte stream.
    with httpx.Client() as client:
        r = client.get(SIC_CODES_URI)
        df = pl.read_excel(BytesIO(r.content))

    df = (
        # Make columns snake_case.
        df.rename(lambda column_name: column_name.lower().replace(" ", "_"))
        # The "section" column has extra preceding whitespace, so need to strip it out.
        .with_columns(pl.all().str.strip_chars())
    )

    level_names = get_args(LevelNames)

    classes = (
        # Filter classes.
        df.filter(pl.col("level_headings").is_in(["Class", "Sub Class"]))
        # Rearrange col order and rename one column to "class".
        .select(
            [
                *level_names,
                pl.col("most_disaggregated_level").alias("class"),
                "description",
            ]
        )
    )
    # Convert level codes and descriptions to a struct, and join onto the
    # taxonomy of the classes.
    sic_codes = classes.clone()
    for level_name in level_names:
        level = process_levels(df, level_name)
        sic_codes = (
            sic_codes.join(level, on=level_name, coalesce=True)
            .drop([level_name, "description_right"])
            .rename({f"{level_name}_struct": level_name})
        )

    # Reorder so level names are at the start.
    sic_codes = sic_codes.select(*level_names, pl.exclude(*level_names))
    # NOTE: Set sorted flags not yet working for Struct types in polars. There
    # was the hope that BigQuery would auto-detect to use nested and repeated
    # fields in the schema with this but doesn't seem to be the case currently.
    return sic_codes.set_sorted(level_names)


def process_levels(df: pl.DataFrame, level_name: LevelNames) -> pl.DataFrame:
    """Get code and description for given level name and gather into a Struct column.

    Gather the code and description for each level into a struct to make use of nested
    and repeated columns when loading to BigQuery.

    Args:
        df: The polars dataframe with columns: "level_headings", "description" and the
          given level name.
        level_name: One of the grouping levels: ["section", "division", "group"].

    Returns:
        Filtered dataframe with additional struct-type column of the "code" and
        "description" columns, where "code" is an alias for the original level
        name column.
    """
    return (
        df.filter(pl.col("level_headings").str.to_lowercase() == level_name)
        .select([level_name, "description"])
        .with_columns(
            pl.struct(pl.col(level_name).alias("code"), pl.col("description")).alias(
                f"{level_name}_struct"
            )
        )
    )


if __name__ == "__main__":
    # Load to BigQuery when main is run.
    load_sic_codes_to_bigquery()
