from io import BytesIO

import httpx
import polars as pl

from companies_house.save_polars_to_bq import save_dataframe_to_bq


def load_sic_codes():
    """ """
    sic_codes_2007_uri = "https://www.ons.gov.uk/file?uri=/methodology/classificationsandstandards/ukstandardindustrialclassificationofeconomicactivities/uksic2007/publisheduksicsummaryofstructureworksheet.xlsx"
    with httpx.Client() as client:
        r = client.get(sic_codes_2007_uri)
        df = pl.read_excel(BytesIO(r.content))

    # The "section" column has extra preceding whitespace, so need to strip it.
    df = df.rename(
        lambda column_name: column_name.lower().replace(" ", "_")
    ).with_columns(pl.all().str.strip_chars())

    level_names = ["section", "division", "group"]

    classes = df.filter(pl.col("level_headings").is_in(["Class", "Sub Class"])).select(
        [*level_names, pl.col("most_disaggregated_level").alias("class"), "description"]
    )
    sic_codes = classes.clone()
    for level_name in level_names:
        level = process_levels(df, level_name)
        sic_codes = (
            sic_codes.join(level, on=level_name, coalesce=True)
            .drop([level_name, "description_right"])
            .rename({f"{level_name}_struct": level_name})
        )

    sic_codes = sic_codes.select(*level_names, pl.exclude(*level_names))
    sic_codes_sorted = sic_codes.sort(level_names, descending=False).set_sorted(
        level_names
    )
    save_dataframe_to_bq(sic_codes_sorted, "sic_codes")


def process_levels(df: pl.DataFrame, level_name: str):
    """ """
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
    load_sic_codes()
