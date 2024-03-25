"""Extract EIA 860 plants data and load into pandas dataframe."""

import pandas as pd
import sqlalchemy as sa

import pudl


def extract(pudl_engine: sa.engine.Engine | None = None) -> pd.DataFrame:
    """Extract EIA 860 generators PUDL output table.

    PUDL workspace settings need to be set to point to PUDL DB.
    See PUDL documentation for more info.
    """
    if pudl_engine is None:
        pudl_engine = sa.create_engine(pudl.workspace.setup.get_defaults()["pudl_db"])

    pudl_out = pudl.output.pudltabl.PudlTabl(pudl_engine, freq="AS")
    gens = pudl_out.gens_eia860()
    return gens
