# imports

import io
import zipfile
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import requests

"""Read in file with final employment data (will be refactored to not be in csv form later"""

qualifying_employment_areas = pd.read_csv(
    "/Users/mcastillo/Documents/Github/rmi-energy-communities/notebooks/files/qualifying_employment_areas.csv",
    dtype={"geoid": "str"},
)

qualifying_employment_areas = qualifying_employment_areas.rename(
    columns=({"geoid": "GEOID"})
)

"""Function to convert shapefile to json, which is necessary for working with plotly."""


def shapefile_to_json(link):
    url = link
    local_path = "tmp/"

    print("Downloading shapefile...")
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    print("Done")
    z.extractall(path=local_path)  # extract to folder
    filenames = [
        y
        for y in sorted(z.namelist())
        for ending in ["dbf", "prj", "shp", "shx"]
        if y.endswith(ending)
    ]
    print(filenames)

    dbf, prj, shp, shx = (filename for filename in filenames)
    geodf = gpd.read_file(local_path + shp)

    geodf = geodf.to_crs("WGS84")

    geodf = geodf.loc[geodf["GEOID"].isin(qualifying_employment_areas["GEOID"])]

    return geodf


"""Function to create chloropleth plotly map from json """


def make_map_from_json(df, df2, filename):

    fig = px.choropleth_mapbox(
        df,
        geojson=df.geometry,
        locations=df.index,
        color_continuous_scale="red",
        mapbox_style="open-street-map",
        zoom=9,
        hover_name="NAME",
    )

    fig.add_scattermapbox(
        lat=df2.Latitude,
        lon=df2.Longitude,
        mode="markers+text",
        text=df2["Site Name"],
        marker_size=2,
        marker_color="rgb(235, 0, 100)",
    )

    fig.update_layout(
        title="Qualifying Energy Community Areas - Employment Criteria",
        height=1000,
        autosize=True,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        geo_scope="usa",
    )

    fig.update_traces(name="brownfield site", selector=dict(type="scattermapbox"))

    fig.update_traces(
        name="fossil employment area", selector=dict(type="choroplethmapbox")
    )

    fig.show()

    # py.plot(fig,filename=filename,auto_open=True)
