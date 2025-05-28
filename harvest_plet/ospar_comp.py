import os
import geopandas as gpd
from typing import List
from typing import Optional
import contextily as ctx
import matplotlib.pyplot as plt
from shapely.geometry import base
from shapely.geometry.base import BaseGeometry


class OSPARRegions:
    """
    A class for fetching and handling OSPAR WFS component data from a GeoJSON
    endpoint.
    """

    def __init__(self, url: str = "https://odims.ospar.org/geoserver/odims/wfs"
                                  "?service=WF"
                                  "S&version=2.0.0"
                                  "&request=GetFeature"
                                  "&typeName=ospar_comp_au_2023_01_001"
                                  "&outputFormat=application/json"
                 ) -> None:
        """
        Initialize the OSPARRegions object and load data from the given URL.

        :param url: The WFS endpoint URL returning GeoJSON data.
        :type url: str
        """
        self.url: str = url
        self.gdf: gpd.GeoDataFrame = self._get_json()

    def _get_json(self) -> gpd.GeoDataFrame:
        """
        Fetch and return a GeoDataFrame from the GeoJSON URL.

        :return: A GeoDataFrame containing the spatial data.
        :rtype: geopandas.GeoDataFrame
        :raises IOError: If the URL cannot be accessed or parsed as GeoJSON.
        """
        try:
            gdf = gpd.read_file(self.url)
        except Exception as e:
            raise IOError(f"Error accessing or reading GeoJSON data: {e}")
        return gdf

    def _get_geometry(self, id: str) -> Optional[BaseGeometry]:
        """
        Return the raw geometry for a feature with the given ID.

        :param id: The feature ID to search for.
        :type id: str
        :return: The shapely geometry object, or None if the ID is not found.
        :rtype: Optional[BaseGeometry]
        """
        if 'ID' not in self.gdf.columns:
            raise ValueError("Column 'ID' not found in GeoDataFrame.")
        match = self.gdf[self.gdf['ID'] == id]
        if not match.empty:
            return match.geometry.iloc[0]
        return None

    def get_wkt(self, id: str, simplify: bool = False) -> Optional[str]:
        """
        Return the WKT geometry for a feature with the given ID.
        Optionally simplifies the geometry to ensure the WKT string length
        stays below 100,000 characters.

        :param id: The feature ID to search for.
        :type id: str
        :param simplify: Whether to simplify the geometry to reduce WKT length.
            When using the wkt in URL requests, a too long URL causes error414.
        :type simplify: bool
        :return: The WKT representation of the geometry, or None if not found.
        :rtype: Optional[str]
        """
        geometry: Optional[base.BaseGeometry] = self._get_geometry(id)
        if not geometry:
            return None

        if simplify:
            tolerance = 0.001  # Start with very small simplification
            simplified = geometry.simplify(tolerance, preserve_topology=True)
            while len(simplified.wkt) > 5000 and tolerance < 1.0:
                tolerance *= 2
                simplified = geometry.simplify(tolerance,
                                               preserve_topology=True)
            geometry = simplified

        return geometry.wkt

    def get_all_ids(self) -> List[str]:
        """
        Return a list of all feature IDs in the GeoDataFrame.

        :return: List of all values in the 'ID' column.
        :rtype: List[str]
        :raises ValueError: If the 'ID' column is not present in the dataset.
        """
        if 'ID' not in self.gdf.columns:
            raise ValueError("Column 'ID' not found in GeoDataFrame.")
        return self.gdf['ID'].dropna().unique().tolist()

    def plot_map(self,
                 id: Optional[str] = None,
                 show: bool = True,
                 output_dir: Optional[str] = None
                 ) -> None:
        """
        Plot a region by ID or the entire dataset if ID is not provided.
        Optionally display and/or save the plot with an OSM background.

        :param id: The feature ID to plot. If None, plots all features.
        :type id: Optional[str]
        :param show: Whether to display the plot interactively.Default True.
        :type show: bool
        :param output_dir: Optional directory to save the plot as PNG.
        :type output_dir: Optional[str]
        :return: None
        :rtype: None
        """
        if 'ID' not in self.gdf.columns:
            raise ValueError("Column 'ID' not found in GeoDataFrame.")

        # Use full dataset if ID is not provided
        if id is None:
            data_to_plot = self.gdf.copy()
            filename = "full_dataset.png"
            title = "OSPAR Regions"
        else:
            data_to_plot = self.gdf[self.gdf['ID'] == id]
            if data_to_plot.empty:
                raise ValueError(f"No feature with ID '{id}' found.")
            filename = f"{id}.png"
            title = f"Region ID: {id}"

        # Reproject to Web Mercator for contextily
        data_to_plot = data_to_plot.to_crs(epsg=3857)

        fig, ax = plt.subplots()
        data_to_plot.plot(ax=ax, edgecolor='black', facecolor='lightblue',
                          alpha=0.6)
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
        ax.set_title(title)
        plt.axis('off')

        saved = False
        if output_dir:
            try:
                os.makedirs(output_dir, exist_ok=True)
                file_path = os.path.join(output_dir, filename)
                plt.savefig(file_path, bbox_inches='tight')
                saved = True
            except Exception as e:
                print(
                    f"Warning: Could not save plot to '{output_dir}'. Error: {e}")

        if show:
            plt.show()
        elif not saved:
            print(
                "Warning: 'show' is False and no valid 'output_dir' provided. "
                "Plot was not saved or displayed.")

        plt.close()

    def download_single_region(self,
                               id: str,
                               output_dir: str,
                               format: str = "ESRI Shapefile"
                               ) -> None:
        """
        Export a single feature to a file in the specified format.

        :param id: The feature ID to export.
        :type id: str
        :param output_dir: Directory where the file will be saved.
        :type output_dir: str
        :param format: File format (e.g., "ESRI Shapefile", "GeoJSON").
        :type format: str
        :return: None
        :rtype: None
        """
        if 'ID' not in self.gdf.columns:
            raise ValueError("Column 'ID' not found in GeoDataFrame.")

        match = self.gdf[self.gdf['ID'] == id]
        if match.empty:
            raise ValueError(f"No feature with ID '{id}' found.")

        os.makedirs(output_dir, exist_ok=True)

        filename = os.path.join(output_dir, f"{id}.shp") \
            if format == "ESRI Shapefile" \
            else os.path.join(output_dir, f"{id}.geojson")

        match.to_file(filename, driver=format)

    def download_all_regions(self, output_dir: str) -> None:
        """
        Download and save the entire dataset as a Shapefile.

        :param output_dir: Directory where the shapefile will be saved.
        :type output_dir: str
        :return: None
        :rtype: None
        """
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, "ospar_regions.shp")
        self.gdf.to_file(filepath, driver="ESRI Shapefile")


# if __name__ == "__main__":
#     comp_regions = OSPARRegions()
#     comp_regions.plot_map("SNS")


