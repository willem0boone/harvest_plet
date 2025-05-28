import os
import requests
from typing import List
from typing import Optional
import matplotlib.pyplot as plt
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

import cartopy.crs as ccrs
import cartopy.feature as cfeature


class OSPARRegions:
    """
    A class for fetching and handling OSPAR WFS component data.
    """

    def __init__(self, url: str = "https://odims.ospar.org/geoserver/odims/wfs"
                                  "?service=WFS&version=2.0.0"
                                  "&request=GetFeature"
                                  "&typeName=ospar_comp_au_2023_01_001"
                                  "&outputFormat=application/json") -> None:
        self.url = url
        self.data = self._get_json()

    def _get_json(self) -> dict:
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise IOError(f"Error accessing or reading GeoJSON data: {e}")

    def _get_feature_by_id(self, id: str) -> Optional[dict]:
        for feature in self.data.get("features", []):
            if feature["properties"].get("ID") == id:
                return feature
        return None

    def _get_geometry(self, id: str) -> Optional[BaseGeometry]:
        feature = self._get_feature_by_id(id)
        if feature:
            return shape(feature["geometry"])
        return None

    def get_wkt(self, id: str, simplify: bool = False) -> Optional[str]:
        geometry = self._get_geometry(id)
        if not geometry:
            return None

        if simplify:
            tolerance = 0.001
            simplified = geometry.simplify(tolerance, preserve_topology=True)
            while len(simplified.wkt) > 5000 and tolerance < 1.0:
                tolerance *= 2
                simplified = geometry.simplify(tolerance,
                                               preserve_topology=True)
            geometry = simplified

        return geometry.wkt

    def get_all_ids(self) -> List[str]:
        return [
            feature["properties"].get("ID")
            for feature in self.data.get("features", [])
            if feature["properties"].get("ID")
        ]

    def plot_map(self,
                 id: Optional[str] = None,
                 show: bool = True,
                 output_dir: Optional[str] = None
                 ) -> None:

        features = self.data["features"]

        if id:
            features = [f for f in features if f["properties"].get("ID") == id]
            if not features:
                raise ValueError(f"No feature with ID '{id}' found.")
            filename = f"{id}.png"
            title = f"Region ID: {id}"
        else:
            filename = "full_dataset.png"
            title = "OSPAR Regions"

        # Create a cartopy GeoAxes with PlateCarree projection (lon/lat)
        fig = plt.figure(figsize=(10, 10))
        ax = plt.axes(projection=ccrs.PlateCarree())
        ax.set_title(title)

        # Add basic cartopy features for basemap
        ax.add_feature(cfeature.LAND, facecolor='lightgray')
        ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
        ax.add_feature(cfeature.COASTLINE)
        ax.add_feature(cfeature.BORDERS, linestyle=':')
        ax.gridlines(draw_labels=True, dms=True, x_inline=False,
                     y_inline=False)

        # Plot geometries on the map
        for feature in features:
            geom = shape(feature["geometry"])
            if geom.geom_type == "Polygon":
                x, y = geom.exterior.xy
                ax.fill(x, y, facecolor="r", edgecolor="black",
                        alpha=0.6,
                        transform=ccrs.PlateCarree())
            elif geom.geom_type == "MultiPolygon":
                for poly in geom.geoms:
                    x, y = poly.exterior.xy
                    ax.fill(x, y, facecolor="r", edgecolor="black",
                            alpha=0.6,
                            transform=ccrs.PlateCarree())

        # Set map extent to the bounds of all features plotted
        all_coords = []
        for feature in features:
            geom = shape(feature["geometry"])
            all_coords.extend(
                list(geom.exterior.coords) if geom.geom_type == "Polygon" else
                [pt for poly in geom.geoms for pt in poly.exterior.coords])
        if all_coords:
            xs, ys = zip(*all_coords)
            buffer = 0.5  # degree buffer around features
            ax.set_extent([min(xs) - buffer, max(xs) + buffer,
                           min(ys) - buffer, max(ys) + buffer],
                          crs=ccrs.PlateCarree())

        saved = False
        if output_dir:
            try:
                os.makedirs(output_dir, exist_ok=True)
                file_path = os.path.join(output_dir, filename)
                plt.savefig(file_path, bbox_inches="tight")
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


# if __name__ == "__main__":
#     comp_regions = OSPARRegions()
#     comp_regions.plot_map()


