import os
import requests
from shapely import wkt
from typing import List
from typing import Optional
from staticmap import Polygon
from staticmap import StaticMap
import matplotlib.pyplot as plt
from shapely.geometry import shape
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon
from shapely.geometry.base import BaseGeometry


class OSPARRegions:
    """
    A class for fetching and handling OSPAR WFS component data.
    Description of data: https://odims.ospar.org/en/submissions/ospar_comp_au_2023_01/
    json url: https://odims.ospar.org/geoserver/odims/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=ospar_comp_au_2023_01_001&outputFormat=application/json
    """

    def __init__(self) -> None:
        self.url = ("https://odims.ospar.org/geoserver/odims/wfs?service=WFS&"
                    "version=2.0.0&request=GetFeature&"
                    "typeName=ospar_comp_au_2023_01_001"
                     "&outputFormat=application/json")
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
        """
        Retrieve the WKT (Well-Known Text) geometry string for a given feature
        ID.

        Optionally simplifies the geometry to reduce its size while preserving
        topology. Simplification continues iteratively until the WKT length is
        below 5000 characters or the tolerance reaches 1.0.

        :param id: The unique identifier of the feature.
        :type id: str
        :param simplify: Whether to simplify the geometry before returning.
        :type simplify: bool

        :returns: The WKT string of the geometry, or None if not found.
        :rtype: Optional[str]
        """
        geometry: BaseGeometry = self._get_geometry(id)
        if geometry is None:
            return None

        if simplify:
            tolerance = 0.001
            simplified = geometry

            while True:
                simplified = simplified.simplify(tolerance,
                                                 preserve_topology=True)
                simplified = simplified.buffer(0)  # fix minor invalidities

                # Convert single MultiPolygon to Polygon
                if isinstance(simplified, MultiPolygon) and len(
                        simplified.geoms) == 1:
                    simplified = simplified.geoms[0]

                # Optionally round coordinates to reduce WKT length
                simplified = wkt.loads(
                    wkt.dumps(simplified, rounding_precision=5))

                if len(simplified.wkt) <= 5000 or tolerance >= 1.0:
                    break

                tolerance *= 2

            geometry = simplified

        return geometry.wkt

    def get_all_ids(self) -> List[str]:
        """
        Get a list of all feature IDs in the dataset.

        :returns: A list of feature IDs.
        :rtype: List[str]
        """
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
        """
        Plot the geometry of a specific feature ID or all features on a map.

        If an ID is provided, only that feature is plotted. Otherwise, all
        features in the dataset are plotted.

        :param id: Feature ID to plot. If None, plots all features.
        :type id: Optional[str]
        :param show: Whether to display the plot interactively.
        :type show: bool
        :param output_dir: Directory to save the plot image. If None, plot is
            not saved.
        :type output_dir: Optional[str]

        :returns: None
        :rtype: None
        """
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

        m = StaticMap(800, 800)

        for feature in features:
            geom = shape(feature["geometry"])

            def coords_to_tuples(geom):
                if geom.geom_type == "Polygon":
                    return [(x, y) for x, y in geom.exterior.coords]
                elif geom.geom_type == "MultiPolygon":
                    # Return list of rings for each polygon part
                    return [
                        [(x, y) for x, y in poly.exterior.coords]
                        for poly in geom.geoms
                    ]
                else:
                    return []

            coords = coords_to_tuples(geom)

            if geom.geom_type == "Polygon":
                polygon = Polygon(coords,
                                  fill_color='#FF000080',
                                  outline_color='#FF0000')
                m.add_polygon(polygon)
            elif geom.geom_type == "MultiPolygon":
                for poly_coords in coords:
                    polygon = Polygon(poly_coords,
                                      fill_color='#FF000080',
                                      outline_color='#FF0000')
                    m.add_polygon(polygon)

        # Render map to PIL image
        image = m.render()

        # Save or show the image
        if output_dir:
            try:
                os.makedirs(output_dir, exist_ok=True)
                file_path = os.path.join(output_dir, filename)
                image.save(file_path)
                print(f"Saved map image to {file_path}")
            except Exception as e:
                print(
                    f"Warning: Could not save plot to '{output_dir}'. "
                    f"Error: {e}")

        if show:
            # Display image with matplotlib
            plt.imshow(image)
            plt.axis('off')
            plt.title(title)
            plt.show()


if __name__ == "__main__":
    comp_regions = OSPARRegions()
    print(comp_regions.get_wkt("NAAP2", simplify=False))
    print('-----')
    print(comp_regions.get_wkt("NAAP2", simplify=True))
    comp_regions.plot_map("NAAC3")
    # id_list = comp_regions.get_all_ids()
    # for item in id_list:
    #     print(item)



