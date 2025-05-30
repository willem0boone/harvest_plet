import os
import requests
from typing import List
from typing import Optional
from staticmap import Polygon
from staticmap import StaticMap
import matplotlib.pyplot as plt
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry


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
    comp_regions.plot_map("SNS")
    id_list = comp_regions.get_all_ids()
    for item in id_list:
        print(item)



