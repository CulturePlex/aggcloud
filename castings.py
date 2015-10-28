import ast
import geojson


def point(latitude, longitude=None):
    """
    latitude, longitude examples:
        43.075296, -81.460172
        [43.075296, -81.460172], None
        (43.075296, -81.460172), None

        "43.075296", "-81.460172"
        "[43.075296, -81.460172]", None
        "(43.075296, -81.460172)", None

        ["43.075296", "-81.460172"], None  # This will not be possible in a CSV
        ("43.075296", "-81.460172"), None  # This will not be possible in a CSV
    """
    if not longitude:
        if isinstance(latitude, basestring):
            latitude = ast.literal_eval(latitude)
        longitude = latitude[1]
        latitude = latitude[0]
    if isinstance(latitude, basestring):
        latitude = float(latitude)
    if isinstance(longitude, basestring):
        longitude = float(longitude)
    geojson_point = geojson.Point((latitude, longitude))
    return geojson.dumps(geojson_point)


def path(coordinates):
    """
    coordinates examples:
        "[[-105, 40], [-110, 45], [-115, 55]]"
        [[-105, 40], [-110, 45], [-115, 55]]
    """
    if isinstance(coordinates, basestring):
        coordinates = ast.literal_eval(coordinates)
    geojson_path = geojson.LineString(coordinates)
    return geojson.dumps(geojson_path)


def area(coordinates):
    """
    coordinates examples:
        "[[-105, 40], [-110, 45], [-115, 55]]"
        [[-105, 40], [-110, 45], [-115, 55]]
    """
    if isinstance(coordinates, basestring):
        coordinates = ast.literal_eval(coordinates)
    geojson_area = geojson.Polygon(coordinates)
    return geojson.dumps(geojson_area)
