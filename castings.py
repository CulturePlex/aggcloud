import ast
import geojson

# If the coordinates come in (Latitude, Longitude) format, setting
# "REVERSE_COORDINATES" to "True" it will reverse them for be in
# "GeoJSON format".
REVERSE_COORDINATES = True


# Util functions
def string_to_list_or_tuple(string_input):
    if isinstance(string_input, basestring):
        string_input = ast.literal_eval(string_input)
    return string_input


def join_coordinates(coordinates):
    if len(coordinates) > 1:
        coordinates = [string_to_list_or_tuple(coordinate)
                       for coordinate in coordinates]
    else:
        coordinates = string_to_list_or_tuple(coordinates[0])
    return coordinates


def check_geojson_validity(geojson_input):
    validity = geojson.is_valid(geojson_input)
    if validity['valid'] == 'no':
        raise ValueError(validity['message'])


# Casting functions
def point(latitude, longitude=None):
    """
    latitude, longitude examples:
        43.075296, -81.460172
        [43.075296, -81.460172], None
        (43.075296, -81.460172), None

        "43.075296", "-81.460172"
        "[43.075296, -81.460172]", None
        "(43.075296, -81.460172)", None

        ["43.075296", "-81.460172"], None
        ("43.075296", "-81.460172"), None
    """
    if not longitude:
        latitude = string_to_list_or_tuple(latitude)
        longitude = latitude[1]
        latitude = latitude[0]
    latitude = float(latitude)
    longitude = float(longitude)
    coordinates = (latitude, longitude)
    if REVERSE_COORDINATES:
        coordinates = coordinates[::-1]
    geojson_point = geojson.Point(coordinates)
    check_geojson_validity(geojson_point)
    return geojson.dumps(geojson_point)


def path(*coordinates):
    """
    coordinates examples:
        "[[-105, 40], [-110, 45], [-115, 55]]"
        [[-105, 40], [-110, 45], [-115, 55]]

        "[(-105, 40), (-110, 45), (-115, 55)]"
        [(-105, 40), (-110, 45), (-115, 55)]

        (-105, 40), (-110, 45), (-115, 55)
        [-105, 40], [-110, 45], [-115, 55]

        "(-105, 40)", "(-110, 45)", "(-115, 55)"
        "[-105, 40]", "[-110, 45]", "[-115, 55]"
    """
    coordinates = join_coordinates(coordinates)
    if REVERSE_COORDINATES:
        coordinates = [coors[::-1] for coors in coordinates]
    geojson_path = geojson.LineString(coordinates)
    check_geojson_validity(geojson_path)
    return geojson.dumps(geojson_path)


def area(*coordinates):
    """
    coordinates examples:
        "[[[-105, 40], [-110, 45], [-115, 55], [-105, 40]]]"
        [[[-105, 40], [-110, 45], [-115, 55], [-105, 40]]]

        "[[(-105, 40), (-110, 45), (-115, 55), (-105, 40)]]"
        [[(-105, 40), (-110, 45), (-115, 55), (-105, 40)]]

        "[[-105, 40], [-110, 45], [-115, 55], [-105, 40]]"
        [[-105, 40], [-110, 45], [-115, 55], [-105, 40]]

        "[(-105, 40), (-110, 45), (-115, 55), (-105, 40)]"
        [(-105, 40), (-110, 45), (-115, 55), (-105, 40)]

        (-105, 40), (-110, 45), (-115, 55), (-105, 40)
        [-105, 40], [-110, 45], [-115, 55], [-105, 40]
        "(-105, 40)", "(-110, 45)", "(-115, 55)", "(-105, 40)"
        "[-105, 40]", "[-110, 45]", "[-115, 55]", "[-105, 40]"
    """
    coordinates = join_coordinates(coordinates)
    # The right GeoJSON format for Polygons is a 3 dimensional list:
    if not isinstance(coordinates[0][0], (list, tuple)):
        coordinates = [coordinates]
    # The last coordinate of a polygon must be the same that the first:
    for polygon in coordinates:
        if polygon[0] != polygon[-1]:
            polygon.append(polygon[0])
    if REVERSE_COORDINATES:
        coordinates = [[coors[::-1] for polygon in coordinates
                        for coors in polygon]]
    geojson_area = geojson.Polygon(coordinates)
    check_geojson_validity(geojson_area)
    return geojson.dumps(geojson_area)


def combine_lon_lat(*params):
    """
    Simple example creating coordinates using params
    """
    coordinates = "_".join(params)
    return ("Coordinates", coordinates)


def number(value):
    for func in [int, float, long]:
        try:
            return func(value)
        except ValueError:
            pass
        else:
            break
    return value
