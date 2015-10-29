import ast
import geojson


# Util functions
def string_to_list_or_tuple(string_input):
    if isinstance(string_input, basestring):
        string_input = ast.literal_eval(string_input)
    return string_input


def join_coordinates(coordinates, args):
    if len(args) > 0:
        coordinates = [coordinates]
        for coordinate in args:
            coordinate = string_to_list_or_tuple(coordinate)
            coordinates.append(coordinate)
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
    geojson_point = geojson.Point((latitude, longitude))
    check_geojson_validity(geojson_point)
    return geojson.dumps(geojson_point)


def path(coordinates, *args):
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
    coordinates = string_to_list_or_tuple(coordinates)
    coordinates = join_coordinates(coordinates, args)
    geojson_path = geojson.LineString(coordinates)
    check_geojson_validity(geojson_path)
    return geojson.dumps(geojson_path)


def area(coordinates, *args):
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
    coordinates = string_to_list_or_tuple(coordinates)
    if len(args) > 0:
        coordinates = [coordinates]
        for coordinate in args:
            coordinate = string_to_list_or_tuple(coordinate)
            coordinates.append(coordinate)
    # The right GeoJSON format for Polygons is a 3 dimensional list:
    if not isinstance(coordinates[0][0], (list, tuple)):
        coordinates = [coordinates]
    geojson_area = geojson.Polygon(coordinates)
    check_geojson_validity(geojson_area)
    return geojson.dumps(geojson_area)
