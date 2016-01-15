import ast
import geojson

# If the coordinates come in (Latitude, Longitude) format, setting
# "REVERSE_COORDINATES" to "True" it will reverse them for be in
# "GeoJSON format", but only when they are coming inside list(s).
REVERSE_COORDINATES = True


# Datatypes to apply the default casting
DATATYPE = {
    u's': 'string',
    u'b': 'boolean',
    u'n': 'number',
    u'x': 'string',
    u'f': 'float_func',
    u'r': 'string',
    u'i': 'number',
    u'o': 'number',
    u'e': 'string'
}


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
        if REVERSE_COORDINATES:
            longitude = latitude[1]
            latitude = latitude[0]
        else:
            longitude = latitude[0]
            latitude = latitude[1]
    latitude = float(latitude)
    longitude = float(longitude)
    geojson_point = geojson.Point((longitude, latitude))
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


def float_func(value):
    try:
        return float(value)
    except ValueError:
        return value


def string(value):
    try:
        return unicode(value)
    except ValueError:
        return value


def boolean(value):
    if isinstance(value, (str, unicode)):
        if value == 'True':
            return True
        else:
            return False
    try:
        return bool(value)
    except ValueError:
        return value


def default(value):
    return value
