RECTS = [
    # [0, 50, 30, 70], # Europe
    # [-10, 0, 30, 60], # GB
    # [-20, 0, 0, 30], # Western Africa
    [0, 50, 0, 30], # Northern Africa
    [10, 30, -40, 0], # Southwestern Africa
    [30, 50, -30, 0], # Southeastern Africa
    [50, 60, 10, 20], # Oman
    [50, 90, 20, 60], # Western Asia
    [70, 90, 0, 20], # India
    [90, 170, 50, 60], # Mongolia
    [90, 150, 30, 50], # China
    [90, 130, 10, 30], # Indochina
    [90, 130, -10, 10], # Indonesia
    [110, 130, -40, -10], # Western Australia
    [130, 140, -40, 0], # Central Australia
    [140, 150, -50, 0], # Eastern Australia
    [150, 160, -40, -20], # Far Eastern Australia
    [160, 170, -50, -40], # Southwestern New Zealand
    [170, 180, -50, -30], # Eastern New Zealand
    [-80, -60, -60, -40], # Patagonia
    [-80, -50, -40, -30], # Gran Chaco
    [-80, -40, -30, 0], # Central Southern America
    [-40, -30, -20, 0], # Eastern Brasil
    [-90, -80, -10, 0], # Ecuador
    [-60, -50, 0, 10], # Guyana
    [-90, -60, 0, 20], # Caribbean
    [-110, -90, 10, 20], # Central America
    [-120, -70, 20, 60], # USA
    [-70, -50, 40, 60], # Newfoundland
    [-130, -120, 30, 60], # Pacific Coast
    [50, 160, 60, 80], # Northern Asia
    [160, 180, 60, 70], # Far East
    [-130, -10, 60, 80], # Nunavut
    [-140, -130, 50, 70], # Eastern Alaska
    [-150, -140, 60, 70], # Central Alaska
    [-160, -150, 70, 80], # Northern Alaska
    [-180, -150, 50, 70] # Bering Sea
]


def _get_combinations_from_rect(min_lon: int, max_lon: int, min_lat: int, max_lat: int):
    lon_lat_combinations = []
    lon = min_lon
    lat = min_lat
    while lon < max_lon:
        while lat < max_lat:
            lon_lat_combinations.append((lon, lat))
            lat += 10
        lon += 10
        lat = min_lat
    return lon_lat_combinations


def get_list_of_combinations():
    lon_lat_combinations = []
    for rect in RECTS:
        lon_lat_combinations.extend(_get_combinations_from_rect(rect[0], rect[1], rect[2], rect[3]))
    return lon_lat_combinations
