RECTS = [
    # [0, 50, 30, 70], # Europe
    # [-10, 0, 30, 60], # GB
    # [-20, 0, 0, 30], # Western Africa
    # [0, 50, 0, 30], # Northern Africa
    # [10, 30, -40, 0], # Southwestern Africa
    # [30, 50, -30, 0], # Southeastern Africa     L
    # [50, 60, 10, 20], # Oman
    # [50, 90, 20, 40], # Middle East
    # [50, 90, 40, 60], # Kazakhstan
    [90, 130, 40, 60], # Mongolia
    [90, 130, 20, 40], # China
    [100, 110, 10, 20],  # Indochina
    [110, 160, -40, -20],  # Australia
    [-80, -60, -30, 0],  # Peru
    [-60, -40, -30, 0],  # Brazil
    [-130, -100, 30, 60],  # Western USA
    [-100, -70, 30, 60],  # Eastern USA
    [70, 100, 0, 20], # India
    [100, 120, 0, 10], # Malaysia
    [120, 130, 0, 20], # Philippines
    [130, 170, 50, 60], # Kamchatka
    [130, 150, 30, 50], # Japan
    [100, 120, -10, 0], # Indonesia
    [120, 150, -20, 0], # Northern Australia
    [140, 150, -50, -40], # Tasmania
    [160, 170, -50, -40], # Southwestern New Zealand
    [170, 180, -50, -30], # Eastern New Zealand
    [-80, -60, -60, -40], # Patagonia
    [-80, -50, -40, -30], # Gran Chaco
    [-40, -30, -20, 0], # Eastern Brazil
    [-90, -50, 0, 10], # Colombia
    [-110, -70, 10, 30], # Mexico
    [-120, -110, 20, 30], # Baja California
    [-70, -50, 40, 60], # Newfoundland
    [50, 160, 60, 80], # Northern Asia
    [160, 180, 60, 70], # Far East
    [-130, 10, 60, 80], # Nunavut
    [-140, -130, 50, 70], # Eastern Alaska
    [-150, -140, 60, 70], # Central Alaska
    [-160, -150, 70, 80], # Northern Alaska
    [-170, -150, 50, 70], # Bering Sea
    [-180, -170, 60, 70] # Anadyr
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


def get_list_of_combinations(half: int = None):
    lon_lat_combinations = []
    for i, rect in enumerate(RECTS):
        if half and i % 2 == half:
            lon_lat_combinations.extend(
                _get_combinations_from_rect(rect[0], rect[1], rect[2], rect[3])
            )
    return lon_lat_combinations
