RECTS = [
    # [0, 50, 30, 70], # Europe
    # [-10, 0, 30, 60], # GB
    # [-20, 0, 0, 30], # Western Africa
    # [0, 50, 0, 30], # Northern Africa
    # [10, 30, -40, 0], # Southwestern Africa
    # [30, 50, -30, 0], # Southeastern Africa
    # [50, 60, 10, 20], # Oman
    # [50, 90, 20, 40], # Middle East
    # [50, 90, 40, 60], # Kazakhstan
    # [90, 130, 40, 60], # Mongolia
    # [90, 130, 20, 40], # China
    # [100, 110, 10, 20],  # Indochina
    # [110, 160, -40, -20],  # Australia
    # [-80, -60, -30, 0],  # Peru
    # [-60, -40, -30, 0],  # Brazil
    # [-130, -100, 30, 60],  # Western USA
    # [-100, -70, 30, 60],  # Eastern USA
    # [50, 160, 60, 70],  # Russia
    # [160, 180, 60, 70],  # Far East
    # [-130, -90, 60, 70],  # Nunavut
    # [-140, -130, 50, 70],  # Eastern Alaska
    # [-150, -140, 60, 70],  # Central Alaska

    [-80, -50, -40, -30],  # Gran Chaco                 2
    [130, 170, 50, 60],  # Kamchatka                    3
    [-90, -50, 60, 70],  # Baffin Island                4
    [-170, -150, 50, 70],  # Bering Sea                 4
    [50, 110, 70, 80],  # Taimyr                        6
    [110, 160, 70, 80],  # Northeastern Siberia         5
    [-90, -70, 70, 80],  # Southern Ellesmere Island    2
    [-100, -90, 70, 80],  # Queen Elizabeth Islands     1
    [-100, -70, 70, 80],  # Northern Ellesmere Island   3
    [-70, -40, 70, 80],  # Northwestern Greenland       3

    [-70, -50, 40, 60],  # Newfoundland                 3
    [-110, -70, 10, 30],  # Mexico                      5
    [-90, -50, 0, 10],  # Colombia                      2
    [120, 150, -20, 0],  # Northern Australia           1
    [100, 120, 0, 10],  # Malaysia                      2
    [-40, -30, -20, 0],  # Eastern Brazil               2
    [-180, -170, 60, 70],  # Anadyr                     1

    [70, 100, 0, 20],  # India                          4
    [-50, 10, 60, 70],  # Southern Greenland            4
    [130, 150, 30, 50],  # Japan                        3
    [-80, -60, -60, -40],  # Patagonia                  2
    [120, 130, 0, 20],  # Philippines                   2
    [100, 120, -10, 0],  # Indonesia                    2
    [140, 150, -50, -40],  # Tasmania                   1
    [170, 180, -50, -30],  # Eastern New Zealand        2
    [160, 170, -50, -40],  # Southwestern New Zealand   1
    [-120, -110, 20, 30],  # Baja California            1

    # [-70, 10, 70, 80], # Greenland                      6
    # [-130, -100, 70, 80], # Victoria Island             3
    # [-40, -10, 70, 80],  # Northeastern Greenland       3
    # [-160, -150, 70, 80], # Northern Alaska             1
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
        if half is not None and i % 2 == half:
            continue
        lon_lat_combinations.extend(
            _get_combinations_from_rect(rect[0], rect[1], rect[2], rect[3])
        )
    return lon_lat_combinations
