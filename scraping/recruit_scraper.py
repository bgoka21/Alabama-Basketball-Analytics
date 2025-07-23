from twofourseven.twofourseven import getBBPlayerData


def get_recruits(year: int):
    """Fetch recruit data for the given year using the twofourseven package."""
    players = getBBPlayerData(year)
    return players
