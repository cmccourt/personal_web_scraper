import pytest

from src.web_scraping.eihl_website_scraping import extract_team_match_stats


@pytest.mark.parametrize("match_url,expected",
                         [("https://www.eliteleague.co.uk/game/4002-fif-she/team-stats",
                           {"home_team": {'Shots': 31.0, 'Shots on goal': 25.0,
                                          'Shots efficiency': 12.00,
                                          'Power plays': 3.0, 'Power play efficiency': 66.67,
                                          'Penalty minutes': 8.0,
                                          'Penalty kill efficiency': 66.67,
                                          'Saves': 37.0, 'Save percentage': 94.87,
                                          'Faceoffs won': 40.0},
                            "away_team": {'Shots': 61.0, 'Shots on goal': 39.0,
                                          'Shots efficiency': 5.13, 'Power plays': 3.0,
                                          'Power play efficiency': 33.33,
                                          'Penalty minutes': 8.0,
                                          'Penalty kill efficiency': 33.33, 'Saves': 22.0,
                                          'Save percentage': 88.00, 'Faceoffs won': 33.0}}),

                          ("https://www.eliteleague.co.uk/game/2154-lon-bas/team-stats",
                           {"home_team": {'Shots': 0, 'Shots on goal': 0,
                                          'Shots efficiency': 0,
                                          'Power plays': 0, 'Power play efficiency': 0,
                                          'Penalty minutes': 0,
                                          'Penalty kill efficiency': 0,
                                          'Saves': 0, 'Save percentage': 0,
                                          'Faceoffs won': 0},
                            "away_team": {'Shots': 0, 'Shots on goal': 0,
                                          'Shots efficiency': 0, 'Power plays': 0,
                                          'Power play efficiency': 0,
                                          'Penalty minutes': 0,
                                          'Penalty kill efficiency': 0, 'Saves': 0,
                                          'Save percentage': 0, 'Faceoffs won': 0}})])
def test_extract_team_match_stats(match_url, expected):
    match_team_stats = extract_team_match_stats(match_url)

    for team, stats in match_team_stats.items():
        if stats != expected[team]:
            pytest.fail(f"expected result: {expected[team]} \n"
                        f"Actual result: {stats}")
