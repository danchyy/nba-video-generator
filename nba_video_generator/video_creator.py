import time
import subprocess
import argparse

import pandas as pd

from nba_api.stats.static.teams import find_teams_by_full_name
from nba_api.stats.static.players import find_players_by_full_name
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import videoevents
from nba_api.stats.endpoints import playbyplayv2
import requests


_MADE_SHOTS = "shots_made"
_MISSED_SHOTS = "missed_shots"
_ASSISTS = "assists"


def get_game_for_team(team_name: str, game_date: str):
    team_id = find_teams_by_full_name(team_name)[0]['id']

    gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
    games = gamefinder.get_data_frames()[0]
    return games.loc[games.GAME_DATE == game_date]


def get_player_id(player_name):
    return find_players_by_full_name(player_name)[0]['id']


def get_game_for_player(player_id, game_date: str):
    gamefinder = leaguegamefinder.LeagueGameFinder(player_or_team_abbreviation='P', player_id_nullable=player_id)
    games = gamefinder.get_data_frames()[0]

    return games.loc[games.GAME_DATE == game_date].GAME_ID


def get_pbp_for_game(game_id):
    pbp = playbyplayv2.PlayByPlayV2(game_id=game_id)
    pbp = pbp.get_data_frames()[0]
    return pbp


def filter_home_away(pbp: pd.DataFrame, is_home: bool):
    if is_home:
        return pbp.loc[pbp.HOMEDESCRIPTION.notnull()]
    return pbp.loc[pbp.VISITORDESCRIPTION.notnull()]


def get_all_made_shots(pbp: pd.DataFrame):
    made_shots = pbp.loc[pbp.EVENTMSGTYPE == 1]
    return made_shots


def get_all_missed_shots(pbp: pd.DataFrame):
    made_shots = pbp.loc[pbp.EVENTMSGTYPE == 2]
    return made_shots


def get_all_made_shots_by_player(pbp: pd.DataFrame, player_id):
    made_shots = pbp.loc[pbp.EVENTMSGTYPE == 1]
    return made_shots.loc[made_shots.PLAYER1_ID == player_id]


def get_all_missed_shots_by_player(pbp: pd.DataFrame, player_id):
    made_shots = pbp.loc[pbp.EVENTMSGTYPE == 2]
    return made_shots.loc[made_shots.PLAYER1_ID == player_id]


def get_all_assists_by_player(pbp: pd.DataFrame, player_id):
    all_made_shots = get_all_made_shots(pbp)
    return all_made_shots.loc[all_made_shots.PLAYER2_ID == player_id]


def dump_videos_for_data_frame(pbp: pd.DataFrame):
    for index in range(len(pbp)):
        item = pbp.iloc[index]
        game_id = item.GAME_ID
        event_id = item.EVENTNUM
        video_event = videoevents.VideoEvents(game_id=game_id, game_event_id=event_id)
        uuid = video_event.get_dict()['resultSets']['Meta']['videoUrls'][0]['uuid']
        playlist_dict = video_event.get_dict()['resultSets']['playlist'][0]
        video_url = f"https://videos.nba.com/nba/pbp/media/{playlist_dict['y']}/{playlist_dict['m']}/{playlist_dict['d']}/{game_id}/{event_id}/{uuid}_1280x720.mp4"
        resp = requests.get(video_url)

        with open(f'videos/doncic_ast_{index + 1}.mp4', 'wb') as f:
            f.write(resp.content)
            f.flush()
        print(f"Finished with {index + 1}. item out of {len(pbp)}")
        time.sleep(1)

    lines = [f"file videos/doncic_ast_{index + 1}.mp4\n" for index in range(len(pbp))]

    with open('video_list.txt', 'w') as f:
        f.writelines(lines)

    subprocess.call(["ffmpeg", "-safe", "0", "-f", "concat", "-i", "video_list.txt", "-c", "copy", "output.mp4"])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--game_date",
        help="Game date in format of YYYY-MM-DD",
        type=str
    )
    parser.add_argument(
        "--player_name",
        help="Name of the player",
        type=str
    )
    parser.add_argument(
        "--target_highlight",
        help="Game date in format of YYYY-MM-DD",
        type=str,
        choices=[_MADE_SHOTS, _MISSED_SHOTS, _ASSISTS]
    )
    args = parser.parse_args()

    game_date = args.game_date
    player_name = args.player_name

    target_highlight = args.target_highlight

    player_id = get_player_id(player_name)

    pbp_df = get_pbp_for_game(get_game_for_player(player_id=player_id, game_date=game_date))

    if target_highlight == _MADE_SHOTS:
        filtered_df = get_all_made_shots_by_player(pbp=pbp_df, player_id=player_id)
    elif target_highlight == _MISSED_SHOTS:
        filtered_df = get_all_missed_shots_by_player(pbp=pbp_df, player_id=player_id)
    elif target_highlight == _ASSISTS:
        filtered_df = get_all_assists_by_player(pbp=pbp_df, player_id=player_id)
    else:
        raise ValueError("Mistake")
    dump_videos_for_data_frame(filtered_df)
