# coding=utf-8
# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from datetime import datetime, time, timedelta

import numpy as np
import pandas as pd
import json
import pytz
from pandas import Series
from pandas.io.json import json_normalize
import math

def get_quote(df, displayName):
    return df[displayName]

def pass_arrived(event):
    return event == "pass_arrived"

def get_tracking_csv_for_game_id(game_id):
    list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
    game_id = int(game_id)
    for i in list:
        weekFile = "nfl-big-data-bowl-2021/week" + str(i) + ".csv"
        df = pd.read_csv(weekFile)
        if df.gameId[df.gameId == game_id].count() != 0:
           return weekFile

def create_game_id_to_tracking_csv_map():
    game_list = pd.read_csv("nfl-big-data-bowl-2021/game_id_list.csv", error_bad_lines=False)
    k = 0
    for i, row in game_list.iterrows():
        game_Id = game_list.at[i, "gameId"]
        k = k + 1
        print(game_Id)
        print(k)
        game_list.at[i, "tracking_csv_file_name"] = get_tracking_csv_for_game_id(game_Id)
    game_list.to_csv("game_list_to_file_name.csv")




def calculate(game_id):
    offensive_positions = ['WR', 'RB', 'TE']
    defensive_positions = ['LB', 'MLB', 'OLB', 'CB', 'FS', 'SS']

    ##Get the tracking info
    gameListToFileName = pd.read_csv("nfl-big-data-bowl-2021/game_list_to_file_name.csv")
    fileNameList = gameListToFileName.query('gameId == @game_id')
    fileName = fileNameList.iloc[0].at["tracking_csv_file_name"]
    print(fileNameList.iloc[0].at["tracking_csv_file_name"])


    tracking = pd.read_csv(fileName)
    plays = pd.read_csv("nfl-big-data-bowl-2021/plays.csv", error_bad_lines=False)
    players = pd.read_csv("nfl-big-data-bowl-2021/players.csv")
    games = pd.read_csv("nfl-big-data-bowl-2021/games.csv")

    ##Get tracking data only from the individual game
    tracking = tracking.query('gameId == @game_id')
    plays = plays.query('gameId == @game_id')


    ##Generate Football Distance
    for i, row in tracking.iterrows():
        if tracking.at[i, 'event'] == 'pass_arrived':
           xPosition =  int(tracking.at[i, "x"])
           yPosition = int(tracking.at[i, "y"])
           play_Id = tracking.at[i, "playId"]
           frame_Id = tracking.at[i, "frameId"]
           football_Name = "Football"
           football_event = tracking.query('playId == @play_Id').query('frameId == @frame_Id').query('displayName == @football_Name')
           xFootballPosition = int(football_event.iloc[0].at["x"])
           yFootballPosition = int(football_event.iloc[0].at["y"])
           tracking.at[i, "total_distance_from_football"] = math.sqrt((xFootballPosition - xPosition)**2+((yFootballPosition - yPosition)**2))

    ##Generate player in coverage.
    for i, row in tracking.iterrows():
        if tracking.at[i, 'event'] == 'pass_arrived':
           play_Id = tracking.at[i, "playId"]
           frame_Id = tracking.at[i, "frameId"]
           game_Id = tracking.at[i, "gameId"]
           homeTeam = games.query('gameId == @game_Id').iloc[0].at["homeTeamAbbr"]
           awayTeam = games.query('gameId == @game_Id').iloc[0].at["visitorTeamAbbr"]
           players_at_pass_arrival = tracking.query('playId == @play_Id').query('frameId == @frame_Id');
           offensive_players_at_pass_arrival =  players_at_pass_arrival.query('position in @offensive_positions')
           defensive_players_at_pass_arrival = players_at_pass_arrival.query('position in @defensive_positions')
           targeted_player_row = offensive_players_at_pass_arrival[offensive_players_at_pass_arrival.total_distance_from_football ==
                                             offensive_players_at_pass_arrival.total_distance_from_football.min()]
           coverage_player_row = defensive_players_at_pass_arrival[
               defensive_players_at_pass_arrival.total_distance_from_football ==
               defensive_players_at_pass_arrival.total_distance_from_football.min()]
           tracking.at[i, "targeted_player_name"] = targeted_player_row.iloc[0].at["displayName"]
           tracking.at[i, "player_in_coverage"] = coverage_player_row.iloc[0].at["displayName"]


    ##Join with Play by Play Data
    joined_with_play_data = tracking.merge(plays, left_on=['playId', 'gameId'],
                                                  right_on=['playId', 'gameId'],
                                                  how='inner')
    cleaned_play_data = joined_with_play_data[joined_with_play_data["displayName"] ==
                                              joined_with_play_data["targeted_player_name"]]

    cleaned_play_data.to_csv(game_id + "-" + "cleaned_play_data.csv")

    ##Set the Away Team/Home Team, so we can set players
    for i, row in cleaned_play_data.iterrows():
        game_Id = cleaned_play_data.at[i, "gameId"]
        homeTeam = games.query('gameId == @game_Id').iloc[0].at["homeTeamAbbr"]
        awayTeam = games.query('gameId == @game_Id').iloc[0].at["visitorTeamAbbr"]
        if homeTeam == cleaned_play_data.at[i, "possessionTeam"]:
            cleaned_play_data.at[i, "defenseTeam"] = awayTeam
        else:
            cleaned_play_data.at[i, "defenseTeam"] = homeTeam

    ##Generate the epa data by defensive players.
    epa_game_report = cleaned_play_data.groupby(['player_in_coverage','defenseTeam'])[['epa']].agg('sum').reset_index();

    for i, row in epa_game_report.iterrows():
        playerName = epa_game_report.at[i, "player_in_coverage"]
        playerRow = players.query("@playerName == displayName")
        epa_game_report.at[i, "nflId"] = playerRow.iloc[0].at["nflId"]

    #Write the data
    epa_game_report.to_csv(game_id + "-" + "epa_game_report.csv")


'''
def calculate2():
    players = pd.read_csv("nfl-big-data-bowl-2021/players.csv")
    games = pd.read_csv("nfl-big-data-bowl-2021/games.csv")
    cleaned_play_data = pd.read_csv("cleaned_play_data.csv")
    for i, row in cleaned_play_data.iterrows():
        game_Id = cleaned_play_data.at[i, "gameId"]
        homeTeam = games.query('gameId == @game_Id').iloc[0].at["homeTeamAbbr"]
        awayTeam = games.query('gameId == @game_Id').iloc[0].at["visitorTeamAbbr"]
        if homeTeam == cleaned_play_data.at[i, "possessionTeam"]:
           cleaned_play_data.at[i, "defenseTeam"] = awayTeam
        else:
           cleaned_play_data.at[i, "defenseTeam"] = homeTeam
    cleaned_play_data.to_csv("cleaned_play_data_2.csv")
'''

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    now = datetime.now()
    calculate("2018090600")
    print(datetime.now() - now)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
