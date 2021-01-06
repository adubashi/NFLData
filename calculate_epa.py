

import pandas as pd
import matplotlib.pyplot as plt
import math
import numpy as np
from datetime import datetime
import random


offensive_positions = ['WR', 'RB', 'TE']
defensive_positions = ['LB', 'MLB', 'OLB', 'CB', 'FS', 'SS']

event_priority = {'pass_arrived': 1, 'pass_outcome_interception': 2}
football_Name = "Football"

excluded_penalty_list = ["RPS"] #Roughing the passer

def get_event_priority(event):
    return event_priority[event]

def get_list_of_players_from_pff():
    defense_grades = pd.read_csv("nfl-big-data-bowl-2021/pff-data/defense-grades.csv")
    position_for_analysis = "CB"
    snap_count_filter = 550

    filtered_by_position_and_snap_count = defense_grades\
        .query("@position_for_analysis == position")\
        .query("snap_counts_total > @snap_count_filter")
    return filtered_by_position_and_snap_count.player.unique()

def write_modified_tracking_dataframe():
    gameListToFileName = pd.read_csv("nfl-big-data-bowl-2021/modified-tracking-data/game_list_to_file_name.csv")
    plays = pd.read_csv("nfl-big-data-bowl-2021/plays.csv", error_bad_lines=False)
    players = pd.read_csv("nfl-big-data-bowl-2021/players.csv")
    games = pd.read_csv("nfl-big-data-bowl-2021/games.csv")
    uniqueFileNames = gameListToFileName.tracking_csv_file_name.unique()
    df_list = [pd.read_csv(file) for file in uniqueFileNames]
    tracking_df = pd.concat(df_list)
    tracking_df.to_csv("nfl-big-data-bowl-2021/aggregated-modified-tracking-data/aggregated-tracking-data.csv")

def write_pff_with_epa_report(playerList):
    epaReportDefenseGrades = compare_pff_with_epa_report(playerList)
    s = "-"
    s = s.join(playerList)
    epaReportDefenseGrades.to_csv(s + "-epa-report.csv")

def compare_pff_with_epa_report(playerList):
    playerToEPAReport = get_merged_epa_report_for_player_list(playerList)
    defense_grades = pd.read_csv("nfl-big-data-bowl-2021/pff-data/defense-grades.csv")

    epaReportDefenseGrades = playerToEPAReport.merge(defense_grades, left_on=['displayName'],
                                                  right_on=['player'],
                                                  how='inner')
    epaReportDefenseGrades_DroppedColumns = epaReportDefenseGrades[["displayName", "nflId", "position_x","defenseTeam","epa_play_count","epa","epa_per_targeted_play","player_game_count","snap_counts_total","snap_counts_coverage","grades_coverage_defense","qb_rating_against"]].copy()
    return epaReportDefenseGrades_DroppedColumns

def merge_epa_reports_for_player_list(epaReportDict):
    dfEpaReport = epaReportDict.values()
    epa_report_with_names = pd.concat(dfEpaReport)
    return epa_report_with_names

def write_epa_reports_for_player_list(playerList):
    playerToEPAReport = get_merged_epa_report_for_player_list(playerList)
    s = "-"
    s = s.join(playerList)
    playerToEPAReport.to_csv(s + "-epa-report.csv")

def write_epa_reports_for_player(playerName):
    df_epa_report = get_epa_report_for_player(playerName)
    df_epa_report.to_csv(playerName + "-epa-report.csv")

def get_epa_report_for_player(playerName, merged_tracking_df):
    epa_reports = calculate_epa_reports_for_player(playerName, merged_tracking_df)
    df_epa_report = aggregate_game_reports_for_player(epa_reports, playerName)
    return df_epa_report

def get_merged_epa_report_for_player_list(playerList):
    merged_tracking_df = pd.read_csv("nfl-big-data-bowl-2021/aggregated-modified-tracking-data/aggregated-tracking-data.csv")
    dfDict = {}
    for playerName in playerList:
            epa_report_data_frame = get_epa_report_for_player(playerName, merged_tracking_df)
            dfDict[playerName] = epa_report_data_frame
    return merge_epa_reports_for_player_list(dfDict)

def aggregate_game_reports_for_player(epa_reports, player_Name):
    print("Player Name during Aggregate: "+ player_Name)
    epa_report_list = epa_reports.values()
    if len(epa_report_list) == 0:
       return pd.DataFrame()
    df_epa_report = pd.concat(epa_report_list)
    df_epa_report = df_epa_report.query("@player_Name == displayName")
    df_epa_report = df_epa_report.groupby(['displayName', 'nflId', 'position', 'defenseTeam'])\
        .agg({'epa': 'sum', 'epa_play_count': 'sum'}).reset_index()
    for i, row in df_epa_report.iterrows():
        df_epa_report.at[i, "epa_per_targeted_play"] = df_epa_report.at[i, "epa"] / df_epa_report.at[i, "epa_play_count"]
    return df_epa_report

def calculate_epa_reports_for_player(playerName, merged_tracking_df):
    now = datetime.now()
    gameListToFileName = pd.read_csv("nfl-big-data-bowl-2021/modified-tracking-data/game_list_to_file_name.csv")
    plays = pd.read_csv("nfl-big-data-bowl-2021/plays.csv", error_bad_lines=False)
    players = pd.read_csv("nfl-big-data-bowl-2021/players.csv")
    games = pd.read_csv("nfl-big-data-bowl-2021/games.csv")
    print("Time Taken - Reading Files: " + str((datetime.now() - now).total_seconds()))
    now = datetime.now()
    tracking_df_with_player = merged_tracking_df.query('displayName == @playerName')
    game_id_list = tracking_df_with_player.gameId.unique()
    print("Time Taken - Querying for games: " + str((datetime.now() - now).total_seconds()))
    game_id_list_str = [str(i) for i in game_id_list]
    print(game_id_list_str)
    now = datetime.now()
    epa_reports = calculate_epa_reports_for_player_df(merged_tracking_df, plays, players, games, game_id_list_str, gameListToFileName)
    print("Time Taken - Report Calc: " + str((datetime.now() - now).total_seconds()))
    return epa_reports

def calculate_epa_reports_for_player_df(tracking, plays, players, games, gameIdList, gameListToFileName):
    dfDict = {}
    for i in gameIdList:
        game_data_frame = calculate_epa_game_report_with_df_tracking_set(i, gameListToFileName, tracking, plays, players, games)
        dfDict[i] = game_data_frame
    return dfDict

def calculate_epa_game_report(game_id):
    gameListToFileName = pd.read_csv("nfl-big-data-bowl-2021/modified-tracking-data/game_list_to_file_name.csv")
    fileNameForGameId = gameListToFileName.query("@game_id == gameId")
    tracking = pd.read_csv(fileNameForGameId.iloc[0].at["tracking_csv_file_name"])
    plays = pd.read_csv("nfl-big-data-bowl-2021/plays.csv", error_bad_lines=False)
    players = pd.read_csv("nfl-big-data-bowl-2021/players.csv")
    games = pd.read_csv("nfl-big-data-bowl-2021/games.csv")
    return calculate_epa_game_report_with_df_tracking_set(game_id, gameListToFileName, tracking, plays,players, games)

def calculate_epa_game_report_with_df(game_id, gameListToFileName, tracking, plays, players, games):
    ##Get tracking data only from the individual game
    tracking = tracking.query('gameId == @game_id')
    plays = plays.query('gameId == @game_id')

    now = datetime.now()
    ##Generate Football Distance for Pass Arrived And Interceptions
    generate_football_distance(tracking)
    print("Time Taken - Football Distance: " + str((datetime.now() - now).total_seconds()))

    now = datetime.now()
    ##Generate player in coverage.
    generate_coverage_player(tracking)
    print("Time Taken - Coverage Player : " + str((datetime.now() - now).total_seconds()))

    now = datetime.now()
    ##Correct For Interceptions
    correct_interceptions(tracking)
    print("Time Taken: - Interception Correction" + str((datetime.now() - now).total_seconds()))

    ##Join with Play by Play Data
    now = datetime.now()
    joined_with_play_data = tracking.merge(plays, left_on=['playId', 'gameId'],
                                                  right_on=['playId', 'gameId'],
                                                  how='inner')
    print("Time Taken: Play By Play Join" + str((datetime.now() - now).total_seconds()))
    ##Get only one frame per play event(I.E one per pass_arrived, intercepted), take interceptions over pass arrived.
    ##Only one event per play(interception or PASS arived, but not both).
    now = datetime.now()
    cleaned_play_data = get_cleaned_play_data(joined_with_play_data)
    print("Time Taken: - Get Unique Play Events" + str((datetime.now() - now).total_seconds()))

    ##Set the Away Team/Home Team, so we can set players, jersey number and penalty abbreviations
    now = datetime.now()
    set_defense_and_offense_team(cleaned_play_data, games)
    print("Time Taken: - Set Home Away Team" + str((datetime.now() - now).total_seconds()))

    ##Create a mapping of player by game
    now = datetime.now()
    players_by_game = create_players_by_game(tracking, cleaned_play_data)
    print("Time Taken: - Create Players By Game Map" + str((datetime.now() - now).total_seconds()))

    ##Remove certain penalties and assign player in coverage to the penalized player
    now = datetime.now()
    correct_penalties(cleaned_play_data, players_by_game)
    print("Time Taken: - Assign Penalties" + str((datetime.now() - now).total_seconds()))

    ##Generate the epa data by defensive players.
    now = datetime.now()
    epa_game_report = generate_epa_game_report(cleaned_play_data, players)
    print("Time Taken: - Generate the EPA Data" + str((datetime.now() - now).total_seconds()))

    #Write the data
    #epa_game_report.to_csv(game_id + "-" + "epa_game_report.csv")
    #joined_with_play_data.to_csv(game_id + "-" + "joined_with_play_data.csv")
    #cleaned_play_data.to_csv(game_id + "-" + "cleaned_play_data.csv")
    #tracking.to_csv(game_id + "-" + "tracking_data.csv")
    #players_by_game.to_csv(game_id + "-" + "players_by_game_report.csv")

    return epa_game_report

def calculate_epa_game_report_with_df_tracking_set(game_id, gameListToFileName, tracking, plays, players, games):
    ##Get tracking data only from the individual game
    tracking = tracking.query('gameId == @game_id')
    plays = plays.query('gameId == @game_id')

    ##Join with Play by Play Data
    now = datetime.now()
    joined_with_play_data = tracking.merge(plays, left_on=['playId', 'gameId'],
                                                  right_on=['playId', 'gameId'],
                                                  how='inner')
    print("Time Taken: Play By Play Join" + str((datetime.now() - now).total_seconds()))
    ##Get only one frame per play event(I.E one per pass_arrived, intercepted), take interceptions over pass arrived.
    ##Only one event per play(interception or PASS arived, but not both).
    now = datetime.now()
    cleaned_play_data = get_cleaned_play_data(joined_with_play_data)
    print("Time Taken: - Get Unique Play Events" + str((datetime.now() - now).total_seconds()))

    ##Set the Away Team/Home Team, so we can set players, jersey number and penalty abbreviations
    now = datetime.now()
    set_defense_and_offense_team(cleaned_play_data, games)
    print("Time Taken: - Set Home Away Team" + str((datetime.now() - now).total_seconds()))

    ##Create a mapping of
    now = datetime.now()
    players_by_game = create_players_by_game(tracking, cleaned_play_data)
    print("Time Taken: - Create Players By Game Map" + str((datetime.now() - now).total_seconds()))

    ##Remove certain penalties and assign player in coverage to the penalized player
    now = datetime.now()
    correct_penalties(cleaned_play_data, players_by_game)
    print("Time Taken:  -  Assign Penalities" + str((datetime.now() - now).total_seconds()))

    ##Generate the epa data by defensive players.
    now = datetime.now()
    epa_game_report = generate_epa_game_report(cleaned_play_data, players)
    print("Time Taken: - Generate the EPA Data" + str((datetime.now() - now).total_seconds()))

    #Write the data
    #epa_game_report.to_csv(game_id + "-" + "epa_game_report.csv")
    #joined_with_play_data.to_csv(game_id + "-" + "joined_with_play_data.csv")
    #cleaned_play_data.to_csv(game_id + "-" + "cleaned_play_data.csv")
    #tracking.to_csv(game_id + "-" + "tracking_data.csv")
    #players_by_game.to_csv(game_id + "-" + "players_by_game_report.csv")

    return epa_game_report

def generate_football_distance_for_tracking_week(filename):
    now = datetime.now()
    tracking_df = pd.read_csv("nfl-big-data-bowl-2021" + "/" + filename)
    print("Time Taken: - Read Tracking: " + str((datetime.now() - now).total_seconds()))

    now = datetime.now()
    generate_football_distance(tracking_df)
    print("Time Taken: - Generate Distance: " + str((datetime.now() - now).total_seconds()))

    now = datetime.now()
    generate_coverage_player(tracking_df)
    print("Time Taken: - Coverage Player: " + str((datetime.now() - now).total_seconds()))

    now = datetime.now()
    correct_interceptions(tracking_df)
    print("Time Taken: - Correct Interceptions: " + str((datetime.now() - now).total_seconds()))
    tracking_df.to_csv("nfl-big-data-bowl-2021" + "/" + filename + "-modified-with-distance-and-coverage" + ".csv")

def generate_football_distance(tracking):
    for i, row in tracking.iterrows():
        if tracking.at[i, 'event'] == 'pass_outcome_interception':
            xPosition = int(tracking.at[i, "x"])
            yPosition = int(tracking.at[i, "y"])
            play_Id = tracking.at[i, "playId"]
            frame_Id = tracking.at[i, "frameId"]
            game_Id = tracking.at[i, "gameId"]
            tracking.at[i, "total_distance_from_football_pass_outcome_interception"] = generate_distance(tracking, play_Id, frame_Id, game_Id, xPosition, yPosition)
        if tracking.at[i, 'event'] == 'pass_arrived':
            xPosition = int(tracking.at[i, "x"])
            yPosition = int(tracking.at[i, "y"])
            play_Id = tracking.at[i, "playId"]
            frame_Id = tracking.at[i, "frameId"]
            game_Id = tracking.at[i, "gameId"]
            tracking.at[i, "total_distance_from_football_pass_arrived"] = generate_distance(tracking, play_Id, frame_Id, game_Id, xPosition, yPosition)


def generate_distance(tracking, play_Id, frame_Id, game_Id, xPosition, yPosition):
    football_event = tracking.query('playId == @play_Id').query('frameId == @frame_Id').query('displayName == @football_Name').query('@game_Id == gameId')
    if not (football_event.empty):
        xFootballPosition = int(football_event.iloc[0].at["x"])
        yFootballPosition = int(football_event.iloc[0].at["y"])
        return math.sqrt((xFootballPosition - xPosition) ** 2 + ((yFootballPosition - yPosition) ** 2))

def generate_coverage_player(tracking):
    for i, row in tracking.iterrows():
        if tracking.at[i, 'event'] == 'pass_arrived':
           play_Id = tracking.at[i, "playId"]
           frame_Id = tracking.at[i, "frameId"]
           game_Id = tracking.at[i, "gameId"]
           players_at_pass_arrival = tracking.query('playId == @play_Id').query('frameId == @frame_Id');
           offensive_players_at_pass_arrival =  players_at_pass_arrival.query('position in @offensive_positions')
           defensive_players_at_pass_arrival = players_at_pass_arrival.query('position in @defensive_positions')
           targeted_player_row = offensive_players_at_pass_arrival[offensive_players_at_pass_arrival.total_distance_from_football_pass_arrived ==
                                             offensive_players_at_pass_arrival.total_distance_from_football_pass_arrived.min()]
           coverage_player_row = defensive_players_at_pass_arrival[
               defensive_players_at_pass_arrival.total_distance_from_football_pass_arrived ==
               defensive_players_at_pass_arrival.total_distance_from_football_pass_arrived.min()]
           if not (targeted_player_row.empty):
              tracking.at[i, "targeted_player_name"] = targeted_player_row.iloc[0].at["displayName"]
           if not (coverage_player_row.empty):
               tracking.at[i, "player_in_coverage"] = coverage_player_row.iloc[0].at["displayName"]

def correct_interceptions(tracking):
    for i, row in tracking.iterrows():
        if tracking.at[i, 'event'] == 'pass_outcome_interception':
           play_Id = tracking.at[i, "playId"]
           frame_Id = tracking.at[i, "frameId"]
           players_at_pass_arrival = tracking.query('playId == @play_Id').query('frameId == @frame_Id');
           offensive_players_at_pass_arrival =  players_at_pass_arrival.query('position in @offensive_positions')
           defensive_players_at_pass_arrival = players_at_pass_arrival.query('position in @defensive_positions')
           targeted_player_row = offensive_players_at_pass_arrival[offensive_players_at_pass_arrival.total_distance_from_football_pass_outcome_interception ==
                                             offensive_players_at_pass_arrival.total_distance_from_football_pass_outcome_interception.min()]
           coverage_player_row = defensive_players_at_pass_arrival[
               defensive_players_at_pass_arrival.total_distance_from_football_pass_outcome_interception ==
               defensive_players_at_pass_arrival.total_distance_from_football_pass_outcome_interception.min()]
           if not (targeted_player_row.empty):
                tracking.at[i, "targeted_player_name"] = targeted_player_row.iloc[0].at["displayName"]
           if not (coverage_player_row.empty):
                tracking.at[i, "player_in_coverage"] = coverage_player_row.iloc[0].at["displayName"]

def generate_epa_game_report(cleaned_play_data, players):
    epa_game_report = cleaned_play_data.groupby(['player_in_coverage', 'defenseTeam'])[['epa']].agg('sum').reset_index();
    epa_game_report = players.merge(epa_game_report
                                    , right_on='player_in_coverage'
                                    , left_on='displayName'
                                    , how='inner')
    epa_game_report.drop(['height', 'weight', 'birthDate', 'displayName'], axis=1)
    for i, row in epa_game_report.iterrows():
        playerInCoverage = epa_game_report.at[i, "player_in_coverage"]
        df = cleaned_play_data.query("@playerInCoverage == player_in_coverage")
        epa_game_report.at[i, "epa_play_count"] = len(df.index)
        epa_game_report.at[i, "epa_per_targeted_play"] = epa_game_report.at[i, "epa"] / len(df.index)
    return epa_game_report

def correct_penalties(cleaned_play_data, players_by_game):
    cleaned_play_data = cleaned_play_data.query("penaltyCodes not in @excluded_penalty_list")
    for i, row in cleaned_play_data.iterrows():
        penaltyCodesFromRow = cleaned_play_data.at[i, "penaltyJerseyNumbers"]
        if not pd.isna(penaltyCodesFromRow):
            rows = players_by_game.query("penaltyAbbr in @penaltyCodesFromRow")
            if not rows.empty:
                playerName = rows.iloc[0].at["displayName"]
                cleaned_play_data.at[i, 'player_in_coverage'] = playerName

def set_defense_and_offense_team(cleaned_play_data, games):
    for i, row in cleaned_play_data.iterrows():
        game_Id = cleaned_play_data.at[i, "gameId"]
        homeTeam = games.query('gameId == @game_Id').iloc[0].at["homeTeamAbbr"]
        awayTeam = games.query('gameId == @game_Id').iloc[0].at["visitorTeamAbbr"]
        if homeTeam == cleaned_play_data.at[i, "possessionTeam"]:
            cleaned_play_data.at[i, "defenseTeam"] = awayTeam
        else:
            cleaned_play_data.at[i, "defenseTeam"] = homeTeam

def create_players_by_game(tracking, cleaned_play_data):
    players_by_game = pd.DataFrame(columns=['displayName', 'jerseyNumber'])
    for i in tracking.displayName.unique():
        players_by_game = players_by_game.append({'displayName': i}, ignore_index=True)
    for i, row in players_by_game.iterrows():
        display_Name = players_by_game.at[i, 'displayName']
        players_by_game.at[i, 'jerseyNumber'] = tracking.query("displayName == @display_Name").iloc[0].at[
            "jerseyNumber"]
        defensivePlayer = cleaned_play_data.query("player_in_coverage == @display_Name")
        offensivePlayer = cleaned_play_data.query("displayName == @display_Name")
        if not offensivePlayer.empty:
            players_by_game.at[i, 'teamAbbr'] = offensivePlayer.iloc[0].at["possessionTeam"]
            players_by_game.at[i, 'penaltyAbbr'] = players_by_game.at[i, 'teamAbbr'] + str(
                int(players_by_game.at[i, 'jerseyNumber']))
        if not defensivePlayer.empty:
            players_by_game.at[i, 'teamAbbr'] = defensivePlayer.iloc[0].at["defenseTeam"]
            players_by_game.at[i, 'penaltyAbbr'] = players_by_game.at[i, 'teamAbbr'] + " " + str(
                int(players_by_game.at[i, 'jerseyNumber']))
    return players_by_game

def get_event_priority(event):
    return event_priority[event]

def get_cleaned_play_data(joined_with_play_data):
    ##Get only one frame per play event(I.E one per pass_arrived, intercepted).
    cleaned_play_data = joined_with_play_data[joined_with_play_data["displayName"] == joined_with_play_data["targeted_player_name"]]

    ##Set the event priority
    cleaned_play_data["event_priority"] = cleaned_play_data.apply(lambda row : get_event_priority(row["event"]), axis = 1)

    ##Sort Based on Event Priority
    cleaned_play_data.sort_values("event_priority", ascending=True)

    ##Remove duplicates on play ID and ONLY keep the highest priority event
    cleaned_play_data = cleaned_play_data.drop_duplicates(subset=['playId'], keep="last")
    return cleaned_play_data


def plot_game_id(plotDF):
    plotDF = plotDF.loc[plotDF['epa_play_count'] >= 3]
    plotDF.sort_values("epa_per_targeted_play", ascending=True)
    plotDF.plot.bar(x="player_in_coverage", y="epa_per_targeted_play", rot=0, title="EPA by Player")
    plt.xticks(rotation=90)
    plt.show()

def calculate_and_plot(game_id):
    plotDF = calculate_epa_game_report(game_id)
    plotDF.to_csv(game_id + "-" + "epa_game_report.csv")
    plot_game_id(plotDF)