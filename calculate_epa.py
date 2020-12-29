

import pandas as pd
import matplotlib.pyplot as plt
import math

def calculate(game_id):
    offensive_positions = ['WR', 'RB', 'TE']
    defensive_positions = ['LB', 'MLB', 'OLB', 'CB', 'FS', 'SS']

    event_priority = {'pass_arrived': 1, 'pass_outcome_interception': 2}

    ##Get the tracking info
    gameListToFileName = pd.read_csv("nfl-big-data-bowl-2021/game_list_to_file_name.csv")
    fileNameList = gameListToFileName.query('gameId == @game_id')
    tracking = pd.read_csv(fileNameList.iloc[0].at["tracking_csv_file_name"])
    plays = pd.read_csv("nfl-big-data-bowl-2021/plays.csv", error_bad_lines=False)
    players = pd.read_csv("nfl-big-data-bowl-2021/players.csv")
    games = pd.read_csv("nfl-big-data-bowl-2021/games.csv")

    ##Get tracking data only from the individual game
    tracking = tracking.query('gameId == @game_id')
    plays = plays.query('gameId == @game_id')

    ##Generate Football Distance for Pass Arrived And Interceptions
    football_Name = "Football"
    for i, row in tracking.iterrows():
        if tracking.at[i, 'event'] == 'pass_outcome_interception':
            xPosition = int(tracking.at[i, "x"])
            yPosition = int(tracking.at[i, "y"])
            play_Id = tracking.at[i, "playId"]
            frame_Id = tracking.at[i, "frameId"]
            football_event = tracking.query('playId == @play_Id').query('frameId == @frame_Id').query(
                'displayName == @football_Name')
            xFootballPosition = int(football_event.iloc[0].at["x"])
            yFootballPosition = int(football_event.iloc[0].at["y"])
            tracking.at[i, "total_distance_from_football_pass_outcome_interception"] = math.sqrt((xFootballPosition - xPosition) ** 2 + ((yFootballPosition - yPosition) ** 2))
        if tracking.at[i, 'event'] == 'pass_arrived':
            xPosition = int(tracking.at[i, "x"])
            yPosition = int(tracking.at[i, "y"])
            play_Id = tracking.at[i, "playId"]
            frame_Id = tracking.at[i, "frameId"]
            football_event = tracking.query('playId == @play_Id').query('frameId == @frame_Id').query(
                'displayName == @football_Name')
            xFootballPosition = int(football_event.iloc[0].at["x"])
            yFootballPosition = int(football_event.iloc[0].at["y"])
            tracking.at[i, "total_distance_from_football_pass_arrived"] = math.sqrt((xFootballPosition - xPosition) ** 2 + ((yFootballPosition - yPosition) ** 2))

    ##Generate player in coverage.
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
           tracking.at[i, "targeted_player_name"] = targeted_player_row.iloc[0].at["displayName"]
           tracking.at[i, "player_in_coverage"] = coverage_player_row.iloc[0].at["displayName"]

    ##Correct For Interceptions
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
           tracking.at[i, "targeted_player_name"] = targeted_player_row.iloc[0].at["displayName"]
           tracking.at[i, "player_in_coverage"] = coverage_player_row.iloc[0].at["displayName"]

    ##Join with Play by Play Data
    joined_with_play_data = tracking.merge(plays, left_on=['playId', 'gameId'],
                                                  right_on=['playId', 'gameId'],
                                                  how='inner')
    ##Get only one frame per play event(I.E one per pass_arrived, intercepted).
    cleaned_play_data = joined_with_play_data[joined_with_play_data["displayName"] ==
                                              joined_with_play_data["targeted_player_name"]]
    ##Set the event priority
    for i, row in cleaned_play_data.iterrows():
        cleaned_play_data.at[i, "event_priority"] = event_priority[cleaned_play_data.at[i, "event"]]

    ##Sort Based on Event Priority
    cleaned_play_data.sort_values("event_priority", ascending=True)

    ##Remove duplicates on play ID and ONLY keep the highest priority event
    cleaned_play_data = cleaned_play_data.drop_duplicates(subset=['playId'], keep="last")

    ##Set the Away Team/Home Team, so we can set players, jersey number and penalty abbreviations
    players_by_game = pd.DataFrame(columns=['displayName', 'jerseyNumber'])
    for i, row in cleaned_play_data.iterrows():
        game_Id = cleaned_play_data.at[i, "gameId"]
        homeTeam = games.query('gameId == @game_Id').iloc[0].at["homeTeamAbbr"]
        awayTeam = games.query('gameId == @game_Id').iloc[0].at["visitorTeamAbbr"]
        if homeTeam == cleaned_play_data.at[i, "possessionTeam"]:
            cleaned_play_data.at[i, "defenseTeam"] = awayTeam
        else:
            cleaned_play_data.at[i, "defenseTeam"] = homeTeam
    for i in tracking.displayName.unique():
            players_by_game = players_by_game.append({'displayName': i}, ignore_index=True)
    for i, row in players_by_game.iterrows():
            display_Name = players_by_game.at[i, 'displayName']
            players_by_game.at[i, 'jerseyNumber'] = tracking.query("displayName == @display_Name").iloc[0].at["jerseyNumber"]
            defensivePlayer = cleaned_play_data.query("player_in_coverage == @display_Name")
            offensivePlayer = cleaned_play_data.query("displayName == @display_Name")
            if not offensivePlayer.empty:
                players_by_game.at[i, 'teamAbbr'] = offensivePlayer.iloc[0].at["possessionTeam"]
                players_by_game.at[i, 'penaltyAbbr'] = players_by_game.at[i, 'teamAbbr'] + str(int(players_by_game.at[i, 'jerseyNumber']))
            if not defensivePlayer.empty:
                players_by_game.at[i, 'teamAbbr'] = defensivePlayer.iloc[0].at["defenseTeam"]
                players_by_game.at[i, 'penaltyAbbr'] = players_by_game.at[i, 'teamAbbr'] + " " + str(int(players_by_game.at[i, 'jerseyNumber']))

    ##Remove certain penalties and assign player in coverage to the penalized player
    penalty_list = ["RPS"] #Roughing the passer
    cleaned_play_data = cleaned_play_data.query("penaltyCodes not in @penalty_list")
    for i, row in cleaned_play_data.iterrows():
        penaltyCodesFromRow = cleaned_play_data.at[i, "penaltyJerseyNumbers"]
        if not pd.isna(penaltyCodesFromRow):
            rows = players_by_game.query("penaltyAbbr in @penaltyCodesFromRow")
            if (rows.count() != 0).any():
                playerName = rows.iloc[0].at["displayName"]
                cleaned_play_data.at[i, 'player_in_coverage'] = playerName

    ##Generate the epa data by defensive players.
    epa_game_report = cleaned_play_data.groupby(['player_in_coverage','defenseTeam'])[['epa']].agg('sum').reset_index();

    for i, row in epa_game_report.iterrows():
        playerName = epa_game_report.at[i, "player_in_coverage"]
        playerInCoverage = epa_game_report.at[i, "player_in_coverage"]
        playerRow = players.query("@playerName == displayName")
        epa_game_report.at[i, "nflId"] = playerRow.iloc[0].at["nflId"]
        df = cleaned_play_data.query("@playerInCoverage == player_in_coverage")
        epa_game_report.at[i, "epa_play_count"] = len(df.index)
        epa_game_report.at[i, "epa_per_targeted_play"] = epa_game_report.at[i, "epa"]/len(df.index)

    #Write the data
    epa_game_report.to_csv(game_id + "-" + "epa_game_report.csv")
    joined_with_play_data.to_csv(game_id + "-" + "joined_with_play_data.csv")
    cleaned_play_data.to_csv(game_id + "-" + "cleaned_play_data.csv")
    tracking.to_csv(game_id + "-" + "tracking_data.csv")
    players_by_game.to_csv(game_id + "-" + "players_by_game_report.csv")

def plot_game_id(game_id):
    plotDF = pd.read_csv(game_id + "-epa_game_report.csv")
    print(plotDF)
    plotDF = plotDF.loc[plotDF['epa_play_count'] > 3]
    plotDF.sort_values("epa_per_targeted_play", ascending=True)
    plotDF.plot.bar(x="player_in_coverage", y="epa_per_targeted_play", rot=0, title="EPA by Player")
    plt.xticks(rotation=90)
    plt.show()


def calculate_and_plot(game_id):
    calculate(game_id)
    plot_game_id(game_id)