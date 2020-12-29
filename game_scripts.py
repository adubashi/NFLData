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