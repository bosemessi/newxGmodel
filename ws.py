## Read all json files
import tqdm 
import json 
import pandas as pd 
import numpy as np 
import os
import unidecode

def jsonfiles_to_h5(jsonfiles, h5file, append=True):
    eventtypesdf = pd.DataFrame(eventtypes, columns=["type_id", "type_name"])
    eventtypesdf.to_hdf(h5file, key="eventtypes")
    append=True
    seen_files = set()
    if append:
        try:
            df = pd.read_hdf(h5file, key="files")
            seen_files = set(df.file_url)
        except KeyError:
            pass
    jsonfiles = set(jsonfiles) - seen_files

    d : dict = {
        key: []
        for key in [
            "games",
            "teams",
            "players",
            "teamgamestats",
            "playergamestats",
            "files",
        ]
    }
    with pd.HDFStore(h5file) as optastore:
        for jsonfile_url in tqdm.tqdm(jsonfiles):
            try:
                data = extract_data(jsonfile_url)

                d["games"] += [data["game"]]
                d["players"] += data["players"]
                d["teams"] += data["teams"]
                d["teamgamestats"] += data["teamgamestats"]
                d["playergamestats"] += data["playergamestats"]

                game_id = data["game"]["game_id"]
                key = f"events/game_{game_id}"
                eventsdf = pd.DataFrame(data["events"])
                optastore[key] = eventsdf
                d["files"] += [{"file_url": jsonfile_url, "corrupt": False}]
            except (ValueError, MissingDataError):
                d["files"] += [{"file_url": jsonfile_url, "corrupt": True}]

        deduplic = dict(
            games=("game_id", "game_id"),
            teams=(["team_id", "team_name"], "team_id"),
            players=(
                ["player_id", "firstname", "lastname", "fullname"],
                "player_id",
            ),
            teamgamestats=(["game_id", "team_id"], ["game_id", "team_id"]),
            playergamestats=(
                ["game_id", "team_id", "player_id"],
                ["game_id", "team_id", "player_id"],
            ),
            files=("file_url", "file_url"),
        )

        for k, v in d.items():
            d[k] = pd.DataFrame(v)

        for k, df in d.items():
            if append:
                try:
                    ori_df = optastore[k]
                    df = pd.concat([ori_df, df])
                except (FileNotFoundError, KeyError):
                    pass
            sortcols, idcols = deduplic[k]
            df.sort_values(by=sortcols, ascending=False, inplace=True)
            df.drop_duplicates(subset=idcols, inplace=True)
            optastore[k] = df

def extract_data(jsonfile):
    with open(jsonfile, encoding="utf-8") as fh:
        root = json.load(fh)

    return {
        "game": extract_game(root),
        "players": extract_players(root),
        "teams": extract_teams(root),
        "teamgamestats": extract_teamgamestats(root),
        "playergamestats": extract_playergamestats(root),
        "events": extract_events(root),
    }

class MissingDataError(Exception):
    pass

def assertget(dictionary, key):
    value = dictionary.get(key)
    assert value is not None, "KeyError: " + key + " not found in " + str(dictionary)
    return value
def extract_game(root):
    game_dict = dict(
        game_id=int(assertget(root, "matchId")),
        home_team_id=int(assertget(root['home'], "teamId")),
        away_team_id=int(assertget(root['away'], "teamId")),
    )
    return game_dict
def extract_players(root):
    teams = [root['home'],root['away']]
    players = []
    for team in teams:
        for player in team["players"]:
            player_id = int(player['playerId'])
            player = dict(
                    player_id=player_id,
                    firstname=player['name'].split()[0] or None,
                    lastname=player['name'].split()[-1] or None,
                    fullname=player['name'] or None,
                )
            for f in ["firstname", "lastname", "fullname"]:
                if player[f]:
                    player[f] = unidecode.unidecode(player[f])
            players.append(player)
    return players
def extract_teams(root):
    teamroot = [root['home'],root['away']]
    teams = []
    for team in teamroot:
        if "teamId" in team.keys():
            team = dict(
                team_id=int(team["teamId"]),
                team_name=team["name"],
            )
            for f in ["team_name"]:
                if team[f]:
                    team[f] = unidecode.unidecode(team[f])
            teams.append(team)
    return teams
def extract_teamgamestats(root):
    game_id = extract_game(root)["game_id"]
    teamroot = [root['home'],root['away']]
    teams_gamestats = []
    for team in teamroot:
        team_gamestats = dict(
            game_id=game_id,
            team_id=int(team['teamId']),
            side=team['field'],
            score=team["scores"],
        )

        teams_gamestats.append(team_gamestats)
    return teams_gamestats
def extract_playergamestats(root):
    game_id = extract_game(root)["game_id"]
    teamroot = [root['home'],root['away']]
    players_gamestats = []
    for team in teamroot:
        team_id = int(team['teamId'])
        for player in team["players"]:
            p = dict(
                game_id=game_id,
                team_id=team_id,
                player_id=int(player['playerId']),
                shirtnumber=int(player["shirtNo"]),
                player_type=player["position"],
            )
            players_gamestats.append(p)
    return players_gamestats
def extract_events(root):
    game_id = extract_game(root)["game_id"]
    events = []
    for element in assertget(root, "events"):
        qualifiers = {}
        qualifiers = {int(q["type"]["value"]): q["value"] if 'value' in q else None
                      for q in element.get("qualifiers", [])}
        start_x = float(assertget(element, "x")) if "x" in list(element.keys()) else 0.0
        start_y = float(assertget(element, "y")) if "y" in list(element.keys()) else 0.0
        end_x = get_end_x(qualifiers)
        end_y = get_end_y(qualifiers)
        if end_x is None:
            end_x = start_x
        if end_y is None:
            end_y = start_y

        event = dict(
            game_id=game_id,
            event_id=int(assertget(element, "eventId")) if "eventId" in list(element.keys()) else 0,
            type_id=int(assertget(element['type'], "value")),
            period_id=int(assertget(element['period'], "value")),
            minute=int(assertget(element, "minute")),
            second=int(assertget(element, "second")) if "second" in list(element.keys()) else 0,
            player_id=int(assertget(element, "playerId")) if "playerId" in list(element.keys()) else 0,
            team_id=int(assertget(element, "teamId")),
            outcome=assertget(element['outcomeType'],'value'),
            start_x=start_x,
            start_y=start_y,
            end_x=end_x,
            end_y=end_y,
            qualifiers=qualifiers,
        )
        events.append(event)
    return events
def get_end_x(qualifiers):
    try:
        # pass
        if 140 in qualifiers:
            return float(qualifiers[140])
        # blocked shot
        elif 146 in qualifiers:
            return float(qualifiers[146])
        # passed the goal line
        elif 102 in qualifiers:
            return float(100)
        else:
            return None
    except ValueError:
        return None


def get_end_y(qualifiers):
    try:
        # pass
        if 141 in qualifiers:
            return float(qualifiers[141])
        # blocked shot
        elif 147 in qualifiers:
            return float(qualifiers[147])
        # passed the goal line
        elif 102 in qualifiers:
            return float(qualifiers[102])
        else:
            return None
    except ValueError:
        return None

# extract_events(matchdict)

eventtypes = [
    (1, "pass"),
    (2, "offside pass"),
    (3, "take on"),
    (4, "foul"),
    (5, "out"),
    (6, "corner awarded"),
    (7, "tackle"),
    (8, "interception"),
    (9, "turnover"),
    (10, "save"),
    (11, "claim"),
    (12, "clearance"),
    (13, "miss"),
    (14, "post"),
    (15, "attempt saved"),
    (16, "goal"),
    (17, "card"),
    (18, "player off"),
    (19, "player on"),
    (20, "player retired"),
    (21, "player returns"),
    (22, "player becomes goalkeeper"),
    (23, "goalkeeper becomes player"),
    (24, "condition change"),
    (25, "official change"),
    (26, "unknown26"),
    (27, "start delay"),
    (28, "end delay"),
    (29, "unknown29"),
    (30, "end"),
    (31, "unknown31"),
    (32, "start"),
    (33, "unknown33"),
    (34, "team set up"),
    (35, "player changed position"),
    (36, "player changed jersey number"),
    (37, "collection end"),
    (38, "temp_goal"),
    (39, "temp_attempt"),
    (40, "formation change"),
    (41, "punch"),
    (42, "good skill"),
    (43, "deleted event"),
    (44, "aerial"),
    (45, "challenge"),
    (46, "unknown46"),
    (47, "rescinded card"),
    (48, "unknown46"),
    (49, "ball recovery"),
    (50, "dispossessed"),
    (51, "error"),
    (52, "keeper pick-up"),
    (53, "cross not claimed"),
    (54, "smother"),
    (55, "offside provoked"),
    (56, "shield ball opp"),
    (57, "foul throw in"),
    (58, "penalty faced"),
    (59, "keeper sweeper"),
    (60, "chance missed"),
    (61, "ball touch"),
    (62, "unknown62"),
    (63, "temp_save"),
    (64, "resume"),
    (65, "contentious referee decision"),
    (66, "possession data"),
    (67, "50/50"),
    (68, "referee drop ball"),
    (69, "failed to block"),
    (70, "injury time announcement"),
    (71, "coach setup"),
    (72, "caught offside"),
    (73, "other ball contact"),
    (74, "blocked pass"),
    (75, "delayed start"),
    (76, "early end"),
    (77, "player off pitch"),
]

##### Convert optah5 to spadlh5 

import config as spadlconfig

spadl_length = spadlconfig.field_length
spadl_width = spadlconfig.field_width

bodyparts = spadlconfig.bodyparts
results = spadlconfig.results
actiontypes = spadlconfig.actiontypes

def convert_to_spadl(optah5, spadlh5):

    with pd.HDFStore(optah5) as optastore, pd.HDFStore(spadlh5) as spadlstore:
        games = optastore["games"]
        spadlstore["games"] = games

        players = optastore["players"]
        players = players.rename(
            index=str,
            columns={
                "firstname": "first_name",
                "lastname": "last_name",
            },
        )
        spadlstore["players"] = players

        teams = optastore["teams"]
        spadlstore["teams"] = teams

        teamgames = optastore["teamgamestats"]
        teamgames = teamgames.rename(index=str)
        spadlstore["teamgames"] = teamgames

        playergames = optastore["playergamestats"]
        playergames = playergames.rename(
            index=str
        )
        spadlstore["playergames"] = playergames

        spadlstore["actiontypes"] = pd.DataFrame(
            list(enumerate(actiontypes)), columns=["type_id", "type_name"]
        )

        spadlstore["bodyparts"] = pd.DataFrame(
            list(enumerate(bodyparts)), columns=["bodypart_id", "bodypart_name"]
        )

        spadlstore["results"] = pd.DataFrame(
            list(enumerate(results)), columns=["result_id", "result_name"]
        )

        eventtypes = optastore["eventtypes"]
        for game in tqdm.tqdm(list(games.itertuples())):

            events = optastore[f"events/game_{game.game_id}"]
            events = (
                events.merge(eventtypes, on="type_id", how="left")
                .sort_values(["game_id", "period_id", "minute", "second"])
                .reset_index(drop=True)
            )
            actions = convert_to_actions(events, home_team_id=game.home_team_id)
            spadlstore[f"actions/game_{game.game_id}"] = actions

def convert_to_actions(events, home_team_id):
    actions = events
    actions["time_seconds"] = 60 * actions.minute + actions.second
    for col in ["start_x", "end_x"]:
        actions[col] = actions[col] / 100 * spadl_length
    for col in ["start_y", "end_y"]:
        actions[col] = actions[col] / 100 * spadl_width
    actions["bodypart_id"] = actions.qualifiers.apply(get_bodypart_id)
    actions["type_id"] = actions[["type_name", "outcome", "qualifiers"]].apply(
        get_type_id, axis=1
    )
    actions["result_id"] = actions[["type_name", "outcome", "qualifiers"]].apply(
        get_result_id, axis=1
    )

    actions = (
        actions[actions.type_id != actiontypes.index("non_action")]
        .sort_values(["game_id", "period_id", "time_seconds"])
        .reset_index(drop=True)
    )
    actions = fix_owngoal_coordinates(actions)
    actions = fix_direction_of_play(actions, home_team_id)
    actions = fix_clearances(actions)
    actions["action_id"] = range(len(actions))
    actions = add_dribbles(actions)
    return actions[
        [
            "game_id",
            "period_id",
            "time_seconds",
            "team_id",
            "player_id",
            "start_x",
            "start_y",
            "end_x",
            "end_y",
            "result_id",
            "bodypart_id",
            "type_id",
        ]
    ]


def get_bodypart_id(qualifiers):
    if 15 in qualifiers:
        b = "head"
    elif 21 in qualifiers:
        b = "other"
    else:
        b = "foot"
    return bodyparts.index(b)


def get_result_id(args):
    e, outcome, q = args
    if e == "offside pass":
        r = "offside"  # offside
    elif e == "foul":
        if outcome == 1:
            r = "success"
        else:
            r = "fail"
    elif e in ["attempt saved", "miss", "post"]:
        r = "fail"
    elif e == "goal":
        if 28 in q:
            r = "owngoal"  # own goal, x and y must be switched
        else:
            r = "success"
    elif e == "ball touch":
        if outcome == 1:
            r = "success"
        else:
            r = "fail"
    elif outcome == 1:
        r = "success"
    else:
        r = "fail"
    return results.index(r)


def get_type_id(args):
    eventname, outcome, q = args
    if eventname == "pass" or eventname == "offside pass":
        cross = 2 in q
        freekick = 5 in q
        corner = 6 in q
        throw_in = 107 in q
        if throw_in:
            a = "throw_in"
        elif freekick and cross:
            a = "freekick_crossed"
        elif freekick:
            a = "freekick_short"
        elif corner and cross:
            a = "corner_crossed"
        elif corner:
            a = "corner_short"
        elif cross:
            a = "cross"
        else:
            a = "pass"
    elif eventname == "take on":
        a = "take_on"
    elif eventname == "foul" and outcome == 0:
        a = "foul"
    elif eventname == "tackle":
        a = "tackle"
    elif eventname == "interception" or eventname == "blocked pass":
        a = "interception"
    elif eventname in ["attempt saved", "miss", "post", "goal"]:
        if 9 in q:
            a = "shot_penalty"
        elif 26 in q:
            a = "shot_freekick"
        else:
            a = "shot"
    elif eventname == "save":
        a = "keeper_save"
    elif eventname == "claim":
        a = "keeper_claim"
    elif eventname == "punch":
        a = "keeper_punch"
    elif eventname == "keeper pick-up":
        a = "keeper_pick_up"
    elif eventname == "clearance":
        a = "clearance"
    elif eventname == "ball touch" and outcome == 0:
        a = "bad_touch"
    elif eventname == 'ball recovery':
        a = "ball_recovery"
    else:
        a = "non_action"
    return actiontypes.index(a)

def fix_owngoal_coordinates(actions):
    owngoals_idx = (actions.result_id == results.index("owngoal")) & (
        actions.type_id == actiontypes.index("shot")
    )
    actions.loc[owngoals_idx, "end_x"] = (
        spadl_length - actions[owngoals_idx].end_x.values
    )
    actions.loc[owngoals_idx, "end_y"] = (
        spadl_width - actions[owngoals_idx].end_y.values
    )
    return actions


min_dribble_length = 3
max_dribble_length = 60
max_dribble_duration = 10


def add_dribbles(actions):
    next_actions = actions.shift(-1)

    same_team = actions.team_id == next_actions.team_id
    # not_clearance = actions.type_id != actiontypes.index("clearance")

    dx = actions.end_x - next_actions.start_x
    dy = actions.end_y - next_actions.start_y
    far_enough = dx ** 2 + dy ** 2 >= min_dribble_length ** 2
    not_too_far = dx ** 2 + dy ** 2 <= max_dribble_length ** 2

    dt = next_actions.time_seconds - actions.time_seconds
    same_phase = dt < max_dribble_duration
    same_period = actions.period_id == next_actions.period_id

    dribble_idx = same_team & far_enough & not_too_far & same_phase & same_period

    dribbles = pd.DataFrame()
    prev = actions[dribble_idx]
    nex = next_actions[dribble_idx]
    dribbles["game_id"] = nex.game_id
    dribbles["period_id"] = nex.period_id
    dribbles["action_id"] = prev.action_id + 0.1
    dribbles["time_seconds"] = (prev.time_seconds + nex.time_seconds) / 2
    dribbles["team_id"] = nex.team_id
    dribbles["player_id"] = nex.player_id
    dribbles["start_x"] = prev.end_x
    dribbles["start_y"] = prev.end_y
    dribbles["end_x"] = nex.start_x
    dribbles["end_y"] = nex.start_y
    dribbles["bodypart_id"] = bodyparts.index("foot")
    dribbles["type_id"] = actiontypes.index("dribble")
    dribbles["result_id"] = results.index("success")

    actions = pd.concat([actions, dribbles], ignore_index=True, sort=False)
    actions = actions.sort_values(["game_id", "period_id", "action_id"]).reset_index(
        drop=True
    )
    actions["action_id"] = range(len(actions))
    return actions


def fix_clearances(actions):
    next_actions = actions.shift(-1)
    next_actions[-1:] = actions[-1:]
    clearance_idx = actions.type_id == actiontypes.index("clearance")
    actions.loc[clearance_idx, "end_x"] = next_actions[clearance_idx].start_x.values
    actions.loc[clearance_idx, "end_y"] = next_actions[clearance_idx].start_y.values
    return actions


def fix_direction_of_play(actions, home_team_id):
    away_idx = (actions.team_id != home_team_id).values
    for col in ["start_x", "end_x"]:
        actions.loc[away_idx, col] = spadl_length - actions[away_idx][col].values
    for col in ["start_y", "end_y"]:
        actions.loc[away_idx, col] = spadl_width - actions[away_idx][col].values

    return actions