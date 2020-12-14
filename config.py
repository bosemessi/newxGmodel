import pandas as pd  # type: ignore

from typing import List

field_length: float = 100.0  
field_width: float = 100.0 

bodyparts: List[str] = ["foot", "head", "other"]
results: List[str] = [
    "fail",
    "success",
    "offside",
    "owngoal",
    "yellow_card",
    "red_card",
]
actiontypes: List[str] = [
    "pass",
    "cross",
    "throw_in",
    "freekick_crossed",
    "freekick_short",
    "corner_crossed",
    "corner_short",
    "take_on",
    "foul",
    "tackle",
    "interception",
    "shot",
    "shot_penalty",
    "shot_freekick",
    "keeper_save",
    "keeper_claim",
    "keeper_punch",
    "keeper_pick_up",
    "clearance",
    "bad_touch",
    "non_action",
    "dribble",
    "goalkick",
    "ball_recovery"
]


def actiontypes_df() -> pd.DataFrame:
    return pd.DataFrame(list(enumerate(actiontypes)), columns=["type_id", "type_name"])


def results_df() -> pd.DataFrame:
    return pd.DataFrame(list(enumerate(results)), columns=["result_id", "result_name"])


def bodyparts_df() -> pd.DataFrame:
    return pd.DataFrame(
        list(enumerate(bodyparts)), columns=["bodypart_id", "bodypart_name"]
    )