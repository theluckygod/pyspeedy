import common_package.kiki_logs_constants as kiki_logs_constants

_USER_DIF_THRESH = {
    "song": 3,
    "album": 2,
    "video": 3,
}
_QUERIES_THRESH = {
    "song": 20,
    "album": 7,
    "video": 20,
}
_ACCEPTED_QUERIES_THRESH = {
    "song": 0.75,
    "album": 0.65,
    "video": 0.75,
}
_REJECTED_QUERIES_THRESH = {
    "song": 0.508,
    "album": 0.378,
    "video": 0.508,
}
_ACCEPTED_SCORE_THRESH = {
    "song": 1.5,
    "album": 1.1,
    "video": 1.5,
}
_REJECTED_SCORE_THRESH = {
    "song": -2,
    "album": -1.1,
    "video": -2,
}

CONFIRM_OPEN_MP3_INTENTS = [
    kiki_logs_constants.INTENT.CONFIRM_INTENT_NO,
    kiki_logs_constants.INTENT.CONFIRM_INTENT_YES,
]
INTERESTING_INTENTS_WHICH_NO_DURATION = [kiki_logs_constants.INTENT.CONFIRM_INTENT_NO]
