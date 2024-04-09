class INTENT:
    CONFIRM_INTENT_YES = "music.ask_music.confirm_open_mp3_yes"
    CONFIRM_INTENT_NO = "music.ask_music.confirm_open_mp3_no"
    CONFIRM_INTENTS = [CONFIRM_INTENT_YES, CONFIRM_INTENT_NO]
    LYRICS_INTENT = "music.ask_music.ask_play_lyrics"
    LYRICS_INTENTS = ["music.ask_music.ask_play_lyrics",
                      CONFIRM_INTENT_YES, CONFIRM_INTENT_NO]
    NEARLY_MATCH_ZMP3_INTENT = "music.ask_music.ask_song_nearly_match_mp3_app"
    NEARLY_MATCH_INTENT = ["music.ask_music.ask_song_nearly_match",
                           NEARLY_MATCH_ZMP3_INTENT]
    ASK_SONG_INTENT = ["music.ask_music.ask_song",
                       "music.ask_music.ask_song_mp3_app"]
    ASK_MUSIC_GENRE_INTENT = [
        "music.ask_music.ask_play_music_genre", "music.ask_music.ask_play_music_genre_mp3_app"]
    ASK_MV_INTENT = "music.ask_music.ask_mv"
    ASK_TOP_SONG_INTENT = ["music.ask_music.ask_top_song"]
    ASK_LIEN_KHUC_INTENT = ["music.ask_music.ask_lien_khuc",
                            "music.ask_music.ask_lien_khuc_mp3_app"]
    ASK_PLAY_PERSONAL_PLAYLIST_UNCERTAIN = [
        "music.ask_music.ask_play_personal_playlist_uncertain"]
    ASK_PLAY_ZINGCHART = ["music.ask_music.ask_zingchart_song_mp3_app",
                       "music.ask_music.ask_zingchart_song"]
    ASK_RANDOM_SONGS = ["music.ask_music.ask_resume_or_random_song",
                       "music.ask_music.ask_random_song"]
    SPECIAL_INTENTS = ["music.ask_music.ask_top_song",
                       "music.ask_music.ask_play_personal_playlist_certain",
                       "music.ask_music.ask_favorite_song"] \
        + ASK_PLAY_PERSONAL_PLAYLIST_UNCERTAIN \
        + ASK_PLAY_ZINGCHART \
        + ASK_RANDOM_SONGS
    FALLBACK_TO_SEARCH_MP3 = "fallback_to_search_mp3_app"
    ASK_MUSIC_RADIO_INTENT = "music.ask_music.ask_radio"
    MAPPED_INTENT_PLAY = "mapped_intent.map"
    MAPPED_INTENT_FALLBACK = "mapped_intent.fallback"
    ASK_PLAY_PRESET_MUSIC_PLAYLIST_SET = "ask_play_preset_music_playlist_set"
    ASK_SONG_N_TIMES = "music.ask_music.ask_song_n_times"
    MAPPED_INTENTS = [MAPPED_INTENT_FALLBACK, MAPPED_INTENT_PLAY]
    ASK_PLAY_MUSIC_INTENTS = LYRICS_INTENTS + NEARLY_MATCH_INTENT \
        + ASK_SONG_INTENT + ASK_MUSIC_GENRE_INTENT \
        + [ASK_MV_INTENT] + ASK_LIEN_KHUC_INTENT \
        + [ASK_MUSIC_RADIO_INTENT] + ASK_TOP_SONG_INTENT + SPECIAL_INTENTS \
        + [MAPPED_INTENT_PLAY] \
        + [ASK_PLAY_PRESET_MUSIC_PLAYLIST_SET] + [ASK_SONG_N_TIMES]
    ASK_ZMP3_MUSIC_INTENTS = LYRICS_INTENTS + ASK_TOP_SONG_INTENT + ASK_RANDOM_SONGS \
        + ASK_LIEN_KHUC_INTENT + ASK_PLAY_ZINGCHART + ASK_MUSIC_GENRE_INTENT \
        + ASK_SONG_INTENT + ASK_PLAY_PERSONAL_PLAYLIST_UNCERTAIN \
        + [NEARLY_MATCH_ZMP3_INTENT, MAPPED_INTENT_PLAY, FALLBACK_TO_SEARCH_MP3,
           ASK_PLAY_PRESET_MUSIC_PLAYLIST_SET, ASK_MUSIC_RADIO_INTENT, ASK_SONG_N_TIMES]


class APP:
    APP_ZING = ["APP_ZINGMP3"]
    APP_ZING_IOS = ["APP_ZINGMP3_IOS"]
    APP_ZINGS = ["APP_ZINGMP3", "APP_ZINGMP3_IOS"]
    APP_KIKI = ["APP_KIKI"]
    APP_KIKIS = ["APP_KIKI", "APP_KIKI_IOS"]
    APP_CARS = "APP_KIKI_ANDROID_*"  # but APP_KIKI_ANDROID_TV
    APP_TV = ["APP_KIKI_ANDROID_TV"]
    ALL_APPS = ["ALL"]


class LOGMODE:
    GUIDE_TOP_SONG_LOG_MODE = "mode_guide_top_song_name"
    GUIDE_ASK_SONG_LOG_MODE = ["mode_skip_guiding_top_song_name",
                               GUIDE_TOP_SONG_LOG_MODE,
                               "ml_guide_log_mode"]
    UNOFFICIAL_SONG_LOG_MODE = "mode_unofficial_song_from_elastic_search"
    UNCERTAIN_OFFICIAL_SONG_LOG_MODE = "mode_uncertain_official_song_from_elastic_search"
    GENGE_KMDB = ["kmdb_play_genre_by_song_link", "kmdb_genre_link"]
    PREPARED_INTENT = "mode_prepared_intent"
    DIFF_HIGHLIGHT = "highlight_api_difference_play"
    BLOCK_CONTENT = "mode_query_log_blocked"
    QUERY_LOG_REJECTED = "mode_query_log_rejected"
    FALLBACK_PLAY_QUERY = "mode_fallback_query_log_play"
    MAPPED_KEYWORD_PLAY_LINK = "mapped_keyword_play_link"
    MAPPED_KEYWORD_FALLBACK = "mapped_keyword_fallback"
    MAPPED_KEYWORD_FALLBACK_TO_SEARCH = "mapped_keyword_fallback_to_search"
    MAPPED_KEYWORD_PLAY_LINK_FAILED = "mapped_keyword_play_link_failed"
    MAPPED_KEYWORD_PLAY_LINK_BY_BLOCKED_KEYWORD = "mapped_keyword_play_link_failed_by_blocked_keyword"
    MAPPED_KEYWORD_PLAY_LINK_FAILED_BY_QUERY_LOG_REJECTED = "mapped_keyword_play_link_failed_by_query_log_rejected"
    MAPPED_KEYWORDS = [
        MAPPED_KEYWORD_PLAY_LINK,
        MAPPED_KEYWORD_PLAY_LINK_FAILED,
        MAPPED_KEYWORD_PLAY_LINK_BY_BLOCKED_KEYWORD,
        MAPPED_KEYWORD_PLAY_LINK_FAILED_BY_QUERY_LOG_REJECTED,
        MAPPED_KEYWORD_FALLBACK,
        MAPPED_KEYWORD_FALLBACK_TO_SEARCH]
    KMDB_MAPPED_MODE = [PREPARED_INTENT,
                        "force_open_kmdb",
                        "kmdb_song_alias",
                        "kmdb_artist_link", "kmdb_play_artist_by_song_link",
                        "kmdb_song_link_play_failed", "kmdb_genre_link_play_failed"] \
        + GENGE_KMDB + MAPPED_KEYWORDS

    # ZingMP3 search song mode
    ZMP3SS_SINGLE_BEST = "zingmp3_search_song.matching_a_single_best"
    ZMP3SS_RECENT_ARTISTS = "zingmp3_search_song.matching_on_recently_requested_artists"
    ZMP3SS_ZMP3_ORDER = "zingmp3_search_song.matching_by_zmp3_order"
    ZMP3SS_RECENT_VERSION = "zingmp3_search_song.matching_on_recently_version"
    ZMP3SS_ARTIST_FILTED = "zingmp3_search_song.matching_artist_filted"
    ZMP3SS_EMPTY_RESULT = "zingmp3_search_song_empty_result_mode"
    ZINGMP3_SEARCH_SONG = [ZMP3SS_SINGLE_BEST,
                           ZMP3SS_RECENT_ARTISTS,
                           ZMP3SS_ZMP3_ORDER,
                           ZMP3SS_RECENT_VERSION,
                           ZMP3SS_ARTIST_FILTED,
                           ZMP3SS_EMPTY_RESULT,]

    # ZingMP3 search album mode
    ZMP3SA_SINGLE_BEST = "zingmp3_search_album.matching_a_single_best"
    ZMP3SA_MULTI_ALBUMS = "zingmp3_search_album.has_multi_albums_with_the_same_highest_score"
    ZMP3SA_ZMP3_ORDER = "zingmp3_search_album.matching_by_zmp3_order"
    ZINGMP3_SEARCH_ALBUM = [ZMP3SA_SINGLE_BEST,
                            ZMP3SA_MULTI_ALBUMS,
                            ZMP3SA_ZMP3_ORDER,]
    # ZingMP3 search song artist matching
    ZMP3SSAM_MATCHING_PERFECTLY = "zingmp3_search_song_artist_matching.matching_perfectly"
    ZMP3SSAM_ARTIST_CONTAINS = "zingmp3_search_song_artist_matching.containing"
    ZMP3SSAM_SONG_NAME_CONTAINS_ARTIST = "zingmp3_search_song_artist_matching.song_name_contains_artist"
    ZMP3SSAM_ANOTHER_ARTIST = "zingmp3_search_song_artist_matching.another_artist_performs_the_same_song"
    ZINGMP3_SEARCH_SONG_ARTIST_MATCHING = [ZMP3SSAM_MATCHING_PERFECTLY,
                                           ZMP3SSAM_ARTIST_CONTAINS,
                                           ZMP3SSAM_SONG_NAME_CONTAINS_ARTIST,
                                           ZMP3SSAM_ANOTHER_ARTIST]
    # medley
    MEDLEY_FROM_SONG = "medley_comes_from_song"
    MEDLEY_FROM_ALBUM = "medley_comes_from_album"
    WHERE_MEDLEY_FROM = [MEDLEY_FROM_SONG, MEDLEY_FROM_ALBUM]
    ALBUM_NAME_CONTAINS_MEDLEY = "album_name_contains_medley"
    ALBUM_NAME_NOT_CONTAINS_MEDLEY = "album_name_does_not_contain_medley"
    # album
    ALBUM_FROM_OFFICIAL = "album_comes_from_official"
    ALBUM_FROM_UNOFFICIAL = "album_comes_from_unofficial"
    WHERE_ALBUM_FROM = [ALBUM_FROM_OFFICIAL, ALBUM_FROM_UNOFFICIAL]
    # personalize
    NEARLY_MATCHING_FROM_ES = "nearly_matching_result_from_es"
    NEARLY_MATCHING_FROM_RECENTS = "nearly_matching_result_from_recents"
    NEARLY_MATCHING_FROM_BOTH = "nearly_matching_result_from_both_recents_and_es"
    WHERE_NEARLY_MATCHING_FROM = [
        NEARLY_MATCHING_FROM_ES, NEARLY_MATCHING_FROM_RECENTS, NEARLY_MATCHING_FROM_BOTH]
    # lyrics
    LYRICS_FROM_ZMP3_WITH_MUSIC_TAGS = "lyrics_from_zmp3_with_music_tags"
    LYRICS_FROM_NCT = "lyrics_from_nct"
    LYRICS_FROM_ZMP3 = "lyrics_from_zmp3"
    LYRICS_FROM_ZMP3_BUT_SEARCHED_BY_NCT = "lyrics_from_zmp3_but_searched_by_nct"
    WHERE_LYRICS_FROM = [LYRICS_FROM_ZMP3_WITH_MUSIC_TAGS, LYRICS_FROM_NCT,
                         LYRICS_FROM_ZMP3, LYRICS_FROM_ZMP3_BUT_SEARCHED_BY_NCT]
    LYRICS_REASK_SKIP_BY_HIGH_CONFIRM = "lyrics_reask_skip_by_high_confirm"
    LYRICS_REASK_SKIP_BY_HIGH_UAR = "lyrics_reask_skip_by_high_uar"
    LYRICS_REASK_BY_LOW_UAR = "lyrics_reask_by_low_uar"

    # other
    RECOVER_SONG_NAME_SEARCH = "recover_song_name_search"

class TRACE:
    REASK = "re_ask"


class PATTERN:
    REASK_PATTERN = r"Mình tìm thấy bài hát (.+?). Bạn có muốn mở bài này không?"
    LOGMODE_PATTERN = r"\"mode\":\"\[([^\"]+?)\]\""
    
