-- DROP TABLE IF EXISTS kiki_user_acceptance_stats_v4;
CREATE TABLE IF NOT EXISTS kiki_user_acceptance_stats_v4(
    date date NOT NULL, 
    total int,
    count_happy int,
    count_short_songs int,
    PRIMARY KEY (date)
);

