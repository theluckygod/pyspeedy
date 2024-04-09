-- DROP TABLE IF EXISTS monitor_by_id;
CREATE TABLE IF NOT EXISTS monitor_by_id(
    date datetime NOT NULL, 
    id char(20) NOT NULL,
    type char(10) NOT NULL,
    tracking_type char(50),
    name text,

    uar float,
    frequency int,
    duration_num int,
    accepted_num int,

    intent char(50) NOT NULL,

    PRIMARY KEY (date, id, type, intent)
);

