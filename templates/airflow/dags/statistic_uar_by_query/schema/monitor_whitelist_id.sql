-- DROP TABLE IF EXISTS monitor_whitelist_id;
CREATE TABLE IF NOT EXISTS monitor_whitelist_id(
    id char(20) NOT NULL,
    type char(10) DEFAULT "song" NOT NULL,
    tracking_type char(50),
    created_date datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id, type)
);
