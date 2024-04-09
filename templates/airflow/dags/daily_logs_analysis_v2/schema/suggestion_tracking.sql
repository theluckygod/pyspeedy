-- DROP TABLE IF EXISTS suggestion_tracking;
CREATE TABLE IF NOT EXISTS suggestion_tracking(
    date date NOT NULL,
    device_type VARCHAR(256) NOT NULL,
    backend VARCHAR(128) NOT NULL,
    sdk_version INT NOT NULL,

    suggestion_type VARCHAR(128),
    num_no_result INT,
    num_suggested INT,
    num_direct_play INT,
    num_dur_direct_play INT,
    num_happy_direct_play INT,
    num_indirect_play INT,
    num_dur_indirect_play INT,
    num_happy_indirect_play INT,

    PRIMARY KEY (date, device_type, backend, sdk_version, suggestion_type)
);
