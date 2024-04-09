-- DROP TABLE IF EXISTS logics_tracking_v2;
CREATE TABLE IF NOT EXISTS logics_tracking_v2(
    date date NOT NULL, 
    device_type varchar(256) NOT NULL,
    backend char(100) NOT NULL,
    logic char(100) NOT NULL,

    num int,
    num_responded int,
    num_accepted int,

    description varchar(256),
    state char(4), 
    PRIMARY KEY (device_type, backend, date, logic)
);

