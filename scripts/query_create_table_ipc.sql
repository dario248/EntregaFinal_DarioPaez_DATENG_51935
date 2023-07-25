CREATE TABLE IF NOT EXISTS ipc_nacional (
    date VARCHAR(10),
    ipc decimal(7,2),
    process_date VARCHAR(10) distkey
) SORTKEY(process_date, date);