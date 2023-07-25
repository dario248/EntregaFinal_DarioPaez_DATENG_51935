CREATE TABLE IF NOT EXISTS analisis_economico (
    date VARCHAR(10),
    ipc decimal(7,2),
    tipo_cambio_bna_vendedor decimal(5,2),
    process_date VARCHAR(10) distkey
) SORTKEY(process_date, date);