CREATE TABLE IF NOT EXISTS tipo_de_cambio (
    date VARCHAR(10),
    tipo_cambio_bna_vendedor decimal(5,2),
    process_date VARCHAR(10) distkey
) SORTKEY(process_date, date);