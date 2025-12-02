CREATE TABLE routing.{table}_{ptn}
    PARTITION OF routing.{table}
    FOR VALUES IN ('{ptn}');