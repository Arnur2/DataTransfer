SELECT dblink_disconnect('myconn');

DO $$
DECLARE 
    batch_size     INT := 1000000;
    offset_rows    INT := 0;
    rows_fetched   INT;
    current_batch  INT := 1;
BEGIN
    -- Подключаемся к source_db
    PERFORM dblink_connect('myconn',
     'host=localhost dbname=source_db user=postgres password=1');

    -- Переносим батчи
    LOOP
        BEGIN
            EXECUTE format($sql$
                INSERT INTO recommendations (user_id, item_id, recommendation_date)
                SELECT user_id, item_id, recommendation_date
                FROM dblink('myconn', %L)
                AS t(user_id INT, item_id INT, recommendation_date DATE);
            $sql$,
            format(
                'SELECT user_id, item_id, recommendation_date
                 FROM recommendations
                 WHERE recommendation_date = CURRENT_DATE - INTERVAL ''1 day''
                 ORDER BY user_id
                 OFFSET %s LIMIT %s',
                 offset_rows, batch_size
            ));

            GET DIAGNOSTICS rows_fetched = ROW_COUNT;

            RAISE NOTICE '✅ Batch % completed. Rows inserted: %', current_batch, rows_fetched;

        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '❌ Batch % FAILED at offset %: %', current_batch, offset_rows, SQLERRM;
            -- OPTIONAL: exit or continue to next batch
            EXIT;
        END;

        EXIT WHEN rows_fetched < batch_size;
        offset_rows := offset_rows + batch_size;
        current_batch := current_batch + 1;
    END LOOP;

    PERFORM dblink_disconnect('myconn');
END $$;
