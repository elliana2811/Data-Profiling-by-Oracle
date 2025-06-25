CREATE TABLE tb_data_profiling (
    myid NUMBER PRIMARY KEY,
    my_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255),
    index_name VARCHAR(255) NOT NULL,
    index_value VARCHAR(255) NOT NULL,
    remarks VARCHAR(400)
);

CREATE TABLE tb_log (
    myid NUMBER PRIMARY KEY,
    my_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    phase VARCHAR(50) NOT NULL,
    procedure_name VARCHAR(255) NOT NULL,
    error_code VARCHAR(50),
    error_message VARCHAR(4000)
);

CREATE SEQUENCE sq_data_profiling
    START WITH 1
    INCREMENT BY 1;

CREATE SEQUENCE sq_log
    START WITH 1
    INCREMENT BY 1;

CREATE OR REPLACE PROCEDURE pr_log_message (
    p_phase IN VARCHAR2,
    p_procedure_name IN VARCHAR2,
    p_error_code IN VARCHAR2 DEFAULT NULL,
    p_error_message IN VARCHAR2 DEFAULT NULL
) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO tb_log (myid, my_timestamp, phase, procedure_name, error_code, error_message) 
    VALUES(sq_log.nextval, SYSTIMESTAMP, p_phase, p_procedure_name, p_error_code, p_error_message);
    COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE pr_data_profiling (
    p_table_name IN VARCHAR2
) IS
    CURSOR col_cur IS
        SELECT column_name
        FROM user_tab_columns
        WHERE table_name = UPPER(p_table_name); 
    
    TYPE ref_cursor IS REF CURSOR;
    cur               ref_cursor;

    TYPE num_tab IS TABLE OF VARCHAR2(128);
    nums            num_tab;

    l_sql               VARCHAR2(4000); 
    l_total_rows        NUMBER;
    l_total_cols        NUMBER;
    l_categoric         NUMBER;
    l_numeric           NUMBER;
    l_date              NUMBER;
    l_duplicate_rows    NUMBER;  
    l_nulls             NUMBER; 
    l_distinct          VARCHAR2(400);
    l_distinct_cnt      NUMBER;
    l_col_name          VARCHAR2(400);
    l_min_value         VARCHAR2(100);
    l_max_value         VARCHAR2(100);
    l_mean              NUMBER;
    l_median            NUMBER;
    l_stddev            NUMBER;
    l_p10               NUMBER;
    l_p20               NUMBER;
    l_p30               NUMBER;
    l_p40               NUMBER;
    l_p50               NUMBER;
    l_p60               NUMBER;
    l_p70               NUMBER;
    l_p80               NUMBER;
    l_p90               NUMBER;
    l_corr              NUMBER;
    l_freq_sql          NUMBER;
    l_top_freq_cnt      NUMBER;
    l_top_val           VARCHAR2(400);
    l_top_cnt           NUMBER;
    l_min               TIMESTAMP;
    l_max               TIMESTAMP;
    l_avg               TIMESTAMP;
BEGIN 
    pr_log_message('begin', 'pr_data_profiling');
    
    --- Total rows ---
    l_sql := 'SELECT COUNT(*) FROM ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name);
    EXECUTE IMMEDIATE l_sql INTO l_total_rows;

    INSERT INTO tb_data_profiling (myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
    VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, NULL, 'Total Rows', l_total_rows, 'Total number of rows in the table');

    --- Total columns ---
    l_sql := 'SELECT COUNT(*) FROM user_tab_columns WHERE table_name = :tbl';
    EXECUTE IMMEDIATE l_sql INTO l_total_cols USING UPPER(p_table_name);

    INSERT INTO tb_data_profiling (myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
    VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, NULL, 'Total Columns', l_total_cols, 'Total number of columns in the table');

    --- Total categorical ---
    l_sql := 'SELECT COUNT(*) FROM user_tab_columns WHERE table_name = :tbl AND data_type = ''VARCHAR2''';
    EXECUTE IMMEDIATE l_sql INTO l_categoric USING UPPER(p_table_name); 

    INSERT INTO tb_data_profiling (myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
    VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, NULL, 'Total Categorical Columns', l_categoric, 'Total number of categorical columns in the table');

    --- Total numerical ---
    l_sql := 'SELECT COUNT(*) FROM user_tab_columns WHERE table_name = :tbl AND data_type = ''NUMBER''';
    EXECUTE IMMEDIATE l_sql INTO l_numeric USING UPPER(p_table_name);

    INSERT INTO tb_data_profiling (myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
    VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, NULL, 'Total Numerical Columns', l_numeric, 'Total number of numerical columns in the table');

    --- Total Date ---
    l_sql := 'SELECT COUNT(*) FROM user_tab_columns WHERE table_name =: tbl AND data_type IN (''DATE'', ''TIMESTAMP'')';
    EXECUTE IMMEDIATE l_sql INTO l_date USING UPPER(p_table_name);

    INSERT INTO tb_data_profiling (myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
    VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, NULL, 'Total Date', l_date, 'Total number of date columns in the table');

    --- Duplicated Rows ---
    l_sql := 'SELECT COUNT(*) FROM (SELECT COUNT(*) AS cnt FROM ' 
        || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name) || ' GROUP BY ';
    FOR col_rec IN(
        SELECT column_name
        FROM user_tab_columns
        WHERE table_name = UPPER(p_table_name) 
        ORDER BY column_id
    ) LOOP
        l_col_name := col_rec.column_name;
        l_sql := l_sql || DBMS_ASSERT.SIMPLE_SQL_NAME(l_col_name) || ', ';
    END LOOP;

    l_sql := RTRIM(l_sql, ', ') || ' HAVING COUNT(*) > 1)';
    EXECUTE IMMEDIATE l_sql INTO l_duplicate_rows;

    INSERT INTO tb_data_profiling (myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
    VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, NULL, 'Total Duplicated Rows', l_duplicate_rows, 'Total number of duplicated rows in the table');

    --- Missing values ---
    FOR col_rec IN col_cur LOOP
        l_sql := 'SELECT COUNT(*) AS cnt FROM ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name) ||' WHERE ' || col_rec.column_name || ' IS NULL';
        EXECUTE IMMEDIATE l_sql INTO l_nulls;
        IF l_nulls > 0 THEN
            INSERT INTO tb_data_profiling (myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Missing value of ' ||col_rec.column_name, l_nulls || ' (' || round((l_nulls * 100)/GREATEST(l_total_rows, 1), 2) || '%)', 'Number of null values in column ' || col_rec.column_name);
        END IF;
    END LOOP; 

    --- Distinct ---
    FOR col_rec IN col_cur LOOP
        l_sql := 'SELECT 
                        COUNT(DISTINCT "' || col_rec.column_name || '")
                    FROM ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name);
        EXECUTE IMMEDIATE l_sql INTO l_distinct_cnt;

        INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
        VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Distinct Count of ' ||col_rec.column_name, l_distinct_cnt || '/' || l_total_rows || ' (' || round((l_distinct_cnt * 100) / GREATEST(l_total_rows,1),5) || '%)', 'The Distinct Count of column ' || col_rec.column_name);

    END LOOP;
    
    --- Numerical profiling ---
    FOR col_rec IN (
        SELECT column_name
        FROM user_tab_columns
        WHERE table_name = UPPER(p_table_name)
            AND data_type = 'NUMBER'
    ) LOOP
        l_sql := '
            SELECT MIN("' || col_rec.column_name || '"),
                   MAX("' || col_rec.column_name || '"),
                   AVG("' || col_rec.column_name || '"),
                   MEDIAN("' || col_rec.column_name || '"),
                   ROUND(STDDEV("' || col_rec.column_name || '"), 2)
            FROM ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name);

        EXECUTE IMMEDIATE l_sql INTO l_min_value, l_max_value, l_mean, l_median, l_stddev;
        INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
        VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Min of ' ||col_rec.column_name, l_min_value, 'Minimum Value of ' || col_rec.column_name);

        INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
        VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Max of ' ||col_rec.column_name, l_max_value, 'Maximum Value of ' || col_rec.column_name);

        INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
        VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Mean of ' ||col_rec.column_name, l_mean, 'Mean Value of ' || col_rec.column_name);

        INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
        VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Median of ' ||col_rec.column_name, l_median, 'Median Value of ' || col_rec.column_name);

        INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
        VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Stddev of ' ||col_rec.column_name, l_stddev, 'Standard Deviation Value of ' || col_rec.column_name);     

    END LOOP;

    ---- Percentile ---- 
    FOR col_rec IN (
        SELECT column_name
        FROM user_tab_columns
        WHERE table_name = UPPER(p_table_name)
            AND data_type = 'NUMBER'
    ) LOOP 
        l_col_name := col_rec.column_name;
        l_sql := 'SELECT COUNT(DISTINCT ' || l_col_name || ') FROM ' || p_table_name;
        EXECUTE IMMEDIATE l_sql INTO l_distinct_cnt;
        IF l_distinct_cnt < 500 THEN
            l_sql := 'SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY "' || col_rec.column_name || '"),
                        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY "' || col_rec.column_name || '"),
                        PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY "' || col_rec.column_name || '"),
                        PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY "' || col_rec.column_name || '"),
                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "' || col_rec.column_name || '"),
                        PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY "' || col_rec.column_name || '"),
                        PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY "' || col_rec.column_name || '"),
                        PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY "' || col_rec.column_name || '"),
                        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY "' || col_rec.column_name || '") 
                        FROM ' ||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name);
            EXECUTE IMMEDIATE l_sql INTO l_p10, l_p20, l_p30, l_p40, l_p50, l_p60, l_p70, l_p80, l_p90;
        
            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, '10th Percentile of ' ||col_rec.column_name, l_p10, '10th Percentile Value of ' || col_rec.column_name);

            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, '20th Percentile of ' ||col_rec.column_name, l_p20, '20th Percentile Value of ' || col_rec.column_name);

            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, '30th Percentile of ' ||col_rec.column_name, l_p30, '30th Percentile Value of ' || col_rec.column_name);

            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, '40th Percentile of ' ||col_rec.column_name, l_p40, '40th Percentile Value of ' || col_rec.column_name);

            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, '50th Percentile of ' ||col_rec.column_name, l_p50, '50th Percentile Value of ' || col_rec.column_name);

            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, '60th Percentile of ' ||col_rec.column_name, l_p60, '60th Percentile Value of ' || col_rec.column_name);

            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, '70th Percentile of ' ||col_rec.column_name, l_p70, '70th Percentile Value of ' || col_rec.column_name);

            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, '80th Percentile of ' ||col_rec.column_name, l_p80, '80th Percentile Value of ' || col_rec.column_name);

            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, '90th Percentile of ' ||col_rec.column_name, l_p90, '90th Percentile Value of ' || col_rec.column_name);
        END IF;

    END LOOP; 

    --- Correlation ---
    SELECT column_name
    BULK COLLECT INTO nums
    FROM user_tab_columns
    WHERE table_name = UPPER(p_table_name)
        AND data_type = 'NUMBER';
    
    FOR i IN 1 .. nums.COUNT -1 LOOP
        FOR j IN i+1 .. nums.COUNT LOOP
            l_sql := 'with ranked_data AS (
                        SELECT PERCENT_RANK() OVER (ORDER BY "' || nums(i) || '") AS col1_pct,
                                PERCENT_RANK() OVER (ORDER BY "' || nums(j) || '") AS col2_pct
                        FROM "' ||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name) || '")
                        SELECT CORR(col1_pct, col2_pct)
                        FROM ranked_data';
            EXECUTE IMMEDIATE l_sql INTO l_corr;
            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(sq_data_profiling.nextval, SYSTIMESTAMP, p_table_name, nums(i) || '/ ' || nums(j), 'Correlation of Percentile Rank', ROUND(l_corr, 7), 'Correlation of Percentile Rank between ' || nums(i) || 'and ' ||nums(j));
        END LOOP;
    END LOOP;
            
    --- Frequency of numerical columns
    FOR col_rec IN (
        SELECT column_name
        FROM user_tab_columns
        WHERE table_name = UPPER(p_table_name)
            AND data_type = 'NUMBER'
    ) LOOP
        l_col_name := col_rec.column_name;
        
        l_sql := 'SELECT COUNT(DISTINCT "' || l_col_name || '") FROM ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name);
        EXECUTE IMMEDIATE l_sql INTO l_distinct_cnt;

        IF l_distinct_cnt > 200 THEN
            l_sql := '
                SELECT "' || l_col_name || '", COUNT(*) AS cnt
                FROM ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name) || '
                WHERE "' || l_col_name || '" IS NOT NULL
                GROUP BY "' || l_col_name || '"
                ORDER BY cnt DESC FETCH FIRST 5 ROWS ONLY';
        ELSE
            l_sql := '
                SELECT "' || l_col_name || '", COUNT(*) AS cnt
                FROM ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name) || '
                WHERE "' || l_col_name || '" IS NOT NULL
                GROUP BY "' || l_col_name || '"
                ORDER BY cnt DESC';
        END IF;

        OPEN cur FOR l_sql;
        LOOP
            FETCH cur INTO l_freq_sql, l_top_freq_cnt;
            EXIT WHEN cur%NOTFOUND;

            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(SQ_DATA_PROFILING.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Top value of ' || col_rec.column_name , l_freq_sql || 
            ' (' || round((l_top_freq_cnt * 100)/GREATEST(l_total_rows, 1), 5) || '%)', 'This is the top value and its percentage in the column ' || col_rec.column_name);
        END LOOP;
        CLOSE cur;
    END LOOP;

    --- Categorical profiling --- 
    FOR col_rec IN (
        SELECT column_name 
        FROM user_tab_columns
        WHERE table_name = UPPER(p_table_name)
            AND data_type = 'VARCHAR2'
    ) LOOP 
        l_col_name := col_rec.column_name;

        l_sql := 'SELECT COUNT(DISTINCT ' || l_col_name || ') FROM ' || p_table_name;
        EXECUTE IMMEDIATE l_sql INTO l_distinct_cnt;

        IF l_distinct_cnt > 200 THEN
            l_sql := '
                SELECT ' || l_col_name || ', COUNT(*) AS cnt
                FROM ' || p_table_name || '
                WHERE ' || l_col_name || ' IS NOT NULL
                GROUP BY ' || l_col_name || '
                ORDER BY cnt DESC FETCH FIRST 5 ROWS ONLY';
        ELSE
            l_sql := '
                SELECT ' || l_col_name || ', COUNT(*) AS cnt
                FROM ' || p_table_name || '
                WHERE ' || l_col_name || ' IS NOT NULL
                GROUP BY ' || l_col_name || '
                ORDER BY cnt DESC';
        END IF;

        OPEN cur FOR l_sql;
        LOOP
            FETCH cur INTO l_top_val, l_top_cnt;
            EXIT WHEN cur%NOTFOUND;

            INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
            VALUES(SQ_DATA_PROFILING.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Top value of ' || col_rec.column_name, l_top_val || 
            ' (' || round((l_top_cnt * 100)/GREATEST(l_total_rows, 1), 5) || '%)', 'This is the value that have the highest frequency of column ' || col_rec.column_name);
        END LOOP;
        CLOSE cur;
    END LOOP; 

    --- Date profiling ---
    FOR col_rec IN (
        SELECT column_name
        FROM user_tab_columns 
        WHERE table_name = UPPER(p_table_name)
        AND data_type IN ('DATE', 'TIMESTAMP')
    ) LOOP
        l_sql := 'SELECT '
       || 'MIN("' || col_rec.column_name || '"), '
       || 'MAX("' || col_rec.column_name  || '"), '
       || 'TO_DATE(ROUND(AVG(TO_NUMBER(TO_CHAR("' || col_rec.column_name || '", ''J'')))), ''J'') '
       || 'FROM ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name);

        EXECUTE IMMEDIATE l_sql INTO l_min, l_max, l_avg;
        INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
        VALUES(SQ_DATA_PROFILING.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Min Date of ' ||col_rec.column_name , l_min, 'Minimum Date value of column ' || col_rec.column_name);

        INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
        VALUES(SQ_DATA_PROFILING.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Max Date of ' ||col_rec.column_name, l_max, 'Maximum Date value of column ' || col_rec.column_name);

        INSERT INTO tb_data_profiling(myid, my_timestamp, table_name, column_name, index_name, index_value, remarks)
        VALUES(SQ_DATA_PROFILING.nextval, SYSTIMESTAMP, p_table_name, col_rec.column_name, 'Average Date of ' ||col_rec.column_name, l_avg, 'Average Date value of column ' || col_rec.column_name);
    END LOOP;

    pr_log_message('end', 'pr_data_profiling');

EXCEPTION
    WHEN OTHERS THEN
        pr_log_message('error', 'pr_data_profiling', SQLCODE, SQLERRM);
        RAISE;
END;
/

SET SERVEROUTPUT ON; 
--- Anonymous Block for Commit and Rollback ---
DECLARE
    TYPE t_table_list IS TABLE OF VARCHAR2(50);
    v_tables t_table_list := t_table_list('HD_DUNGTHU_INFO', 'HD_INFO', 'KH_INFO');
BEGIN
    FOR i IN 1 .. v_tables.COUNT LOOP
        pr_data_profiling(v_tables(i));
    END LOOP;
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE(SQLERRM);
END;
/

SELECT * FROM tb_data_profiling;
SELECT * FROM tb_log;
