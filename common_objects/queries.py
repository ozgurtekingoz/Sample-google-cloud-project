# region Load Data Queries

TLC_2014_FILTERED_QUERY = """
                    CREATE TABLE {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}
                    PARTITION BY
                      DATE_TRUNC(pickup_datetime, DAY)
                    OPTIONS (require_partition_filter = TRUE)
                    AS
                    SELECT * 
                        FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014`
                     WHERE DATE(pickup_datetime) >= DATE('{START_DATE}')
                     AND DATE(pickup_datetime) <= DATE('{END_DATE}')"""

TLC_2014_EXPORT_QUERY = """SELECT * 
                        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                     WHERE DATE(pickup_datetime) = DATE('{REPORT_DATE}')"""

# endregion

# region Clean Data Queries

CLEAN_INVALID_RATE_CODES = """DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` 
                            WHERE rate_code NOT IN (1,2,3,4,5,6)
                                AND DATE(pickup_datetime) >= DATE('{START_DATE}')
                                AND DATE(pickup_datetime) <= DATE('{END_DATE}')"""

CLEAN_ZERO_PASSENGERS = """DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` 
                            WHERE passenger_count <=0
                                AND DATE(pickup_datetime) >= DATE('{START_DATE}')
                                AND DATE(pickup_datetime) <= DATE('{END_DATE}')"""

CLEAN_INVALID_LONGITUDE = """DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                            WHERE (pickup_longitude = 0 OR dropoff_longitude = 0)
                                AND DATE(pickup_datetime) >= DATE('{START_DATE}')
                                AND DATE(pickup_datetime) <= DATE('{END_DATE}')"""

CLEAN_INVALID_LATITUDE = """DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                            WHERE (pickup_latitude = 0 OR dropoff_latitude = 0)
                                AND DATE(pickup_datetime) >= DATE('{START_DATE}')
                                AND DATE(pickup_datetime) <= DATE('{END_DATE}')"""

CLEAN_INVALID_PAYMENT_TYPE = """DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` 
                                WHERE payment_type NOT IN (1,2,3,4,5,6)
                                AND DATE(pickup_datetime) >= DATE('{START_DATE}')
                                AND DATE(pickup_datetime) <= DATE('{END_DATE}')"""

CLEAN_INVALID_TRIP_TYPE = """DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` 
                                WHERE trip_type NOT IN(1,2)
                                AND DATE(pickup_datetime) >= DATE('{START_DATE}')
                                AND DATE(pickup_datetime) <= DATE('{END_DATE}')"""

CLEAN_ZERO_TRIP_DISTANCE = """DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` 
                                WHERE (trip_distance = 0 
                                        OR ST_DISTANCE(ST_GEOGPOINT(pickup_longitude, pickup_latitude), 
                                                       ST_GEOGPOINT(dropoff_longitude, dropoff_latitude)) = 0)
                                AND DATE(pickup_datetime) >= DATE('{START_DATE}')
                                AND DATE(pickup_datetime) <= DATE('{END_DATE}')"""

CLEAN_DUPLICATED_RECORDS = """
                                CREATE TABLE `{PROJECT_ID}.{DATASET_ID}.temp_dedup_data` AS
                                SELECT vendor_id, pickup_datetime, dropoff_datetime, 
                                        store_and_fwd_flag, rate_code, pickup_longitude, 
                                        pickup_latitude, dropoff_longitude, dropoff_latitude, 
                                        passenger_count, trip_distance, fare_amount, extra, 
                                        mta_tax, tip_amount, tolls_amount, ehail_fee, 
                                        total_amount, payment_type, distance_between_service,
                                         time_between_service, trip_type, imp_surcharge
                                FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` 
                                WHERE DATE(pickup_datetime) >= DATE('{START_DATE}')
                                AND DATE(pickup_datetime) <= DATE('{END_DATE}')
                                GROUP BY vendor_id, pickup_datetime, dropoff_datetime, store_and_fwd_flag, 
                                    rate_code, pickup_longitude, pickup_latitude, dropoff_longitude, 
                                    dropoff_latitude, passenger_count, trip_distance, fare_amount, extra, 
                                    mta_tax, tip_amount, tolls_amount, ehail_fee, total_amount, payment_type, 
                                    distance_between_service, time_between_service, trip_type, imp_surcharge;
                                
                                TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`; 
                                
                                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` 
                                SELECT *
                                FROM `{PROJECT_ID}.{DATASET_ID}.temp_dedup_data`;"""

# endregion

# region Transform data

ENRICHMENT_QUERY = """CREATE TABLE `{PROJECT_ID}.{DATASET_ID}.temp_enriched_tlc`
                        PARTITION BY DATE_TRUNC(pickup_datetime, DAY)
                        OPTIONS(require_partition_filter=true) AS
                        WITH
                            case_data AS (
                            SELECT f.*,
                                    jslibs.h3.ST_H3(ST_GEOGPOINT(pickup_longitude, pickup_latitude),9) as pickup_hexagon_r9,
                                    jslibs.h3.ST_H3(ST_GEOGPOINT(dropoff_longitude, dropoff_latitude),9) as dropoff_hexagon_r9,
                                    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour
                            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` f
                            WHERE DATE(pickup_datetime) >= DATE('{START_DATE}')
                                AND DATE(pickup_datetime) <= DATE('{END_DATE}')
                            ),
                            
                            main_tab AS (
                                SELECT CASE
                                            WHEN pickup_hour >= 6 AND pickup_hour < 12 THEN 'Morning'
                                            WHEN pickup_hour >= 12 AND pickup_hour < 18 THEN 'Noon'
                                            WHEN pickup_hour >= 18 AND pickup_hour < 24 THEN 'Evening'
                                            WHEN pickup_hour >= 0 AND pickup_hour < 6 THEN 'Night'
                                        END AS daypart_of_pickup,
                                        * EXCEPT(pickup_hour)
                                FROM case_data
                            )
                            
                            select *
                            from main_tab;
                            
                            DROP TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`;
                            
                            ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.temp_enriched_tlc` RENAME TO `{TABLE_ID}`"""
# endregion

# region report data
REPORT_DATA_QUERY = """CREATE TABLE `{PROJECT_ID}.{DATASET_ID}.{REPORT_TABLE_ID}`
                        PARTITION BY report_date
                        OPTIONS(require_partition_filter=true) AS
                WITH 
                
                    top_pickup_hexagon_r9_data AS (
                        SELECT pickup_hexagon_r9, 
                                CAST(NULL AS STRING) AS dropoff_hexagon_r9, 
                                COUNT(1) AS hit_count, 
                                'TOP_{THRESHOLD}_PICKUP_HEXAGONS' AS label,
                                DATE(pickup_datetime) AS report_date
                        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                        WHERE DATE(pickup_datetime) >= DATE('{START_DATE}')
                        AND DATE(pickup_datetime) <= DATE('{END_DATE}')
                        GROUP BY pickup_hexagon_r9, report_date
                        ORDER BY hit_count DESC
                        LIMIT {THRESHOLD}),
                    
                    
                    top_dropoff_hexagon_r9_data AS (
                        SELECT CAST(NULL AS STRING) AS pickup_hexagon_r9, 
                              dropoff_hexagon_r9, 
                              COUNT(1) AS hit_count, 
                              'TOP_{THRESHOLD}_DROPOFF_HEXAGONS' AS label,
                              date(pickup_datetime) as report_date
                        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                        WHERE DATE(pickup_datetime) >= DATE('{START_DATE}')
                        AND DATE(pickup_datetime) <= DATE('{END_DATE}')
                        GROUP BY dropoff_hexagon_r9, report_date
                        ORDER BY hit_count DESC
                        LIMIT {THRESHOLD}),
                    
                    top_routes AS (
                        --the most popular dropoff hexagons
                        SELECT pickup_hexagon_r9,
                               dropoff_hexagon_r9, 
                               count(1) AS hit_count, 
                               'TOP_{THRESHOLD}_ROUTES' AS label,
                               DATE(pickup_datetime) AS report_date
                        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                        WHERE DATE(pickup_datetime) >= DATE('{START_DATE}')
                        AND DATE(pickup_datetime) <= DATE('{END_DATE}')
                        GROUP BY pickup_hexagon_r9, dropoff_hexagon_r9, report_date
                        ORDER BY hit_count DESC
                        LIMIT {THRESHOLD})
                    
                    SELECT * FROM top_pickup_hexagon_r9_data
                    UNION ALL
                    SELECT * FROM top_dropoff_hexagon_r9_data
                    UNION ALL
                    SELECT * FROM top_routes"""
# endregion
