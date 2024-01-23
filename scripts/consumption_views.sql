-- Description: Create materialized consumption views for the database

-- Create a materialized view for the hourly consumptions
DROP MATERIALIZED VIEW IF EXISTS energies_consumption_hour_by_hour CASCADE;
CREATE MATERIALIZED VIEW energies_consumption_hour_by_hour
WITH (timescaledb.continuous)
AS
SELECT 
    time_bucket('1 hour', time) AS datetime,
    metering_point_code,
    measure_type,
    contract_type,
    source,
    measure_unit,
    SUM(value) AS value,
    SUM(energy_basic_fee) AS energy_basic_fee,
    AVG(CASE
        WHEN contract_type = 2 THEN energy_fee
        WHEN contract_type = 3 THEN (spot_price * (tax_percentage / 100. + 1.0)) + energy_margin
        ELSE NULL
    END) * SUM(value) AS energy_fee,
    (AVG(CASE
        WHEN contract_type = 2 THEN energy_fee
        WHEN contract_type = 3 THEN (spot_price * (tax_percentage / 100. + 1.0)) + energy_margin
        ELSE NULL
    END) + AVG(transfer_fee) + AVG(transfer_tax_fee)) * SUM(value) AS price,

    AVG(spot_price * (tax_percentage / 100. + 1.0)) * SUM(value) AS spot_energy_fee_no_margin,
    (AVG(spot_price * (tax_percentage / 100. + 1.0)) + AVG(transfer_fee) + AVG(transfer_tax_fee)) * SUM(value) AS spot_price_no_margin,

    SUM(transfer_basic_fee) AS transfer_basic_fee,
    SUM(transfer_fee * value) AS transfer_fee,
    SUM(transfer_tax_fee * value) AS transfer_tax_fee,
    AVG(tax_percentage) AS tax_percentage,
    BOOL_OR(night) AS night,
    AVG(spot_price) AS spot_price,
    AVG(spot_price * (tax_percentage / 100. + 1.0)) AS spot_price_with_tax
FROM 
    energies
WHERE 
    measure_type = 1
GROUP BY 
    datetime, metering_point_code, measure_type, contract_type, source, measure_unit;

-- To drop the view for the hourly consumptions, run:
-- DROP MATERIALIZED VIEW energies_consumption_hour_by_hour;

-- To manually referesh the hourly consumptions, run:
-- CALL refresh_continuous_aggregate('energies_consumption_hour_by_hour', NULL, NULL);

-- Create a materialized view for the daily consumptions
DROP MATERIALIZED VIEW IF EXISTS energies_consumption_day_by_day;
CREATE MATERIALIZED VIEW energies_consumption_day_by_day
WITH (timescaledb.continuous)
AS
SELECT 
    time_bucket('1 day', datetime, 'Europe/Helsinki') AS date,
    metering_point_code,
    ROUND(AVG(energy_basic_fee) * 100.) / 100. AS avg_energy_basic_fee,
    ROUND(AVG(transfer_basic_fee) * 100.) / 100. AS avg_transfer_basic_fee,
    ROUND(SUM(value) * 100.) / 100. AS sum_value,
    ROUND(SUM(CASE WHEN night = true THEN value ELSE 0 END) * 100.) / 100. AS sum_value_night,
    ROUND(SUM(CASE WHEN night = false THEN value ELSE 0 END) * 100.) / 100. AS sum_value_day,
    ROUND(SUM(energy_fee) * 100.) / 100. AS sum_energy_fee,
    ROUND((SUM(energy_fee) / NULLIF(SUM(value), 0)) * 100.) / 100. AS avg_energy_price,
    
    ROUND(SUM(spot_energy_fee_no_margin) * 100.) / 100. AS sum_spot_energy_fee_no_margin,
    ROUND((SUM(spot_energy_fee_no_margin) / NULLIF(SUM(value), 0)) * 100.) / 100. AS avg_spot_energy_price_no_margin,
    ROUND(SUM(spot_price_no_margin) * 100.) / 100. AS sum_spot_price_no_margin,
    
    ROUND(SUM(price) * 100.) / 100. AS sum_price,
    ROUND(SUM(CASE WHEN night = true THEN price ELSE 0 END) * 100.) / 100. AS sum_price_night,
    ROUND(SUM(CASE WHEN night = false THEN price ELSE 0 END) * 100.) / 100. AS sum_price_day,
    ROUND((SUM(price) / NULLIF(SUM(value), 0)) * 100.) / 100. AS avg_price,
    ROUND(SUM(transfer_fee) * 100.) / 100. AS sum_transfer_fee,
    ROUND(SUM(CASE WHEN night = true THEN transfer_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_fee_night,
    ROUND(SUM(CASE WHEN night = false THEN transfer_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_fee_day,
    ROUND(SUM(transfer_tax_fee) * 100.) / 100. AS sum_transfer_tax_fee,
    ROUND(SUM(CASE WHEN night = true THEN transfer_tax_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_tax_fee_night,
    ROUND(SUM(CASE WHEN night = false THEN transfer_tax_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_tax_fee_day,
    ROUND(AVG(spot_price) * 100.) / 100. AS avg_spot_price,
    ROUND(AVG(spot_price_with_tax) * 100.) / 100. AS avg_spot_price_with_tax
FROM 
    energies_consumption_hour_by_hour
GROUP BY 
    date, metering_point_code, measure_type
ORDER BY 
    date DESC;

-- To drop the view for the daily consumptions, run:
-- DROP MATERIALIZED VIEW energies_consumption_day_by_day;

-- To manually referesh the daily consumptions, run:
-- CALL refresh_continuous_aggregate('energies_consumption_day_by_day', NULL, NULL);

-- Create a materialized view for the monthly consumptions
DROP MATERIALIZED VIEW IF EXISTS energies_consumption_month_by_month;
CREATE MATERIALIZED VIEW energies_consumption_month_by_month
WITH (timescaledb.continuous)
AS
SELECT 
    time_bucket('1 month', datetime, 'Europe/Helsinki') AS date,
    metering_point_code,
    ROUND(AVG(energy_basic_fee) * 100.) / 100. AS avg_energy_basic_fee,
    ROUND(AVG(transfer_basic_fee) * 100.) / 100. AS avg_transfer_basic_fee,
    ROUND(SUM(value) * 100.) / 100. AS sum_value,
    ROUND(SUM(CASE WHEN night = true THEN value ELSE 0 END) * 100.) / 100. AS sum_value_night,
    ROUND(SUM(CASE WHEN night = false THEN value ELSE 0 END) * 100.) / 100. AS sum_value_day,
    ROUND(SUM(energy_fee) * 100.) / 100. AS sum_energy_fee,
    ROUND((SUM(energy_fee) / NULLIF(SUM(value), 0)) * 100.) / 100. AS avg_energy_price,
    
    ROUND(SUM(spot_energy_fee_no_margin) * 100.) / 100. AS sum_spot_energy_fee_no_margin,
    ROUND((SUM(spot_energy_fee_no_margin) / NULLIF(SUM(value), 0)) * 100.) / 100. AS avg_spot_energy_price_no_margin,
    ROUND(SUM(spot_price_no_margin) * 100.) / 100. AS sum_spot_price_no_margin,
    
    ROUND(SUM(price) * 100.) / 100. AS sum_price,
    ROUND(SUM(CASE WHEN night = true THEN price ELSE 0 END) * 100.) / 100. AS sum_price_night,
    ROUND(SUM(CASE WHEN night = false THEN price ELSE 0 END) * 100.) / 100. AS sum_price_day,
    ROUND((SUM(price) / NULLIF(SUM(value), 0)) * 100.) / 100. AS avg_price,
    ROUND(SUM(transfer_fee) * 100.) / 100. AS sum_transfer_fee,
    ROUND(SUM(CASE WHEN night = true THEN transfer_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_fee_night,
    ROUND(SUM(CASE WHEN night = false THEN transfer_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_fee_day,
    ROUND(SUM(transfer_tax_fee) * 100.) / 100. AS sum_transfer_tax_fee,
    ROUND(SUM(CASE WHEN night = true THEN transfer_tax_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_tax_fee_night,
    ROUND(SUM(CASE WHEN night = false THEN transfer_tax_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_tax_fee_day,
    ROUND(AVG(spot_price) * 100.) / 100. AS avg_spot_price,
    ROUND(AVG(spot_price_with_tax) * 100.) / 100. AS avg_spot_price_with_tax
FROM 
    energies_consumption_hour_by_hour
GROUP BY 
    date, metering_point_code, measure_type
ORDER BY 
    date DESC;

-- To drop the view for the monthly consumptions, run:
-- DROP MATERIALIZED VIEW energies_consumption_month_by_month;

-- To manually referesh the monthly consumptions, run:
-- CALL refresh_continuous_aggregate('energies_consumption_month_by_month', NULL, NULL);

-- Create a materialized view for the yearly consumptions
DROP MATERIALIZED VIEW IF EXISTS energies_consumption_year_by_year;
CREATE MATERIALIZED VIEW energies_consumption_year_by_year
WITH (timescaledb.continuous)
AS
SELECT 
    time_bucket('1 year', datetime, 'Europe/Helsinki') AS date,
    metering_point_code,
    ROUND(AVG(energy_basic_fee) * 100.) / 100. AS avg_energy_basic_fee,
    ROUND(AVG(transfer_basic_fee) * 100.) / 100. AS avg_transfer_basic_fee,
    ROUND(SUM(value) * 100.) / 100. AS sum_value,
    ROUND(SUM(CASE WHEN night = true THEN value ELSE 0 END) * 100.) / 100. AS sum_value_night,
    ROUND(SUM(CASE WHEN night = false THEN value ELSE 0 END) * 100.) / 100. AS sum_value_day,
    ROUND(SUM(energy_fee) * 100.) / 100. AS sum_energy_fee,
    ROUND((SUM(energy_fee) / NULLIF(SUM(value), 0)) * 100.) / 100. AS avg_energy_price,
    
    ROUND(SUM(spot_energy_fee_no_margin) * 100.) / 100. AS sum_spot_energy_fee_no_margin,
    ROUND((SUM(spot_energy_fee_no_margin) / NULLIF(SUM(value), 0)) * 100.) / 100. AS avg_spot_energy_price_no_margin,
    ROUND(SUM(spot_price_no_margin) * 100.) / 100. AS sum_spot_price_no_margin,

    ROUND(SUM(price) * 100.) / 100. AS sum_price,
    ROUND(SUM(CASE WHEN night = true THEN price ELSE 0 END) * 100.) / 100. AS sum_price_night,
    ROUND(SUM(CASE WHEN night = false THEN price ELSE 0 END) * 100.) / 100. AS sum_price_day,
    ROUND((SUM(price) / NULLIF(SUM(value), 0)) * 100.) / 100. AS avg_price,
    ROUND(SUM(transfer_fee) * 100.) / 100. AS sum_transfer_fee,
    ROUND(SUM(CASE WHEN night = true THEN transfer_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_fee_night,
    ROUND(SUM(CASE WHEN night = false THEN transfer_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_fee_day,
    ROUND(SUM(transfer_tax_fee) * 100.) / 100. AS sum_transfer_tax_fee,
    ROUND(SUM(CASE WHEN night = true THEN transfer_tax_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_tax_fee_night,
    ROUND(SUM(CASE WHEN night = false THEN transfer_tax_fee ELSE 0 END) * 100.) / 100. AS sum_transfer_tax_fee_day,
    ROUND(AVG(spot_price) * 100.) / 100. AS avg_spot_price,
    ROUND(AVG(spot_price_with_tax) * 100.) / 100. AS avg_spot_price_with_tax
FROM 
    energies_consumption_hour_by_hour
GROUP BY 
    date, metering_point_code, measure_type
ORDER BY 
    date DESC;

-- To drop the view for the yearly consumptions, run:
-- DROP MATERIALIZED VIEW energies_consumption_year_by_year;

-- To manually referesh the yearly consumptions, run:
-- CALL refresh_continuous_aggregate('energies_consumption_year_by_year', NULL, NULL);


-- To manually delete all the views, run:
-- DROP MATERIALIZED VIEW energies_consumption_year_by_year;
-- DROP MATERIALIZED VIEW energies_consumption_month_by_month;
-- DROP MATERIALIZED VIEW energies_consumption_day_by_day;
-- DROP MATERIALIZED VIEW energies_consumption_hour_by_hour;

-- To manually refresh all the views, run:
-- CALL refresh_continuous_aggregate('energies_consumption_hour_by_hour', NULL, NULL);
-- CALL refresh_continuous_aggregate('energies_consumption_day_by_day', NULL, NULL);
-- CALL refresh_continuous_aggregate('energies_consumption_month_by_month', NULL, NULL);
-- CALL refresh_continuous_aggregate('energies_consumption_year_by_year', NULL, NULL);

-- To check the contents of the views, run:
-- SELECT * FROM energies_consumption_hour_by_hour;
-- SELECT * FROM energies_consumption_day_by_day;
-- SELECT * FROM energies_consumption_month_by_month;
-- SELECT * FROM energies_consumption_year_by_year;