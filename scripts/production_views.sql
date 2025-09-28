-- Description: Create materialized production views for the database

-- Create a materialized view for the 15 minute productions
DROP MATERIALIZED VIEW IF EXISTS energies_production_15min_by_15min;
CREATE MATERIALIZED VIEW energies_production_15min_by_15min
WITH (timescaledb.continuous)
AS
SELECT 
    time_bucket('15 minutes', time) AS time,
    metering_point_code,
    measure_type,
    contract_type,
    source,
    measure_unit,
    AVG(tax_percentage) AS tax_percentage,
    BOOL_OR(night) AS night,
    ROUND(AVG(spot_price) * 100000.) / 100000. AS spot_price,
    ROUND(AVG(spot_price * (tax_percentage / 100.)) * 100000.) / 100000. AS spot_price_tax,
    ROUND(AVG(spot_price * (tax_percentage / 100. + 1.0)) * 100000.) / 100000. AS spot_price_with_tax,
    
    -- Basic fees
    ROUND(AVG(energy_basic_fee) * 100000.) / 100000. AS energy_basic_fee,
    ROUND(AVG(transfer_basic_fee) * 100000.) / 100000. AS transfer_basic_fee,
    
    -- Energy production
    ROUND(SUM(value) * 100000.) / 100000. AS energy_production,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_production_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_production_day,
    ROUND(AVG(VALUE) * 100000.) / 100000. AS energy_production_avg,
    
	 -- Energy fee
    ROUND(SUM(CASE
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_fee_night,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_fee_avg,
    
    -- Energy fee margin
    ROUND(SUM(energy_margin * value) * 100000.) / 100000. AS energy_margin,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_margin_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_margin_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(energy_margin * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_margin_avg,
    
    -- Transfer fee
    ROUND(SUM(transfer_fee * value) * 100000.) / 100000. AS transfer_fee,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_fee_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_fee_avg,
    
    -- Transfer tax fee
    ROUND(SUM(transfer_tax_fee * value) * 100000.) / 100000. AS transfer_tax_fee,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_tax_fee_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_tax_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_tax_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_tax_fee_avg,
    
    -- Price
    ROUND(SUM(CASE
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS price_night,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS price_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS price_avg,
    
    -- Spot calculations even when fixed price
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0))) * value) * 100000.) / 100000. AS energy_fee_spot_no_margin,
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value) * 100000.) / 100000. AS price_spot_no_margin
FROM 
    energies
WHERE 
    measure_type = 6 AND resolution_duration = 'PT15M'
GROUP BY 
    1, metering_point_code, measure_type, contract_type, source, measure_unit;

-- To drop the view for the 15 minute productions, run:
-- DROP MATERIALIZED VIEW energies_production_15min_by_15min;

-- To manually referesh the 15 minute productions, run:
-- CALL refresh_continuous_aggregate('energies_production_15min_by_15min', NULL, NULL);

-- To check the earliest timestamp in the 15 minute data for the WHERE clause in the PT1H and up views, run:
-- SELECT MIN(time) FROM energies WHERE resolution_duration = 'PT15M';

-- Create a materialized view for the hourly productions
DROP MATERIALIZED VIEW IF EXISTS energies_production_hour_by_hour;
CREATE MATERIALIZED VIEW energies_production_hour_by_hour
WITH (timescaledb.continuous)
AS
SELECT 
    time_bucket('1 hour', time) AS time,
    metering_point_code,
    measure_type,
    contract_type,
    source,
    measure_unit,
    AVG(tax_percentage) AS tax_percentage,
    BOOL_OR(night) AS night,
    ROUND(AVG(spot_price) * 100000.) / 100000. AS spot_price,
    ROUND(AVG(spot_price * (tax_percentage / 100.)) * 100000.) / 100000. AS spot_price_tax,
    ROUND(AVG(spot_price * (tax_percentage / 100. + 1.0)) * 100000.) / 100000. AS spot_price_with_tax,
    
    -- Basic fees
    ROUND(AVG(energy_basic_fee) * 100000.) / 100000. AS energy_basic_fee,
    ROUND(AVG(transfer_basic_fee) * 100000.) / 100000. AS transfer_basic_fee,
    
    -- Energy production
    ROUND(SUM(value) * 100000.) / 100000. AS energy_production,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_production_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_production_day,
    ROUND(AVG(VALUE) * 100000.) / 100000. AS energy_production_avg,
    
	 -- Energy fee
    ROUND(SUM(CASE
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_fee_night,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_fee_avg,
    
    -- Energy fee margin
    ROUND(SUM(energy_margin * value) * 100000.) / 100000. AS energy_margin,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_margin_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_margin_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(energy_margin * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_margin_avg,
    
    -- Transfer fee
    ROUND(SUM(transfer_fee * value) * 100000.) / 100000. AS transfer_fee,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_fee_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_fee_avg,
    
    -- Transfer tax fee
    ROUND(SUM(transfer_tax_fee * value) * 100000.) / 100000. AS transfer_tax_fee,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_tax_fee_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_tax_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_tax_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_tax_fee_avg,
    
    -- Price
    ROUND(SUM(CASE
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS price_night,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS price_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS price_avg,
    
    -- Spot calculations even when fixed price
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0))) * value) * 100000.) / 100000. AS energy_fee_spot_no_margin,
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value) * 100000.) / 100000. AS price_spot_no_margin
FROM 
    energies
WHERE
    measure_type = 6 AND
    (
        (resolution_duration = 'PT1H' AND TIME < '2023-12-31 22:00:00+00') OR
        (resolution_duration = 'PT15M' AND time >= '2023-12-31 22:00:00+00')
    )
GROUP BY 
    1, metering_point_code, measure_type, contract_type, source, measure_unit;

-- To drop the view for the hourly productions, run:
-- DROP MATERIALIZED VIEW energies_production_hour_by_hour;

-- To manually referesh the hourly productions, run:
-- CALL refresh_continuous_aggregate('energies_production_hour_by_hour', NULL, NULL);

-- Create a materialized view for the daily productions
DROP MATERIALIZED VIEW IF EXISTS energies_production_day_by_day;
CREATE MATERIALIZED VIEW energies_production_day_by_day
WITH (timescaledb.continuous)
AS
SELECT 
    time_bucket('1 day', time, 'Europe/Helsinki') AS time,
    metering_point_code,
    measure_type,
    contract_type,
    source,
    measure_unit,
    AVG(tax_percentage) AS tax_percentage,
    BOOL_OR(night) AS night,
    ROUND(AVG(spot_price) * 100000.) / 100000. AS spot_price,
    ROUND(AVG(spot_price * (tax_percentage / 100.)) * 100000.) / 100000. AS spot_price_tax,
    ROUND(AVG(spot_price * (tax_percentage / 100. + 1.0)) * 100000.) / 100000. AS spot_price_with_tax,
    
    -- Basic fees
    ROUND(AVG(energy_basic_fee) * 100000.) / 100000. AS energy_basic_fee,
    ROUND(AVG(transfer_basic_fee) * 100000.) / 100000. AS transfer_basic_fee,
    
    -- Energy production
    ROUND(SUM(value) * 100000.) / 100000. AS energy_production,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_production_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_production_day,
    ROUND(AVG(VALUE) * 100000.) / 100000. AS energy_production_avg,
    
	 -- Energy fee
    ROUND(SUM(CASE
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_fee_night,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_fee_avg,
    
    -- Energy fee margin
    ROUND(SUM(energy_margin * value) * 100000.) / 100000. AS energy_margin,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_margin_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_margin_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(energy_margin * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_margin_avg,
    
    -- Transfer fee
    ROUND(SUM(transfer_fee * value) * 100000.) / 100000. AS transfer_fee,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_fee_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_fee_avg,
    
    -- Transfer tax fee
    ROUND(SUM(transfer_tax_fee * value) * 100000.) / 100000. AS transfer_tax_fee,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_tax_fee_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_tax_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_tax_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_tax_fee_avg,
    
    -- Price
    ROUND(SUM(CASE
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS price_night,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS price_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS price_avg,
    
    -- Spot calculations even when fixed price
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0))) * value) * 100000.) / 100000. AS energy_fee_spot_no_margin,
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value) * 100000.) / 100000. AS price_spot_no_margin
FROM 
    energies
WHERE 
    measure_type = 6 AND
    (
        (resolution_duration = 'PT1H' AND TIME < '2023-12-31 22:00:00+00') OR
        (resolution_duration = 'PT15M' AND time >= '2023-12-31 22:00:00+00')
    )
GROUP BY 
    1, metering_point_code, measure_type, contract_type, source, measure_unit;

-- To drop the view for the daily productions, run:
-- DROP MATERIALIZED VIEW energies_production_day_by_day;

-- To manually referesh the daily productions, run:
-- CALL refresh_continuous_aggregate('energies_production_day_by_day', NULL, NULL);

-- Create a materialized view for the monthly productions
DROP MATERIALIZED VIEW IF EXISTS energies_production_month_by_month;
CREATE MATERIALIZED VIEW energies_production_month_by_month
WITH (timescaledb.continuous)
AS
SELECT 
    time_bucket('1 month', time, 'Europe/Helsinki') AS time,
    metering_point_code,
    measure_type,
    null AS contract_type,
    null AS source,
    null AS measure_unit,
    AVG(tax_percentage) AS tax_percentage,
    BOOL_OR(night) AS night,
    ROUND(AVG(spot_price) * 100000.) / 100000. AS spot_price,
    ROUND(AVG(spot_price * (tax_percentage / 100.)) * 100000.) / 100000. AS spot_price_tax,
    ROUND(AVG(spot_price * (tax_percentage / 100. + 1.0)) * 100000.) / 100000. AS spot_price_with_tax,
    
    -- Basic fees
    ROUND(AVG(energy_basic_fee) * 100000.) / 100000. AS energy_basic_fee,
    ROUND(AVG(transfer_basic_fee) * 100000.) / 100000. AS transfer_basic_fee,
    
    -- Energy production
    ROUND(SUM(value) * 100000.) / 100000. AS energy_production,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_production_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_production_day,
    ROUND(AVG(VALUE) * 100000.) / 100000. AS energy_production_avg,
    
	 -- Energy fee
    ROUND(SUM(CASE
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_fee_night,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_fee_avg,
    
    -- Energy fee margin
    ROUND(SUM(energy_margin * value) * 100000.) / 100000. AS energy_margin,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_margin_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_margin_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(energy_margin * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_margin_avg,
    
    -- Transfer fee
    ROUND(SUM(transfer_fee * value) * 100000.) / 100000. AS transfer_fee,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_fee_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_fee_avg,
    
    -- Transfer tax fee
    ROUND(SUM(transfer_tax_fee * value) * 100000.) / 100000. AS transfer_tax_fee,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_tax_fee_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_tax_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_tax_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_tax_fee_avg,
    
    -- Price
    ROUND(SUM(CASE
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS price_night,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS price_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS price_avg,
    
    -- Spot calculations even when fixed price
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0))) * value) * 100000.) / 100000. AS energy_fee_spot_no_margin,
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value) * 100000.) / 100000. AS price_spot_no_margin
FROM 
    energies
WHERE 
    measure_type = 6 AND
    (
        (resolution_duration = 'PT1H' AND TIME < '2023-12-31 22:00:00+00') OR
        (resolution_duration = 'PT15M' AND time >= '2023-12-31 22:00:00+00')
    )
GROUP BY 
    1, metering_point_code, measure_type;

-- To drop the view for the monthly productions, run:
-- DROP MATERIALIZED VIEW energies_production_month_by_month;

-- To manually referesh the monthly productions, run:
-- CALL refresh_continuous_aggregate('energies_production_month_by_month', NULL, NULL);

-- Create a materialized view for the yearly productions
DROP MATERIALIZED VIEW IF EXISTS energies_production_year_by_year;
CREATE MATERIALIZED VIEW energies_production_year_by_year
WITH (timescaledb.continuous)
AS
SELECT 
    time_bucket('1 year', time, 'Europe/Helsinki') AS time,
    metering_point_code,
    measure_type,
    null AS contract_type,
    null AS source,
    null AS measure_unit,
    AVG(tax_percentage) AS tax_percentage,
    BOOL_OR(night) AS night,
    ROUND(AVG(spot_price) * 100000.) / 100000. AS spot_price,
    ROUND(AVG(spot_price * (tax_percentage / 100.)) * 100000.) / 100000. AS spot_price_tax,
    ROUND(AVG(spot_price * (tax_percentage / 100. + 1.0)) * 100000.) / 100000. AS spot_price_with_tax,
    
    -- Basic fees
    ROUND(AVG(energy_basic_fee) * 100000.) / 100000. AS energy_basic_fee,
    ROUND(AVG(transfer_basic_fee) * 100000.) / 100000. AS transfer_basic_fee,
    
    -- Energy production
    ROUND(SUM(value) * 100000.) / 100000. AS energy_production,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_production_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_production_day,
    ROUND(AVG(VALUE) * 100000.) / 100000. AS energy_production_avg,
    
	 -- Energy fee
    ROUND(SUM(CASE
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_fee_night,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_fee_avg,
    
    -- Energy fee margin
    ROUND(SUM(energy_margin * value) * 100000.) / 100000. AS energy_margin,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_margin_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS energy_margin_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(energy_margin * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_margin_avg,
    
    -- Transfer fee
    ROUND(SUM(transfer_fee * value) * 100000.) / 100000. AS transfer_fee,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_fee_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_fee_avg,
    
    -- Transfer tax fee
    ROUND(SUM(transfer_tax_fee * value) * 100000.) / 100000. AS transfer_tax_fee,
    COALESCE(ROUND(SUM(CASE
        WHEN night = true THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_tax_fee_night,
    COALESCE(ROUND(SUM(CASE
        WHEN night = false THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000., 0) AS transfer_tax_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_tax_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_tax_fee_avg,
    
    -- Price
    ROUND(SUM(CASE
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS price_night,
    COALESCE(ROUND(SUM(CASE
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
      ELSE NULL
    END) * 100000.) / 100000., 0) AS price_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(energy_margin, 0) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS price_avg,
    
    -- Spot calculations even when fixed price
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0))) * value) * 100000.) / 100000. AS energy_fee_spot_no_margin,
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0)) - COALESCE(transfer_fee, 0) - COALESCE(transfer_tax_fee, 0)) * value) * 100000.) / 100000. AS price_spot_no_margin
FROM 
    energies
WHERE 
    measure_type = 6 AND
    (
        (resolution_duration = 'PT1H' AND TIME < '2023-12-31 22:00:00+00') OR
        (resolution_duration = 'PT15M' AND time >= '2023-12-31 22:00:00+00')
    )
GROUP BY 
    1, metering_point_code, measure_type;

-- To drop the view for the yearly productions, run:
-- DROP MATERIALIZED VIEW energies_production_year_by_year;

-- To manually referesh the yearly productions, run:
-- CALL refresh_continuous_aggregate('energies_production_year_by_year', NULL, NULL);


-- To manually delete all the views, run:
-- DROP MATERIALIZED VIEW energies_production_year_by_year;
-- DROP MATERIALIZED VIEW energies_production_month_by_month;
-- DROP MATERIALIZED VIEW energies_production_day_by_day;
-- DROP MATERIALIZED VIEW energies_production_hour_by_hour;
-- DROP MATERIALIZED VIEW energies_production_15min_by_15min;

-- To manually refresh all the views, run:
-- CALL refresh_continuous_aggregate('energies_production_15min_by_15min', NULL, NULL);
-- CALL refresh_continuous_aggregate('energies_production_hour_by_hour', NULL, NULL);
-- CALL refresh_continuous_aggregate('energies_production_day_by_day', NULL, NULL);
-- CALL refresh_continuous_aggregate('energies_production_month_by_month', NULL, NULL);
-- CALL refresh_continuous_aggregate('energies_production_year_by_year', NULL, NULL);

-- To check the contents of the views, run:
-- SELECT * FROM energies_production_15min_by_15min;
-- SELECT * FROM energies_production_hour_by_hour;
-- SELECT * FROM energies_production_day_by_day;
-- SELECT * FROM energies_production_month_by_month;
-- SELECT * FROM energies_production_year_by_year;