-- Description: Create materialized consumption views for the database

-- Create a materialized view for the hourly consumptions
DROP MATERIALIZED VIEW IF EXISTS energies_consumption_hour_by_hour CASCADE;
CREATE MATERIALIZED VIEW energies_consumption_hour_by_hour
WITH (timescaledb.continuous)
AS
SELECT
	 -- Main properties
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
    
    -- Energy consumption
    ROUND(SUM(value) * 100000.) / 100000. AS energy_consumption,
    ROUND(SUM(CASE
        WHEN night = true THEN value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_consumption_night,
    ROUND(SUM(CASE
        WHEN night = false THEN value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_consumption_day,
    ROUND(AVG(VALUE) * 100000.) / 100000. AS energy_consumption_avg,
    
	 -- Energy fee
    ROUND(SUM(CASE
      WHEN contract_type = 2 THEN energy_fee * value
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = true THEN energy_fee * value
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee_night,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = false THEN energy_fee * value
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 2 THEN energy_fee * value
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_fee_avg,
    
    -- Energy fee margin
    ROUND(SUM(energy_margin * value) * 100000.) / 100000. AS energy_margin,
    ROUND(SUM(CASE
        WHEN night = true THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_margin_night,
    ROUND(SUM(CASE
        WHEN night = false THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_margin_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(energy_margin * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_margin_avg,
    
    -- Transfer fee
    ROUND(SUM(transfer_fee * value) * 100000.) / 100000. AS transfer_fee,
    ROUND(SUM(CASE
        WHEN night = true THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_fee_night,
    ROUND(SUM(CASE
        WHEN night = false THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_fee_avg,
    
    -- Transfer tax fee
    ROUND(SUM(transfer_tax_fee * value) * 100000.) / 100000. AS transfer_tax_fee,
    ROUND(SUM(CASE
        WHEN night = true THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_tax_fee_night,
    ROUND(SUM(CASE
        WHEN night = false THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_tax_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_tax_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_tax_fee_avg,
    
    -- Price
    ROUND(SUM(CASE
      WHEN contract_type = 2 THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = true THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price_night,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = false THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 2 THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS price_avg,
    
    -- Spot calculations even when fixed price
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0))) * value) * 100000.) / 100000. AS energy_fee_spot_no_margin,
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0)) + transfer_fee + transfer_tax_fee) * value) * 100000.) / 100000. AS price_spot_no_margin
FROM 
    energies
WHERE 
    measure_type = 1
GROUP BY 
    1, metering_point_code, measure_type, contract_type, source, measure_unit;


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
    
    -- Energy consumption
    ROUND(SUM(value) * 100000.) / 100000. AS energy_consumption,
    ROUND(SUM(CASE
        WHEN night = true THEN value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_consumption_night,
    ROUND(SUM(CASE
        WHEN night = false THEN value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_consumption_day,
    ROUND(AVG(VALUE) * 100000.) / 100000. AS energy_consumption_avg,
    
	 -- Energy fee
    ROUND(SUM(CASE
      WHEN contract_type = 2 THEN energy_fee * value
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = true THEN energy_fee * value
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee_night,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = false THEN energy_fee * value
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 2 THEN energy_fee * value
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_fee_avg,
    
    -- Energy fee margin
    ROUND(SUM(energy_margin * value) * 100000.) / 100000. AS energy_margin,
    ROUND(SUM(CASE
        WHEN night = true THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_margin_night,
    ROUND(SUM(CASE
        WHEN night = false THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_margin_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(energy_margin * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_margin_avg,
    
    -- Transfer fee
    ROUND(SUM(transfer_fee * value) * 100000.) / 100000. AS transfer_fee,
    ROUND(SUM(CASE
        WHEN night = true THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_fee_night,
    ROUND(SUM(CASE
        WHEN night = false THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_fee_avg,
    
    -- Transfer tax fee
    ROUND(SUM(transfer_tax_fee * value) * 100000.) / 100000. AS transfer_tax_fee,
    ROUND(SUM(CASE
        WHEN night = true THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_tax_fee_night,
    ROUND(SUM(CASE
        WHEN night = false THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_tax_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_tax_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_tax_fee_avg,
    
    -- Price
    ROUND(SUM(CASE
      WHEN contract_type = 2 THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = true THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price_night,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = false THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 2 THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS price_avg,
    
    -- Spot calculations even when fixed price
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0))) * value) * 100000.) / 100000. AS energy_fee_spot_no_margin,
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0)) + transfer_fee + transfer_tax_fee) * value) * 100000.) / 100000. AS price_spot_no_margin
FROM 
    energies
WHERE 
    measure_type = 1
GROUP BY 
    1, metering_point_code, measure_type, contract_type, source, measure_unit;

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
    
    -- Energy consumption
    ROUND(SUM(value) * 100000.) / 100000. AS energy_consumption,
    ROUND(SUM(CASE
        WHEN night = true THEN value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_consumption_night,
    ROUND(SUM(CASE
        WHEN night = false THEN value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_consumption_day,
    ROUND(AVG(VALUE) * 100000.) / 100000. AS energy_consumption_avg,
    
	 -- Energy fee
    ROUND(SUM(CASE
      WHEN contract_type = 2 THEN energy_fee * value
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = true THEN energy_fee * value
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee_night,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = false THEN energy_fee * value
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 2 THEN energy_fee * value
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_fee_avg,
    
    -- Energy fee margin
    ROUND(SUM(energy_margin * value) * 100000.) / 100000. AS energy_margin,
    ROUND(SUM(CASE
        WHEN night = true THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_margin_night,
    ROUND(SUM(CASE
        WHEN night = false THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_margin_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(energy_margin * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_margin_avg,
    
    -- Transfer fee
    ROUND(SUM(transfer_fee * value) * 100000.) / 100000. AS transfer_fee,
    ROUND(SUM(CASE
        WHEN night = true THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_fee_night,
    ROUND(SUM(CASE
        WHEN night = false THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_fee_avg,
    
    -- Transfer tax fee
    ROUND(SUM(transfer_tax_fee * value) * 100000.) / 100000. AS transfer_tax_fee,
    ROUND(SUM(CASE
        WHEN night = true THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_tax_fee_night,
    ROUND(SUM(CASE
        WHEN night = false THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_tax_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_tax_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_tax_fee_avg,
    
    -- Price
    ROUND(SUM(CASE
      WHEN contract_type = 2 THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = true THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price_night,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = false THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 2 THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS price_avg,
    
    -- Spot calculations even when fixed price
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0))) * value) * 100000.) / 100000. AS energy_fee_spot_no_margin,
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0)) + transfer_fee + transfer_tax_fee) * value) * 100000.) / 100000. AS price_spot_no_margin
FROM 
    energies
WHERE 
    measure_type = 1
GROUP BY 
    1, metering_point_code, measure_type;

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
    
    -- Energy consumption
    ROUND(SUM(value) * 100000.) / 100000. AS energy_consumption,
    ROUND(SUM(CASE
        WHEN night = true THEN value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_consumption_night,
    ROUND(SUM(CASE
        WHEN night = false THEN value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_consumption_day,
    ROUND(AVG(VALUE) * 100000.) / 100000. AS energy_consumption_avg,
    
	 -- Energy fee
    ROUND(SUM(CASE
      WHEN contract_type = 2 THEN energy_fee * value
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = true THEN energy_fee * value
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee_night,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = false THEN energy_fee * value
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
      ELSE NULL
    END) * 100000.) / 100000. AS energy_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 2 THEN energy_fee * value
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_fee_avg,
    
    -- Energy fee margin
    ROUND(SUM(energy_margin * value) * 100000.) / 100000. AS energy_margin,
    ROUND(SUM(CASE
        WHEN night = true THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_margin_night,
    ROUND(SUM(CASE
        WHEN night = false THEN energy_margin * value
        ELSE NULL
    END) * 100000.) / 100000. AS energy_margin_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(energy_margin * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS energy_margin_avg,
    
    -- Transfer fee
    ROUND(SUM(transfer_fee * value) * 100000.) / 100000. AS transfer_fee,
    ROUND(SUM(CASE
        WHEN night = true THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_fee_night,
    ROUND(SUM(CASE
        WHEN night = false THEN transfer_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_fee_avg,
    
    -- Transfer tax fee
    ROUND(SUM(transfer_tax_fee * value) * 100000.) / 100000. AS transfer_tax_fee,
    ROUND(SUM(CASE
        WHEN night = true THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_tax_fee_night,
    ROUND(SUM(CASE
        WHEN night = false THEN transfer_tax_fee * value
        ELSE NULL
    END) * 100000.) / 100000. AS transfer_tax_fee_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(transfer_tax_fee * value) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS transfer_tax_fee_avg,
    
    -- Price
    ROUND(SUM(CASE
      WHEN contract_type = 2 THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = true THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 AND night = true THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price_night,
    ROUND(SUM(CASE
      WHEN contract_type = 2 AND night = false THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
      WHEN contract_type = 3 AND night = false THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
      ELSE NULL
    END) * 100000.) / 100000. AS price_day,
    CASE
      WHEN SUM(value) != 0 THEN ROUND(SUM(CASE
        WHEN contract_type = 2 THEN (energy_fee + transfer_fee + transfer_tax_fee) * value
        WHEN contract_type = 3 THEN ((spot_price * (tax_percentage / 100. + 1.0)) + energy_margin + transfer_fee + transfer_tax_fee) * value
        ELSE NULL
      END) / SUM(value) * 100000.) / 100000.
      ELSE NULL
    END AS price_avg,
    
    -- Spot calculations even when fixed price
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0))) * value) * 100000.) / 100000. AS energy_fee_spot_no_margin,
    ROUND(SUM(((spot_price * (tax_percentage / 100. + 1.0)) + transfer_fee + transfer_tax_fee) * value) * 100000.) / 100000. AS price_spot_no_margin
FROM 
    energies
WHERE 
    measure_type = 1
GROUP BY 
    1, metering_point_code, measure_type;

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