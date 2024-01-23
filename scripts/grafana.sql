SELECT date AS "Aika", 
  sum_value AS "Kulutus", 
  sum_energy_fee / 100 AS "Energia hinta", 
  sum_spot_energy_fee_no_margin / 100 AS "Spot energia hinta (ei marginaalia)", 
  avg_energy_price / 100 AS "Energia keskihinta", 
  avg_spot_energy_price_no_margin / 100 AS "Spot energia keskihinta (ei marginaalia)" 
FROM consumption_day_by_day
WHERE date >= $__timeFrom() AND date <= $__timeTo()
ORDER BY
  1 ASC