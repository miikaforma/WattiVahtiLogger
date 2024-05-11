CREATE TABLE "energies" (
   "time" TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
	"metering_point_code" TEXT NOT NULL,
	"measure_type" INTEGER NOT NULL,
	"contract_type" SMALLINT NOT NULL,
    "source" TEXT NULL DEFAULT NULL,
	"measure_unit" VARCHAR(3) NOT NULL,
    "value" REAL NULL DEFAULT NULL,
	"energy_basic_fee" REAL NULL DEFAULT NULL,
	"energy_fee" REAL NULL DEFAULT NULL,
	"energy_margin" REAL NULL DEFAULT NULL,
	"transfer_basic_fee" REAL NULL DEFAULT NULL,
	"transfer_fee" REAL NULL DEFAULT NULL,
	"transfer_tax_fee" REAL NULL DEFAULT NULL,
	"tax_percentage" REAL NOT NULL DEFAULT '24',
    "night" BOOLEAN NOT NULL DEFAULT 'false',
	UNIQUE (time, metering_point_code, measure_type)
);

SELECT CREATE_HYPERTABLE('energies', BY_RANGE('time'));

-- Migration where we add the resolution_duration column and make the unique constraint to include it
ALTER TABLE "energies"
ADD COLUMN "resolution_duration" VARCHAR(10);

DROP INDEX IF EXISTS energies_time_metering_point_code_measure_type_key;

ALTER TABLE "energies"
ADD CONSTRAINT energies_unique_key UNIQUE (time, metering_point_code, measure_type, resolution_duration);

UPDATE "energies"
SET "resolution_duration" = 'PT1H'
WHERE "resolution_duration" IS NULL;

ALTER TABLE "energies"
ALTER COLUMN "resolution_duration" SET NOT NULL;
