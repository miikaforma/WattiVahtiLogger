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
