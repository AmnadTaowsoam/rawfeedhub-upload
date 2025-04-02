CREATE SCHEMA IF NOT EXISTS raw_material;

-- Create raw_material.materials
CREATE TABLE raw_material.materials (
    material_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    material_code TEXT UNIQUE NOT NULL,
    material_description TEXT NOT NULL
);

-- Create raw_material.plants
CREATE TABLE raw_material.plants (
    plant_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    plant TEXT UNIQUE NOT NULL,
    plant_name TEXT NOT NULL
);

-- Create raw_material.vendors
CREATE TABLE raw_material.vendors (
    vendor_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    vendor_code TEXT UNIQUE NOT NULL,
    vendor_name TEXT NOT NULL
);

-- Correct samples table schema
CREATE TABLE raw_material.samples (
    sample_id UUID DEFAULT gen_random_uuid(),
    material_id UUID NOT NULL REFERENCES raw_material.materials(material_id),
    plant_id UUID NOT NULL REFERENCES raw_material.plants(plant_id),
    vendor_id UUID NOT NULL REFERENCES raw_material.vendors(vendor_id),
    sample_no TEXT NOT NULL,
    inspection_lot TEXT,
    valuation_date DATE NOT NULL,
    batch_no TEXT,
    material_doc TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sample_id, valuation_date) -- Include valuation_date in the primary key
) PARTITION BY RANGE (valuation_date);

-- Create partitions for samples
CREATE TABLE raw_material.samples_2015_2019 PARTITION OF raw_material.samples
FOR VALUES FROM ('2015-01-01') TO ('2019-12-31');

CREATE TABLE raw_material.samples_2020_2024 PARTITION OF raw_material.samples
FOR VALUES FROM ('2020-01-01') TO ('2024-12-31');

CREATE TABLE raw_material.samples_2025_2030 PARTITION OF raw_material.samples
FOR VALUES FROM ('2025-01-01') TO ('2030-12-31');

-- Create raw_material.analysis_results with valuation_date in the primary key
CREATE TABLE raw_material.analysis_results (
    result_id UUID DEFAULT gen_random_uuid(),
    sample_id UUID NOT NULL,
    valuation_date DATE NOT NULL,
    analysis_parameter TEXT NOT NULL,
    analysis_value NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (result_id, valuation_date), -- Include valuation_date in the primary key
    UNIQUE (sample_id, analysis_parameter, valuation_date), -- Ensure uniqueness for sample_id, analysis_parameter, valuation_date
    FOREIGN KEY (sample_id, valuation_date) REFERENCES raw_material.samples (sample_id, valuation_date)
) PARTITION BY RANGE (valuation_date);

-- Create partitions for analysis_results
CREATE TABLE raw_material.analysis_results_2015_2019 PARTITION OF raw_material.analysis_results
FOR VALUES FROM ('2015-01-01') TO ('2019-12-31');

CREATE TABLE raw_material.analysis_results_2020_2024 PARTITION OF raw_material.analysis_results
FOR VALUES FROM ('2020-01-01') TO ('2024-12-31');

CREATE TABLE raw_material.analysis_results_2025_2030 PARTITION OF raw_material.analysis_results
FOR VALUES FROM ('2025-01-01') TO ('2030-12-31');

-- Create raw_material.material_sources with sample_id as the foreign key
CREATE TABLE raw_material.material_sources (
    source_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    sample_id UUID NOT NULL,
    valuation_date DATE NOT NULL,
    plant_origin TEXT NOT NULL,
    producer TEXT NOT NULL,
    country TEXT NOT NULL,
    original_batch TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sample_id, valuation_date) REFERENCES raw_material.samples (sample_id, valuation_date)
);

-- Index creation

-- Indexes for raw_material.materials
CREATE INDEX idx_rm_materials_material_code ON raw_material.materials (material_code);

-- Indexes for raw_material.plants
CREATE INDEX idx_rm_plants_plant ON raw_material.plants (plant);

-- Indexes for raw_material.vendors
CREATE INDEX idx_rm_vendors_vendor_code ON raw_material.vendors (vendor_code);

-- Indexes for raw_material.material_sources
CREATE INDEX idx_rm_material_sources_sample_id ON raw_material.material_sources (sample_id);
CREATE INDEX idx_rm_material_sources_valuation_date ON raw_material.material_sources (valuation_date);

-- Indexes for samples partitions
CREATE INDEX idx_samples_2015_2019_sample_id ON raw_material.samples_2015_2019 (sample_id);
CREATE INDEX idx_samples_2015_2019_valuation_date ON raw_material.samples_2015_2019 (valuation_date);

CREATE INDEX idx_samples_2020_2024_sample_id ON raw_material.samples_2020_2024 (sample_id);
CREATE INDEX idx_samples_2020_2024_valuation_date ON raw_material.samples_2020_2024 (valuation_date);

CREATE INDEX idx_samples_2025_2030_sample_id ON raw_material.samples_2025_2030 (sample_id);
CREATE INDEX idx_samples_2025_2030_valuation_date ON raw_material.samples_2025_2030 (valuation_date);

-- Indexes for analysis_results partitions
CREATE INDEX idx_analysis_results_2015_2019_sample_id ON raw_material.analysis_results_2015_2019 (sample_id);
CREATE INDEX idx_analysis_results_2015_2019_valuation_date ON raw_material.analysis_results_2015_2019 (valuation_date);

CREATE INDEX idx_analysis_results_2020_2024_sample_id ON raw_material.analysis_results_2020_2024 (sample_id);
CREATE INDEX idx_analysis_results_2020_2024_valuation_date ON raw_material.analysis_results_2020_2024 (valuation_date);

CREATE INDEX idx_analysis_results_2025_2030_sample_id ON raw_material.analysis_results_2025_2030 (sample_id);
CREATE INDEX idx_analysis_results_2025_2030_valuation_date ON raw_material.analysis_results_2025_2030 (valuation_date);