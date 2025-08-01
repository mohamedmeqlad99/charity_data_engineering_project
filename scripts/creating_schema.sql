-- fact_donations: Central fact table with metrics and foreign keys
CREATE TABLE fact_donations (
    donation_id NVARCHAR(36) NOT NULL,
    donor_id NVARCHAR(36),
    project_id NVARCHAR(36),
    campaign_id NVARCHAR(36),
    donation_date DATE,
    payment_provider_id NVARCHAR(50),
    region_id NVARCHAR(50),
    volunteer_id NVARCHAR(36),
    beneficiary_id NVARCHAR(36),
    amount FLOAT,
    transaction_status NVARCHAR(50),
    high_value_likelihood FLOAT,
    CONSTRAINT PK_fact_donations PRIMARY KEY (donation_id),
    CONSTRAINT FK_donations_donor FOREIGN KEY (donor_id) REFERENCES dim_donor(donor_id),
    CONSTRAINT FK_donations_project FOREIGN KEY (project_id) REFERENCES dim_project(project_id),
    CONSTRAINT FK_donations_campaign FOREIGN KEY (campaign_id) REFERENCES dim_campaign(campaign_id),
    CONSTRAINT FK_donations_date FOREIGN KEY (donation_date) REFERENCES dim_date(date),
    CONSTRAINT FK_donations_payment_provider FOREIGN KEY (payment_provider_id) REFERENCES dim_payment_provider(payment_provider_id),
    CONSTRAINT FK_donations_region FOREIGN KEY (region_id) REFERENCES dim_region(region_id),
    CONSTRAINT FK_donations_volunteer FOREIGN KEY (volunteer_id) REFERENCES dim_volunteer(volunteer_id),
    CONSTRAINT FK_donations_beneficiary FOREIGN KEY (beneficiary_id) REFERENCES dim_beneficiary(beneficiary_id)
) WITH (
    DISTRIBUTION = HASH(donation_id),
    CLUSTERED COLUMNSTORE INDEX
);

-- dim_date: Stores date-related attributes for time-based analysis
CREATE TABLE dim_date (
    date DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    month_name VARCHAR(20),
    quarter INT,
    CONSTRAINT PK_dim_date PRIMARY KEY (date)
) WITH (
    DISTRIBUTION = REPLICATE,
    HEAP
);

-- dim_payment_provider: Stores payment provider details
CREATE TABLE dim_payment_provider (
    payment_provider_id NVARCHAR(50) NOT NULL,
    provider_name NVARCHAR(50),
    success_rate FLOAT,
    CONSTRAINT PK_dim_payment_provider PRIMARY KEY (payment_provider_id)
) WITH (
    DISTRIBUTION = REPLICATE,
    HEAP
);

-- dim_region: Stores region details
CREATE TABLE dim_region (
    region_id NVARCHAR(50) NOT NULL,
    region_name NVARCHAR(50),
    CONSTRAINT PK_dim_region PRIMARY KEY (region_id)
) WITH (
    DISTRIBUTION = REPLICATE,
    HEAP
);