-- DimTrain: Contains unique identifiers for lines/trips found in the XML files.
CREATE TABLE DimTrain (
    train_id VARCHAR(60) PRIMARY KEY,           -- PK: Composite ID (Category + Number, e.g., 'RE-73768')
    line_category VARCHAR(10) NOT NULL,         -- e.g., 'RE', 'RB', 'IC'
    train_number INTEGER NOT NULL,              -- e.g., 73768
    owner_id VARCHAR(20)                        -- Owner ID (o tag in XML)
);