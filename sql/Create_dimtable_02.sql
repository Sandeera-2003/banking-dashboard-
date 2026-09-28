USE DWBI_Bank;
GO

CREATE TABLE dw.dim_channel (channel_key TINYINT IDENTITY(1,1) PRIMARY KEY, contact_type VARCHAR(20) NOT NULL UNIQUE);
INSERT INTO dw.dim_channel (contact_type) SELECT DISTINCT contact FROM stage.marketing_call_center;
GO

CREATE TABLE dw.dim_weekday (weekday_key TINYINT PRIMARY KEY, day_abbr CHAR(3) NOT NULL UNIQUE, weekday_name VARCHAR(10) NOT NULL);
INSERT INTO dw.dim_weekday (weekday_key, day_abbr, weekday_name) VALUES (1,'mon','Monday'),(2,'tue','Tuesday'),(3,'wed','Wednesday'),(4,'thu','Thursday'),(5,'fri','Friday');
GO

CREATE TABLE dw.dim_previous_outcome (outcome_key TINYINT IDENTITY(1,1) PRIMARY KEY, previous_outcome VARCHAR(20) NOT NULL UNIQUE);
INSERT INTO dw.dim_previous_outcome (previous_outcome) SELECT DISTINCT poutcome FROM stage.marketing_call_center;
GO

SELECT (SELECT COUNT(*) FROM DWBI_Bank.dw.dim_channel) AS channels, (SELECT COUNT(*) FROM DWBI_Bank.dw.dim_weekday) AS weekdays, (SELECT COUNT(*) FROM DWBI_Bank.dw.dim_previous_outcome) AS previous_outcomes;