USE master;
GO

IF DB_ID('OpenFoodDB') IS NULL
    CREATE DATABASE OpenFoodDB;
GO


USE OpenFoodDB;
GO

IF SCHEMA_ID('raw') IS NULL
    EXEC('CREATE SCHEMA raw');
GO

IF SCHEMA_ID('ml') IS NULL
    EXEC('CREATE SCHEMA ml');
GO


IF OBJECT_ID('raw.processed_data', 'U') IS NULL
BEGIN
    CREATE TABLE raw.processed_data
    (
        product_id         INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        code               NVARCHAR(64) NOT NULL,
        energy_kcal_100g   FLOAT NULL,
        fat_100g           FLOAT NULL,
        saturated_fat_100g FLOAT NULL,
        carbohydrates_100g FLOAT NULL,
        sugars_100g        FLOAT NULL,
        proteins_100g      FLOAT NULL,
        salt_100g          FLOAT NULL
    );
END;
GO


IF OBJECT_ID('ml.model_runs', 'U') IS NULL
BEGIN
    CREATE TABLE ml.model_runs (
        run_id                  INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        command                 NVARCHAR(16) NOT NULL,
        status                  NVARCHAR(16) NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
        started_at              DATETIME2(3) NOT NULL,
        finished_at             DATETIME2(3) NULL,
        rows_in                 INT NULL,
        best_k                  INT NULL,
        best_silhouette         FLOAT NULL,
        params                  NVARCHAR(MAX) NULL CHECK (params IS NULL OR ISJSON(params) = 1),
        scaler_path             NVARCHAR(500) NOT NULL,
        error_message           NVARCHAR(1000) NULL
    );
END;
GO

IF OBJECT_ID('ml.predictions', 'U') IS NULL
BEGIN
    CREATE TABLE ml.predictions (
        run_id     INT NOT NULL,
        code       NVARCHAR(64) NOT NULL,
        cluster_id INT NOT NULL,

        CONSTRAINT PK_predictions PRIMARY KEY CLUSTERED (run_id, code),
        CONSTRAINT FK_predictions_run
        FOREIGN KEY (run_id) REFERENCES ml.model_runs (run_id)
        ON DELETE CASCADE
    );
END;
GO
