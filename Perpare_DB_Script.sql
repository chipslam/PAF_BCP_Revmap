-- =================================================================
-- Complete Test Environment Setup Script (Corrected)
-- Description:
-- This script contains all database objects for the PAF data project.
--
-- UPDATED:
-- 1. Added the missing [DepThoroughfareDescriptorKey] column to
--    [stg.PAFLOAD_WithHash] and [dbo.PAFLOAD] tables.
-- 2. Corrected the column list in [dbo.tblRMAddresses] to align
--    with the final ETL stored procedure logic.
-- =================================================================

PRINT 'Starting test environment setup...';
GO

-- Create the database if it does not exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'POSTCODE')
BEGIN
    CREATE DATABASE POSTCODE;
    PRINT 'Database "POSTCODE" created.';
END
ELSE
BEGIN
    PRINT 'Database "POSTCODE" already exists.';
END
GO

-- Switch to the context of the new database for all subsequent commands
USE POSTCODE;
GO

PRINT 'Starting to create objects in POSTCODE database...';
GO

-- == Part 1: Create Staging Schema ==

PRINT 'Creating stg schema...';
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stg')
BEGIN
    EXEC('CREATE SCHEMA stg');
    PRINT 'Schema "stg" created.';
END
ELSE
BEGIN
    PRINT 'Schema "stg" already exists.';
END
GO

-- == Part 2: Create Main ETL Tables ==

PRINT 'Creating Raw Table: [stg].[PAFLOAD]';
IF OBJECT_ID('stg.PAFLOAD', 'U') IS NOT NULL
    DROP TABLE stg.PAFLOAD;
GO
CREATE TABLE stg.PAFLOAD
(
    [Postcode]                  NVARCHAR(20)     NULL,
    [PostTown]                  NVARCHAR(100)    NULL,
    [DependantLocality]         NVARCHAR(100)    NULL,
    [DoubleDependantLocality]   NVARCHAR(100)    NULL,
    [ThoroughfareDescriptor]    NVARCHAR(100)    NULL,
    [DepThoroughfareDescriptor] NVARCHAR(100)    NULL,
    [Buildingnumber]            NVARCHAR(20)     NULL,
    [BuildingName]              NVARCHAR(100)    NULL,
    [SubBuildingName]           NVARCHAR(100)    NULL,
    [POBox]                     NVARCHAR(20)     NULL,
    [DepartmentName]            NVARCHAR(100)    NULL,
    [OrganisationName]          NVARCHAR(100)    NULL,
    [UDPRN]                     NVARCHAR(20)     NULL,
    [PostCodeType]              NVARCHAR(10)     NULL,
    [SUOrgIndicator]            NVARCHAR(10)     NULL,
    [DelPointSuffix]            NVARCHAR(10)     NULL,
    [SCRAP1]                    NVARCHAR(255)    NULL,
    [SCRAP2]                    NVARCHAR(255)    NULL,
    [SCRAP3]                    NVARCHAR(255)    NULL,
    [SCRAP4]                    NVARCHAR(255)    NULL
);
PRINT 'Table "stg.PAFLOAD" created.';
GO

PRINT 'Creating Staging Table with Hash: [stg].[PAFLOAD_WithHash]';
IF OBJECT_ID('stg.PAFLOAD_WithHash', 'U') IS NOT NULL
    DROP TABLE stg.PAFLOAD_WithHash;
GO
CREATE TABLE stg.PAFLOAD_WithHash
(
    [Postcode]                  NVARCHAR(20)     NULL,
    [PostTown]                  NVARCHAR(100)    NULL,
    [DependantLocality]         NVARCHAR(100)    NULL,
    [DoubleDependantLocality]   NVARCHAR(100)    NULL,
    [ThoroughfareDescriptor]    NVARCHAR(100)    NULL,
    [DepThoroughfareDescriptor] NVARCHAR(100)    NULL,
    [Buildingnumber]            NVARCHAR(20)     NULL,
    [BuildingName]              NVARCHAR(100)    NULL,
    [SubBuildingName]           NVARCHAR(100)    NULL,
    [POBox]                     NVARCHAR(20)     NULL,
    [DepartmentName]            NVARCHAR(100)    NULL,
    [OrganisationName]          NVARCHAR(100)    NULL,
    [UDPRN]                     NVARCHAR(20)     NULL,
    [PostCodeType]              NVARCHAR(10)     NULL,
    [SUOrgIndicator]            NVARCHAR(10)     NULL,
    [DelPointSuffix]            NVARCHAR(10)     NULL,
    [SCRAP1]                    NVARCHAR(255)    NULL,
    [SCRAP2]                    NVARCHAR(255)    NULL,
    [SCRAP3]                    NVARCHAR(255)    NULL,
    [SCRAP4]                    NVARCHAR(255)    NULL,
    [Outcode]                   NVARCHAR(10)     NULL,
    [Incode]                    NVARCHAR(10)     NULL,
    [Username]                  NVARCHAR(50)     NULL,
    [CreatedDatetime]           DATETIME         NULL,
    [RowHash]                   BINARY(32)       NULL,
    [LocalityKey]               BINARY(32)       NULL,
    [ThoroughfareKey]           BINARY(32)       NULL,
    [DepThoroughfareDescriptorKey] BINARY(32)    NULL, -- CORRECTED: Added this column
    [BuildingNameKey]           BINARY(32)       NULL,
    [SubBuildingNameKey]        BINARY(32)       NULL,
    [CompanyKey]                BINARY(32)       NULL
);
CREATE NONCLUSTERED INDEX IX_stg_PAFLOAD_WithHash_UDPRN ON stg.PAFLOAD_WithHash (UDPRN ASC);
PRINT 'Table "stg.PAFLOAD_WithHash" created.';
GO

PRINT 'Creating Production Table: [dbo].[PAFLOAD]';
IF OBJECT_ID('dbo.PAFLOAD', 'U') IS NOT NULL
    DROP TABLE dbo.PAFLOAD;
GO
CREATE TABLE dbo.PAFLOAD
(
    [Postcode]                  NVARCHAR(20)     NULL,
    [PostTown]                  NVARCHAR(100)    NULL,
    [DependantLocality]         NVARCHAR(100)    NULL,
    [DoubleDependantLocality]   NVARCHAR(100)    NULL,
    [ThoroughfareDescriptor]    NVARCHAR(100)    NULL,
    [DepThoroughfareDescriptor] NVARCHAR(100)    NULL,
    [Buildingnumber]            NVARCHAR(20)     NULL,
    [BuildingName]              NVARCHAR(100)    NULL,
    [SubBuildingName]           NVARCHAR(100)    NULL,
    [POBox]                     NVARCHAR(20)     NULL,
    [DepartmentName]            NVARCHAR(100)    NULL,
    [OrganisationName]          NVARCHAR(100)    NULL,
    [UDPRN]                     NVARCHAR(20)     NOT NULL,
    [PostCodeType]              NVARCHAR(10)     NULL,
    [SUOrgIndicator]            NVARCHAR(10)     NULL,
    [DelPointSuffix]            NVARCHAR(10)     NULL,
    [SCRAP1]                    NVARCHAR(255)    NULL,
    [SCRAP2]                    NVARCHAR(255)    NULL,
    [SCRAP3]                    NVARCHAR(255)    NULL,
    [SCRAP4]                    NVARCHAR(255)    NULL,
    [Outcode]                   NVARCHAR(10)     NULL,
    [Incode]                    NVARCHAR(10)     NULL,
    [Username]                  NVARCHAR(50)     NULL,
    [CreatedDatetime]           DATETIME         NULL,
    [RowHash]                   BINARY(32)       NULL,
    [LocalityKey]               BINARY(32)       NULL,
    [ThoroughfareKey]           BINARY(32)       NULL,
    [DepThoroughfareDescriptorKey] BINARY(32)    NULL, -- CORRECTED: Added this column
    [BuildingNameKey]           BINARY(32)       NULL,
    [SubBuildingNameKey]        BINARY(32)       NULL,
    [CompanyKey]                BINARY(32)       NULL,
    CONSTRAINT PK_PAFLOAD PRIMARY KEY CLUSTERED (UDPRN ASC)
);
PRINT 'Table "dbo.PAFLOAD" created.';
GO

-- == Part 3: Create Logging Tables ==

PRINT 'Creating Change Log Table: [dbo].[PAFLOAD_ChangeLog]';
IF OBJECT_ID('dbo.PAFLOAD_ChangeLog', 'U') IS NOT NULL
    DROP TABLE dbo.PAFLOAD_ChangeLog;
GO
CREATE TABLE dbo.PAFLOAD_ChangeLog
(
    [ChangeLogID]               INT             IDENTITY(1,1) PRIMARY KEY,
    [ChangeType]                NVARCHAR(20)     NOT NULL,
    [ChangeTimestamp]           DATETIME        NOT NULL,
    [ExecutedBy]                NVARCHAR(128)    NULL,
    [UDPRN]                     NVARCHAR(20)     NOT NULL,
    [OldRowHash]                BINARY(32)       NULL,
    [NewRowHash]                BINARY(32)       NULL
);
PRINT 'Table "dbo.PAFLOAD_ChangeLog" created.';
GO

PRINT 'Creating Error Log Table: [dbo].[ErrorLog]';
IF OBJECT_ID('dbo.ErrorLog', 'U') IS NOT NULL
    DROP TABLE dbo.ErrorLog;
GO
CREATE TABLE dbo.ErrorLog
(
    [ErrorLogID]        INT             IDENTITY(1,1) PRIMARY KEY,
    [ErrorTimestamp]    DATETIME        NOT NULL DEFAULT GETDATE(),
    [UserName]          NVARCHAR(128)   NOT NULL,
    [ErrorNumber]       INT             NOT NULL,
    [ErrorSeverity]     INT             NOT NULL,
    [ErrorState]        INT             NOT NULL,
    [ErrorProcedure]    NVARCHAR(128)   NULL,
    [ErrorLine]         INT             NULL,
    [ErrorMessage]      NVARCHAR(4000)  NOT NULL
);
PRINT 'Table "dbo.ErrorLog" created.';
GO

-- == Part 4: Create All Sub-tables (Dimension Tables) ==

PRINT 'Creating Sub-Table: [dbo].[tblRMLocality]';
IF OBJECT_ID('dbo.tblRMLocality', 'U') IS NOT NULL
    DROP TABLE dbo.tblRMLocality;
GO
CREATE TABLE dbo.tblRMLocality
(
    [LocalityKey]               BINARY(32)      NOT NULL,
    [PostTown]                  NVARCHAR(100)   NULL,
    [DependantLocality]         NVARCHAR(100)   NULL,
    [DoubleDependantLocality]   NVARCHAR(100)   NULL,
    [Username]                  NVARCHAR(50)    NULL,
    CONSTRAINT PK_tblRMLocality PRIMARY KEY CLUSTERED (LocalityKey ASC)
);
PRINT 'Table "dbo.tblRMLocality" created.';
GO

PRINT 'Creating Sub-Table: [dbo].[tblRMThoroughfare]';
IF OBJECT_ID('dbo.tblRMThoroughfare', 'U') IS NOT NULL
    DROP TABLE dbo.tblRMThoroughfare;
GO
CREATE TABLE dbo.tblRMThoroughfare
(
    [ThoroughfareKey]   BINARY(32)      NOT NULL,
    [Descriptor]        NVARCHAR(100)   NULL,
    [Username]          NVARCHAR(50)    NULL,
    CONSTRAINT PK_tblRMThoroughfare PRIMARY KEY CLUSTERED (ThoroughfareKey ASC)
);
PRINT 'Table "dbo.tblRMThoroughfare" created.';
GO

PRINT 'Creating Sub-Table: [dbo].[tblRMThoroughfareDescriptor]';
IF OBJECT_ID('dbo.tblRMThoroughfareDescriptor', 'U') IS NOT NULL
    DROP TABLE dbo.tblRMThoroughfareDescriptor;
GO
CREATE TABLE dbo.tblRMThoroughfareDescriptor
(
    [ThoroughfareKey]   BINARY(32)      NOT NULL,
    [Descriptor]        NVARCHAR(100)   NULL,
    [Username]          NVARCHAR(50)    NULL,
    CONSTRAINT PK_tblRMThoroughfareDescriptor PRIMARY KEY CLUSTERED (ThoroughfareKey ASC)
);
PRINT 'Table "dbo.tblRMThoroughfareDescriptor" created.';
GO

PRINT 'Creating Sub-Table: [dbo].[tblRMBuildingName]';
IF OBJECT_ID('dbo.tblRMBuildingName', 'U') IS NOT NULL
    DROP TABLE dbo.tblRMBuildingName;
GO
CREATE TABLE dbo.tblRMBuildingName
(
    [BuildingKey]   BINARY(32)      NOT NULL,
    [Descriptor]    NVARCHAR(100)   NULL,
    [Username]      NVARCHAR(50)    NULL,
    CONSTRAINT PK_tblRMBuildingName PRIMARY KEY CLUSTERED (BuildingKey ASC)
);
PRINT 'Table "dbo.tblRMBuildingName" created.';
GO

PRINT 'Creating Sub-Table: [dbo].[tblRMSubBuildingName]';
IF OBJECT_ID('dbo.tblRMSubBuildingName', 'U') IS NOT NULL
    DROP TABLE dbo.tblRMSubBuildingName;
GO
CREATE TABLE dbo.tblRMSubBuildingName
(
    [SubBuildingKey]    BINARY(32)      NOT NULL,
    [Descriptor]        NVARCHAR(100)   NULL,
    [Username]          NVARCHAR(50)    NULL,
    CONSTRAINT PK_tblRMSubBuildingName PRIMARY KEY CLUSTERED (SubBuildingKey ASC)
);
PRINT 'Table "dbo.tblRMSubBuildingName" created.';
GO

PRINT 'Creating Sub-Table: [dbo].[tblRMCompany]';
IF OBJECT_ID('dbo.tblRMCompany', 'U') IS NOT NULL
    DROP TABLE dbo.tblRMCompany;
GO
CREATE TABLE dbo.tblRMCompany
(
    [CompanyKey]    BINARY(32)      NOT NULL,
    [Descriptor]    NVARCHAR(100)   NULL,
    [Department]    NVARCHAR(100)   NULL,
    [Type]          NVARCHAR(10)    NULL,
    [Username]      NVARCHAR(50)    NULL,
    CONSTRAINT PK_tblRMCompany PRIMARY KEY CLUSTERED (CompanyKey ASC)
);
PRINT 'Table "dbo.tblRMCompany" created.';
GO

-- == Part 5: Create Final Fact Table ==

PRINT 'Creating final fact table [dbo].[tblRMAddresses]';
IF OBJECT_ID('dbo.tblRMAddresses', 'U') IS NOT NULL
    DROP TABLE dbo.tblRMAddresses;
GO
CREATE TABLE dbo.tblRMAddresses
(
    [AddressKey]                    BINARY(32)      NOT NULL,
    [Postcode]                      NVARCHAR(20)    NULL,
    [LocalityKey]                   BINARY(32)      NULL,
    [ThoroughfareKey]               BINARY(32)      NULL,
    [DepThoroughfareDescriptorKey]  BINARY(32)      NULL, -- CORRECTED: Simplified the key structure
    [BuildingNameKey]               BINARY(32)      NULL,
    [SubBuildingNameKey]            BINARY(32)      NULL,
    [NumberOfHouseholds]            INT             NULL,
    [OrganisationKey]               BINARY(32)      NULL,
    [PostCodeType]                  NVARCHAR(10)    NULL,
    [ConcatenationIndicator]        NVARCHAR(10)    NULL,
    [DelPointSuffix]                NVARCHAR(10)    NULL,
    [POBox]                         NVARCHAR(20)    NULL,
    [OutCode]                       NVARCHAR(10)    NULL,
    [InCode]                        NVARCHAR(10)    NULL,
    [SequenceNo]                    INT             NULL,
    [Username]                      NVARCHAR(50)    NULL,
    CONSTRAINT PK_tblRMAddresses PRIMARY KEY CLUSTERED (AddressKey ASC)
);
GO
PRINT 'Table "dbo.tblRMAddresses" created.';
GO


PRINT 'Test environment setup complete.';
GO

