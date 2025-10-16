-- =================================================================
-- Master Stored Procedure: dbo.usp_ExecuteFullPAFLOAD_ETL
-- Description:
-- This single, comprehensive stored procedure orchestrates the entire
-- ETL process for the PAF data load. It is designed to be the single
-- entry point for the daily data synchronization task.
--
-- It executes the following steps within a single transaction:
-- 1. Prepares the staging data by cleaning and hashing the raw data.
-- 2. Synchronizes the main production table (dbo.PAFLOAD) using MERGE.
-- 3. Synchronizes all seven dimension and fact sub-tables.
--
-- Any failure will result in a complete rollback and an error log entry.
-- =================================================================

PRINT 'Creating Master ETL Procedure [dbo].[usp_ExecuteFullPAFLOAD_ETL]...';
GO

IF OBJECT_ID('dbo.usp_ExecuteFullPAFLOAD_ETL', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_ExecuteFullPAFLOAD_ETL;
GO

CREATE PROCEDURE dbo.usp_ExecuteFullPAFLOAD_ETL
AS
BEGIN
    SET NOCOUNT ON;

    -- Wrap the entire multi-step process in a single transaction.
    BEGIN TRANSACTION;

    BEGIN TRY

        /**************************************************************************
         * STEP 1: PREPARE STAGING DATA
         * - Reads from the raw table (stg.PAFLOAD).
         * - Cleanses, calculates all necessary hashes, and derives columns.
         * - Populates the intermediate staging table (stg.PAFLOAD_WithHash).
         **************************************************************************/
        PRINT 'Step 1: Preparing staging data...';

        TRUNCATE TABLE stg.PAFLOAD_WithHash;

        INSERT INTO stg.PAFLOAD_WithHash (
            [Postcode], [PostTown], [DependantLocality], [DoubleDependantLocality],
            [ThoroughfareDescriptor], [DepThoroughfareDescriptor], [Buildingnumber], [BuildingName],
            [SubBuildingName], [POBox], [DepartmentName], [OrganisationName], [UDPRN],
            [PostCodeType], [SUOrgIndicator], [DelPointSuffix], [SCRAP1], [SCRAP2],
            [SCRAP3], [SCRAP4], [Outcode], [Incode], [Username], [CreatedDatetime],
            [RowHash], [LocalityKey], [ThoroughfareKey], [ThoroughfareDescriptorKey],
            [BuildingNameKey], [SubBuildingNameKey], [CompanyKey]
        )
        SELECT
            TRIM(Postcode), TRIM(PostTown), TRIM(DependantLocality), TRIM(DoubleDependantLocality),
            TRIM(ThoroughfareDescriptor), TRIM(DepThoroughfareDescriptor), TRIM(Buildingnumber), TRIM(BuildingName),
            TRIM(SubBuildingName), TRIM(POBox), TRIM(DepartmentName), TRIM(OrganisationName), TRIM(UDPRN),
            TRIM(PostCodeType), TRIM(SUOrgIndicator), TRIM(DelPointSuffix), TRIM(SCRAP1), TRIM(SCRAP2),
            TRIM(SCRAP3), TRIM(SCRAP4),
            CASE WHEN CHARINDEX(' ', TRIM(Postcode)) > 0 THEN LEFT(TRIM(Postcode), CHARINDEX(' ', TRIM(Postcode)) - 1) ELSE TRIM(Postcode) END,
            CASE WHEN CHARINDEX(' ', TRIM(Postcode)) > 0 THEN SUBSTRING(TRIM(Postcode), CHARINDEX(' ', TRIM(Postcode)) + 1, LEN(TRIM(Postcode))) ELSE NULL END,
            SUSER_SNAME(), GETDATE(),
            HASHBYTES('SHA2_256', CONCAT(ISNULL(TRIM(Postcode),''),'|',ISNULL(TRIM(PostTown),''),'|',ISNULL(TRIM(DependantLocality),''),'|',ISNULL(TRIM(DoubleDependantLocality),''),'|',ISNULL(TRIM(ThoroughfareDescriptor),''),'|',ISNULL(TRIM(DepThoroughfareDescriptor),''),'|',ISNULL(TRIM(Buildingnumber),''),'|',ISNULL(TRIM(BuildingName),''),'|',ISNULL(TRIM(SubBuildingName),''),'|',ISNULL(TRIM(POBox),''),'|',ISNULL(TRIM(DepartmentName),''),'|',ISNULL(TRIM(OrganisationName),''),'|',ISNULL(TRIM(UDPRN),''),'|',ISNULL(TRIM(PostCodeType),''),'|',ISNULL(TRIM(SUOrgIndicator),''),'|',ISNULL(TRIM(DelPointSuffix),''),'|',ISNULL(TRIM(SCRAP1),''),'|',ISNULL(TRIM(SCRAP2),''),'|',ISNULL(TRIM(SCRAP3),''),'|',ISNULL(TRIM(SCRAP4),''))),
            HASHBYTES('SHA2_256', CONCAT(ISNULL(TRIM(PostTown), 'NV'), '|', ISNULL(TRIM(DependantLocality), 'NV'), '|', ISNULL(TRIM(DoubleDependantLocality), 'NV'))),
            HASHBYTES('SHA2_256', ISNULL(TRIM(ThoroughfareDescriptor), 'NV')),
            HASHBYTES('SHA2_256', ISNULL(TRIM(DepThoroughfareDescriptor), 'NV')),
            HASHBYTES('SHA2_256', ISNULL(TRIM(BuildingName), 'NV')),
            HASHBYTES('SHA2_256', ISNULL(TRIM(SubBuildingName), 'NV')),
            HASHBYTES('SHA2_256', CONCAT(ISNULL(TRIM(OrganisationName), 'NV'), '|', ISNULL(TRIM(DepartmentName), 'NV'), '|', ISNULL(TRIM(PostCodeType), 'NV')))
        FROM stg.PAFLOAD;

        PRINT 'Step 1 complete.';

        /**************************************************************************
         * STEP 2: SYNCHRONIZE MAIN PRODUCTION TABLE (dbo.PAFLOAD)
         * - Merges data from stg.PAFLOAD_WithHash into dbo.PAFLOAD.
         * - Captures all changes (Inserts, Updates, Deletes) into the
         * dbo.PAFLOAD_ChangeLog table.
         **************************************************************************/
        PRINT 'Step 2: Synchronizing main production table [dbo].[PAFLOAD]...';

        DECLARE @Changes TABLE(ChangeType NVARCHAR(20), UDPRN NVARCHAR(20), OldRowHash BINARY(32), NewRowHash BINARY(32));

        MERGE dbo.PAFLOAD AS Target
        USING stg.PAFLOAD_WithHash AS Source ON (Target.UDPRN = Source.UDPRN)
        WHEN MATCHED AND Target.RowHash <> Source.RowHash THEN
            UPDATE SET Target.Postcode=Source.Postcode, Target.PostTown=Source.PostTown, Target.DependantLocality=Source.DependantLocality, Target.DoubleDependantLocality=Source.DoubleDependantLocality, Target.ThoroughfareDescriptor=Source.ThoroughfareDescriptor, Target.DepThoroughfareDescriptor=Source.DepThoroughfareDescriptor, Target.Buildingnumber=Source.Buildingnumber, Target.BuildingName=Source.BuildingName, Target.SubBuildingName=Source.SubBuildingName, Target.POBox=Source.POBox, Target.DepartmentName=Source.DepartmentName, Target.OrganisationName=Source.OrganisationName, Target.PostCodeType=Source.PostCodeType, Target.SUOrgIndicator=Source.SUOrgIndicator, Target.DelPointSuffix=Source.DelPointSuffix, Target.SCRAP1=Source.SCRAP1, Target.SCRAP2=Source.SCRAP2, Target.SCRAP3=Source.SCRAP3, Target.SCRAP4=Source.SCRAP4, Target.Outcode=Source.Outcode, Target.Incode=Source.Incode, Target.Username=Source.Username, Target.CreatedDatetime=Source.CreatedDatetime, Target.RowHash=Source.RowHash, Target.LocalityKey=Source.LocalityKey, Target.ThoroughfareKey=Source.ThoroughfareKey, Target.ThoroughfareDescriptorKey=Source.ThoroughfareDescriptorKey, Target.BuildingNameKey=Source.BuildingNameKey, Target.SubBuildingNameKey=Source.SubBuildingNameKey, Target.CompanyKey=Source.CompanyKey
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Postcode, PostTown, DependantLocality, DoubleDependantLocality, ThoroughfareDescriptor, DepThoroughfareDescriptor, Buildingnumber, BuildingName, SubBuildingName, POBox, DepartmentName, OrganisationName, UDPRN, PostCodeType, SUOrgIndicator, DelPointSuffix, SCRAP1, SCRAP2, SCRAP3, SCRAP4, Outcode, Incode, Username, CreatedDatetime, RowHash, LocalityKey, ThoroughfareKey, ThoroughfareDescriptorKey, BuildingNameKey, SubBuildingNameKey, CompanyKey)
            VALUES (Source.Postcode, Source.PostTown, Source.DependantLocality, Source.DoubleDependantLocality, Source.ThoroughfareDescriptor, Source.DepThoroughfareDescriptor, Source.Buildingnumber, Source.BuildingName, Source.SubBuildingName, Source.POBox, Source.DepartmentName, Source.OrganisationName, Source.UDPRN, Source.PostCodeType, Source.SUOrgIndicator, Source.DelPointSuffix, Source.SCRAP1, Source.SCRAP2, Source.SCRAP3, Source.SCRAP4, Source.Outcode, Source.Incode, Source.Username, Source.CreatedDatetime, Source.RowHash, Source.LocalityKey, Source.ThoroughfareKey, Source.ThoroughfareDescriptorKey, Source.BuildingNameKey, Source.SubBuildingNameKey, Source.CompanyKey)
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE
        OUTPUT $action, COALESCE(inserted.UDPRN, deleted.UDPRN), deleted.RowHash, inserted.RowHash INTO @Changes;

        INSERT INTO dbo.PAFLOAD_ChangeLog (ChangeType, ChangeTimestamp, ExecutedBy, UDPRN, OldRowHash, NewRowHash)
        SELECT ChangeType, GETDATE(), SUSER_SNAME(), UDPRN, OldRowHash, NewRowHash FROM @Changes;

        PRINT 'Step 2 complete.';

        /**************************************************************************
         * STEP 3: SYNCHRONIZE ALL SUB-TABLES
         * - Merges data into all six dimension tables.
         * - Truncates and reloads the final fact table.
         **************************************************************************/
        PRINT 'Step 3: Synchronizing all sub-tables...';

        -- Sync Dimension Tables
        MERGE dbo.tblRMLocality AS T USING (SELECT DISTINCT LocalityKey, PostTown, DependantLocality, DoubleDependantLocality FROM stg.PAFLOAD_WithHash) AS S ON (T.LocalityKey=S.LocalityKey) WHEN NOT MATCHED BY TARGET THEN INSERT (LocalityKey,PostTown,DependantLocality,DoubleDependantLocality,Username) VALUES (S.LocalityKey,S.PostTown,S.DependantLocality,S.DoubleDependantLocality,SUSER_SNAME()) WHEN NOT MATCHED BY SOURCE THEN DELETE;
        MERGE dbo.tblRMThoroughfare AS T USING (SELECT DISTINCT ThoroughfareKey, ThoroughfareDescriptor FROM stg.PAFLOAD_WithHash WHERE ThoroughfareDescriptor IS NOT NULL) AS S ON (T.ThoroughfareKey=S.ThoroughfareKey) WHEN NOT MATCHED BY TARGET THEN INSERT (ThoroughfareKey,Descriptor,Username) VALUES (S.ThoroughfareKey,S.ThoroughfareDescriptor,SUSER_SNAME()) WHEN NOT MATCHED BY SOURCE THEN DELETE;
        MERGE dbo.tblRMThoroughfareDescriptor AS T USING (SELECT DISTINCT ThoroughfareDescriptorKey, DepThoroughfareDescriptor FROM stg.PAFLOAD_WithHash WHERE DepThoroughfareDescriptor IS NOT NULL) AS S ON (T.ThoroughfareKey=S.ThoroughfareDescriptorKey) WHEN NOT MATCHED BY TARGET THEN INSERT (ThoroughfareKey,Descriptor,Username) VALUES (S.ThoroughfareDescriptorKey,S.DepThoroughfareDescriptor,SUSER_SNAME()) WHEN NOT MATCHED BY SOURCE THEN DELETE;
        MERGE dbo.tblRMBuildingName AS T USING (SELECT DISTINCT BuildingNameKey, BuildingName FROM stg.PAFLOAD_WithHash WHERE BuildingName IS NOT NULL) AS S ON (T.BuildingKey=S.BuildingNameKey) WHEN NOT MATCHED BY TARGET THEN INSERT (BuildingKey,Descriptor,Username) VALUES (S.BuildingNameKey,S.BuildingName,SUSER_SNAME()) WHEN NOT MATCHED BY SOURCE THEN DELETE;
        MERGE dbo.tblRMSubBuildingName AS T USING (SELECT DISTINCT SubBuildingNameKey, SubBuildingName FROM stg.PAFLOAD_WithHash WHERE SubBuildingName IS NOT NULL) AS S ON (T.SubBuildingKey=S.SubBuildingNameKey) WHEN NOT MATCHED BY TARGET THEN INSERT (SubBuildingKey,Descriptor,Username) VALUES (S.SubBuildingKey,S.SubBuildingName,SUSER_SNAME()) WHEN NOT MATCHED BY SOURCE THEN DELETE;
        MERGE dbo.tblRMCompany AS T USING (SELECT DISTINCT CompanyKey, OrganisationName, DepartmentName, PostCodeType FROM stg.PAFLOAD_WithHash WHERE OrganisationName IS NOT NULL) AS S ON (T.CompanyKey=S.CompanyKey) WHEN NOT MATCHED BY TARGET THEN INSERT (CompanyKey,Descriptor,Department,Type,Username) VALUES (S.CompanyKey,S.OrganisationName,S.DepartmentName,S.PostCodeType,SUSER_SNAME()) WHEN NOT MATCHED BY SOURCE THEN DELETE;

        -- Sync Final Fact Table
        TRUNCATE TABLE dbo.tblRMAddresses;
        INSERT INTO dbo.tblRMAddresses (AddressKey, Postcode, LocalityKey, ThoroughfareKey, ThoroughfareDescriptorKey, DepThoroughfareDescriptorKey, BuildingNameKey, SubBuildingNameKey, OrganisationKey, PostCodeType, DelPointSuffix, POBox, OutCode, InCode, Username)
        SELECT HASHBYTES('SHA2_256', UDPRN), Postcode, LocalityKey, ThoroughfareKey, ThoroughfareDescriptorKey, ThoroughfareDescriptorKey, BuildingNameKey, SubBuildingNameKey, CompanyKey, PostCodeType, DelPointSuffix, POBox, Outcode, Incode, SUSER_SNAME() FROM dbo.PAFLOAD;

        PRINT 'Step 3 complete.';

        -- If all steps were successful, commit the transaction.
        COMMIT TRANSACTION;
        PRINT 'ETL process completed successfully.';

    END TRY
    BEGIN CATCH
        -- If any error occurs, roll back the entire transaction.
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        PRINT 'An error occurred during the ETL process. Transaction has been rolled back.';

        -- Log the detailed error information.
        INSERT INTO dbo.ErrorLog (UserName, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
        VALUES (SUSER_SNAME(), ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE());

        -- Re-throw the error to notify the calling process of the failure.
        THROW;
    END CATCH
END
GO

PRINT '[dbo].[usp_ExecuteFullPAFLOAD_ETL] has been successfully created.';
GO
