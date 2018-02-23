@isTest(seeAllData=false)
private class PopulateGuidDdpPdfStampTest {
    public static List<Loop__DDP__c> ddps;
    public static List<Loop__PDF_Stamp__c> pdfstamp;

    static void init() {
        ddps = new List<Loop__DDP__c>();
        ddps.add(new Loop__DDP__c());
        pdfstamp = new List<Loop__PDF_Stamp__c>();
        pdfstamp.add(new Loop__PDF_Stamp__c());
    }

    // GUID should be generated if it is set to NULL
    @isTest static void testAddNewRecordEmptyDdpMigratorId() {
        init();
        Test.startTest();

        insert ddps;

        ddps = [SELECT Id FROM Loop__DDP__c];
        pdfstamp[0].Loop__DDP__c = ddps[0].Id;

        insert pdfstamp;

        pdfstamp = [SELECT Id, DDP_Migrator_Id__c FROM Loop__PDF_Stamp__c];

        System.assertEquals(1, pdfstamp.size());
        System.assert(pdfstamp[0].DDP_Migrator_Id__c != null, 'DDP_Migrator_Id__c field was not populated');

        Test.stopTest();
    }

    // GUID should not change if it is set and there are no duplicate records
    @isTest static void testAddNewRecordWithDdpMigratorId() {
        init();
        Test.startTest();

        // Create DDP
        insert ddps;

        // Retrieve Id of the DDP
        ddps = [SELECT Id FROM Loop__DDP__c];

        // Map file to the DDP
        pdfstamp[0].Loop__DDP__c = ddps[0].Id;
        String guid = GUIDGenerator.generateGUID();
        pdfstamp[0].DDP_Migrator_Id__c = guid;

        insert pdfstamp;

        pdfstamp = [SELECT Id, DDP_Migrator_Id__c FROM Loop__PDF_Stamp__c];

        System.assertEquals(1, pdfstamp.size());
        System.assertEquals(guid, pdfstamp[0].DDP_Migrator_Id__c);

        Test.stopTest();
    }

    // Trigger should generate new GUID for cloned/duplicate record
    @isTest static void testCloneRecord() {
        init();
        Test.startTest();

        // Create DDP
        insert ddps;

        // Retrieve Id of the DDP
        ddps = [SELECT Id FROM Loop__DDP__c];

        // Map file to the DDP
        pdfstamp[0].Loop__DDP__c = ddps[0].Id;
        String guid = GUIDGenerator.generateGUID();
        pdfstamp[0].DDP_Migrator_Id__c = guid;

        insert pdfstamp;

        pdfstamp = [SELECT Id, DDP_Migrator_Id__c FROM Loop__PDF_Stamp__c];

        // Create duplicate/clone file record
        pdfstamp.add(new Loop__PDF_Stamp__c());
        pdfstamp[1].Loop__DDP__c = ddps[0].Id;
        pdfstamp[1].DDP_Migrator_Id__c = guid;

        insert pdfstamp[1];

        // Retrieve second file only
        pdfstamp = [SELECT Id, DDP_Migrator_Id__c FROM Loop__PDF_Stamp__c WHERE Id != :pdfstamp[0].Id];

        System.assertEquals(1, pdfstamp.size());
        System.assertNotEquals(guid, pdfstamp[0].DDP_Migrator_Id__c);

        Test.stopTest();
    }
}