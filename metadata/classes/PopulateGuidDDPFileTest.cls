@isTest(seeAllData=false)
private class PopulateGuidDDPFileTest {
    public static List<Loop__DDP__c> ddps;
    public static List<Loop__DDP_File__c> files;

    static void init() {
        ddps = new List<Loop__DDP__c>();
        ddps.add(new Loop__DDP__c());
        files = new List<Loop__DDP_File__c>();
        files.add(new Loop__DDP_File__c());
    }

    // GUID should be generated if it is set to NULL
    @isTest static void testAddNewRecordEmptyDdpMigratorId() {
        init();
        Test.startTest();

        insert ddps;

        ddps = [SELECT Id FROM Loop__DDP__c];
        files[0].Loop__DDP__c = ddps[0].Id;

        insert files;

        files = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP_File__c];

        System.assertEquals(1, files.size());
        System.assert(files[0].DDP_Migrator_Id__c != null, 'DDP_Migrator_Id__c field was not populated');

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
        files[0].Loop__DDP__c = ddps[0].Id;
        String guid = GUIDGenerator.generateGUID();
        files[0].DDP_Migrator_Id__c = guid;

        insert files;

        files = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP_File__c];

        System.assertEquals(1, files.size());
        System.assertEquals(guid, files[0].DDP_Migrator_Id__c);

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
        files[0].Loop__DDP__c = ddps[0].Id;
        String guid = GUIDGenerator.generateGUID();
        files[0].DDP_Migrator_Id__c = guid;

        insert files;

        files = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP_File__c];

        // Create duplicate/clone file record
        files.add(new Loop__DDP_File__c());
        files[1].Loop__DDP__c = ddps[0].Id;
        files[1].DDP_Migrator_Id__c = guid;

        insert files[1];

        // Retrieve second file only
        files = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP_File__c WHERE Id != :files[0].Id];

        System.assertEquals(1, files.size());
        System.assertNotEquals(guid, files[0].DDP_Migrator_Id__c);

        Test.stopTest();
    }
}