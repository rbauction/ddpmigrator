@isTest(seeAllData=false)
private class PopulateGuidDDPIntegrationOptionTest {
    public static List<Loop__DDP__c> ddps;
    public static List<Loop__DDP_Integration_Option__c> intopts;

    static void init() {
        ddps = new List<Loop__DDP__c>();
        ddps.add(new Loop__DDP__c());
        intopts = new List<Loop__DDP_Integration_Option__c>();
        intopts.add(new Loop__DDP_Integration_Option__c());
    }

    // GUID should be generated if it is set to NULL
    @isTest static void testAddNewRecordEmptyDdpMigratorId() {
        init();
        Test.startTest();

        insert ddps;

        ddps = [SELECT Id FROM Loop__DDP__c];
        intopts[0].Loop__DDP__c = ddps[0].Id;

        insert intopts;

        intopts = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP_Integration_Option__c];

        System.assertEquals(1, intopts.size());
        System.assert(intopts[0].DDP_Migrator_Id__c != null, 'DDP_Migrator_Id__c field was not populated');

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
        intopts[0].Loop__DDP__c = ddps[0].Id;
        String guid = GUIDGenerator.generateGUID();
        intopts[0].DDP_Migrator_Id__c = guid;

        insert intopts;

        intopts = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP_Integration_Option__c];

        System.assertEquals(1, intopts.size());
        System.assertEquals(guid, intopts[0].DDP_Migrator_Id__c);

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
        intopts[0].Loop__DDP__c = ddps[0].Id;
        String guid = GUIDGenerator.generateGUID();
        intopts[0].DDP_Migrator_Id__c = guid;

        insert intopts;

        intopts = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP_Integration_Option__c];

        // Create duplicate/clone file record
        intopts.add(new Loop__DDP_Integration_Option__c());
        intopts[1].Loop__DDP__c = ddps[0].Id;
        intopts[1].DDP_Migrator_Id__c = guid;

        insert intopts[1];

        // Retrieve second file only
        intopts = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP_Integration_Option__c WHERE Id != :intopts[0].Id];

        System.assertEquals(1, intopts.size());
        System.assertNotEquals(guid, intopts[0].DDP_Migrator_Id__c);

        Test.stopTest();
    }
}