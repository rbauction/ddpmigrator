@isTest(seeAllData=false)
private class PopulateGuidRelatedObjectTest {
    public static List<Loop__DDP__c> ddps;
    public static List<Loop__Related_Object__c> relobjs;

    static void init() {
        ddps = new List<Loop__DDP__c>();
        ddps.add(new Loop__DDP__c());
        relobjs = new List<Loop__Related_Object__c>();
        relobjs.add(new Loop__Related_Object__c());
    }

    // GUID should be generated if it is set to NULL
    @isTest static void testAddNewRecordEmptyDdpMigratorId() {
        init();
        Test.startTest();

        insert ddps;

        ddps = [SELECT Id FROM Loop__DDP__c];
        relobjs[0].Loop__DDP__c = ddps[0].Id;

        insert relobjs;

        relobjs = [SELECT Id, DDP_Migrator_Id__c FROM Loop__Related_Object__c];

        System.assertEquals(1, relobjs.size());
        System.assert(relobjs[0].DDP_Migrator_Id__c != null, 'DDP_Migrator_Id__c field was not populated');

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
        relobjs[0].Loop__DDP__c = ddps[0].Id;
        String guid = GUIDGenerator.generateGUID();
        relobjs[0].DDP_Migrator_Id__c = guid;

        insert relobjs;

        relobjs = [SELECT Id, DDP_Migrator_Id__c FROM Loop__Related_Object__c];

        System.assertEquals(1, relobjs.size());
        System.assertEquals(guid, relobjs[0].DDP_Migrator_Id__c);

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
        relobjs[0].Loop__DDP__c = ddps[0].Id;
        String guid = GUIDGenerator.generateGUID();
        relobjs[0].DDP_Migrator_Id__c = guid;

        insert relobjs;

        relobjs = [SELECT Id, DDP_Migrator_Id__c FROM Loop__Related_Object__c];

        // Create duplicate/clone file record
        relobjs.add(new Loop__Related_Object__c());
        relobjs[1].Loop__DDP__c = ddps[0].Id;
        relobjs[1].DDP_Migrator_Id__c = guid;

        insert relobjs[1];

        // Retrieve second file only
        relobjs = [SELECT Id, DDP_Migrator_Id__c FROM Loop__Related_Object__c WHERE Id != :relobjs[0].Id];

        System.assertEquals(1, relobjs.size());
        System.assertNotEquals(guid, relobjs[0].DDP_Migrator_Id__c);

        Test.stopTest();
    }
}