@isTest(seeAllData=false)
private class PopulateGuidDDPTest {
    public static List<Loop__DDP__c> ddps;

    static void init() {
        ddps = new List<Loop__DDP__c>();
        ddps.add(new Loop__DDP__c());
    }

    // GUID should be generated if it is set to NULL
    @isTest static void testAddNewRecordEmptyDdpMigratorId() {
        init();
        Test.startTest();

        insert ddps;

        ddps = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP__c];

        System.assertEquals(1, ddps.size());
        System.assert(ddps[0].DDP_Migrator_Id__c != null, 'DDP_Migrator_Id__c field was not populated');

        Test.stopTest();
    }

    // GUID should not change if it is set and there are no duplicate records
    @isTest static void testAddNewRecordWithDdpMigratorId() {
        init();
        Test.startTest();

        String guid = GUIDGenerator.generateGUID();
        ddps[0].DDP_Migrator_Id__c = guid;
        insert ddps;

        ddps = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP__c];

        System.assertEquals(1, ddps.size());
        System.assertEquals(guid, ddps[0].DDP_Migrator_Id__c);

        Test.stopTest();
    }

    // Trigger should generate new GUID for cloned/duplicate record
    @isTest static void testCloneRecord() {
        init();
        Test.startTest();

        // Create DDP with specific GUID
        String guid = GUIDGenerator.generateGUID();
        ddps[0].DDP_Migrator_Id__c = guid;
        insert ddps;

        // Retrieve its Id
        ddps = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP__c];

        // Clone/duplicate first DDP records
        ddps.add(new Loop__DDP__c());
        ddps[1].DDP_Migrator_Id__c = guid;

        insert ddps[1];

        // Retrieve second DDP record only
        ddps = [SELECT Id, DDP_Migrator_Id__c FROM Loop__DDP__c WHERE Id != :ddps[0].Id];

        System.assertEquals(1, ddps.size());
        System.assertNotEquals(guid, ddps[0].DDP_Migrator_Id__c);

        Test.stopTest();
    }
}