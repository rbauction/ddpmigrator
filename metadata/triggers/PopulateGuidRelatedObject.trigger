trigger PopulateGuidRelatedObject on Loop__Related_Object__c (before insert, before update) {
    // Before Insert: Assign GUID
    if (trigger.isBefore && (trigger.isInsert || trigger.isUpdate)) {
        Set<String> idsToCheck = new Set<String>();
        Set<String> existingIds = new Set<String>();
        // Check whether newly added GUIDs are already in use
        for (Loop__Related_Object__c l : trigger.new) {
            if (l.DDP_Migrator_Id__c != null)
                idsToCheck.add(l.DDP_Migrator_Id__c);
        }
        List<Loop__Related_Object__c> existingRecords = [
            SELECT DDP_Migrator_Id__c
            FROM Loop__Related_Object__c
            WHERE DDP_Migrator_Id__c IN :idsToCheck
        ];
        for (Loop__Related_Object__c l : existingRecords)
            existingIds.add(l.DDP_Migrator_Id__c);

        try {
            for (Loop__Related_Object__c l : trigger.new) {
                // Generate new GUID if it is empty or if DDP is being cloned
                if (l.DDP_Migrator_Id__c == null || existingIds.contains(l.DDP_Migrator_Id__c))
                    l.DDP_Migrator_Id__c = GUIDGenerator.generateGUID();
            }
        } catch (Exception ex) {
            String body = (ex.getMessage().contains('FIELD_CUSTOM_VALIDATION_EXCEPTION')) ?
                           ex.getDmlMessage(0) :
                           ex.getMessage();
            String recordId = ex.getTypeName().equals('System.DmlException') ? ex.getDmlId(0) : null;

            System.debug(LoggingLevel.ERROR, '\n-----------------------------------------------\n'
                                          +   'An Exception has occurred! \n'
                                          +   'Exception Type: ' + ex.getTypeName() + '\n'
                                          +   'Exception Message: ' + body + '\n'
                                          +   'Exception Stack Trace: ' + ex.getStackTraceString() + '\n'
                                          +   'Exception Record Id: ' + recordId + '\n'
                                          +   '-----------------------------------------------');

            Throw ex;
        }
    }
}
