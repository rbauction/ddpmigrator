trigger PopulateGuidRelatedObject on Loop__Related_Object__c (before insert) {
    // Before Insert: Assign GUID
    if (trigger.isBefore && trigger.isInsert) {
        try {
            for (Loop__Related_Object__c l : trigger.new) {
                if (l.DDP_Migrator_Id__c == null)
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