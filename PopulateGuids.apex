// Loop__DDP__c
List<Loop__DDP__c> ddps = [
	SELECT Id, DDP_Migrator_Id__c
	FROM Loop__DDP__c
	WHERE DDP_Migrator_Id__c = null
];
for (Loop__DDP__c row : ddps) {
	row.DDP_Migrator_Id__c = GUIDGenerator.generateGUID();
}

update(ddps);

// Loop__DDP_File__c
List<Loop__DDP_File__c> files = [
	SELECT Id, DDP_Migrator_Id__c
	FROM Loop__DDP_File__c
	WHERE DDP_Migrator_Id__c = null
];
for (Loop__DDP_File__c row : files) {
	row.DDP_Migrator_Id__c = GUIDGenerator.generateGUID();
}

update(files);

// Loop__DDP_Integration_Option__c
List<Loop__DDP_Integration_Option__c> options = [
	SELECT Id, DDP_Migrator_Id__c
	FROM Loop__DDP_Integration_Option__c
	WHERE DDP_Migrator_Id__c = null
];
for (Loop__DDP_Integration_Option__c row : options) {
	row.DDP_Migrator_Id__c = GUIDGenerator.generateGUID();
}

update(options);

// Loop__Related_Object__c
List<Loop__Related_Object__c> objs = [
	SELECT Id, DDP_Migrator_Id__c
	FROM Loop__Related_Object__c
	WHERE DDP_Migrator_Id__c = null
];
for (Loop__Related_Object__c row : objs) {
	row.DDP_Migrator_Id__c = GUIDGenerator.generateGUID();
}

update(objs);
