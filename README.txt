Installation
============

# Install YAML library
pip install PyYAML

# Install SFDC library
pip install sfdclib

# Add text field DDP_Migrator_Id__c (Length=36, External ID, Unique, Case sensitive) to the following custom objects:
DDP: Loop__DDP__c
DDP File: Loop__DDP_File__c
Delivery Option: Loop__DDP_Integration_Option__c
Relationship: Loop__Related_Object__c

# Execute PopulateGuids.apex script to set values in DDP_Migrator_Id__c field in all four objects
# Populate GUIDs in Production first, then either refresh sandboxes from Production or copy external IDs to sandboxes to have IDs match in all environments

How to create Windows executable
---
Install PyInstaller and then run the following command:
```
pyinstaller --onefile ddpmigrator.py
```
Resulting exe file can be found in dist directory.

Usage
=====

Export
------
The tool exports data and documents into the directory specified by --source-dir switch.
It creates loop directory in the source directory and data and documents sub-directories.

# Export all DDPs
ddpmigrator.py --sandbox --username user@domain.com.sandbox_name --password Secret --source-dir .. export

# Export two DDPs
ddpmigrator.py --sandbox --username user@domain.com.sandbox_name --password Secret --source-dir .. export --ddp "DDP one" "DDP two"

Push external IDs
-----------------
This command is useful during rollout of DDP migrator to make sure external IDs in production and all sandboxes match.
It will push external IDs stored in the source directory to a sandbox.

# Update all external IDs
ddpmigrator.py --sandbox --username user@domain.com.sandbox_name --password Secret --source-dir .. push-ids

Import
------
The tool expects the source directory to be a Git repository so it can run 'git diff' to find changes.

# Import DDPs changed since certain commit
ddpmigrator.py --sandbox --username user@domain.com.sandbox_name --password Secret --source-dir .. --baseline 6771fbc7 import
