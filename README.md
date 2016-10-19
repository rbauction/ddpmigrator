# DDP migrator
Tool to manage DocuSign for Salesforce document and make them Git-friendly


This tool has been designed as an alternative to DocuSign's DDP migration tool
that migrates DDPs from one sandbox to another which doesn't allow to version
control DDPs. In addition every once in a while work-in-progress DDPs get
promoted to higher environments.
The tool addresses these issues by allowing to pull and push DDP files and
corresponding data (stored in Loop__* objects) to/from a Salesforce org.
Data is stored in YAML so any small changes are easily seen in git diff.

## Installation

One can either use Python sources or a Windows executable.

1. Install YAML library
```sh
pip install PyYAML
```

2. Install SFDC library
```sh
pip install sfdclib
```

3. Add text field DDP\_Migrator\_Id\_\_c (Length=36, External ID, Unique, Case sensitive) to the following custom objects:
 * DDP: Loop\_\_DDP\_\_c
 * DDP File: Loop\_\_DDP\_File\_\_c
 * Delivery Option: Loop\_\_DDP\_Integration_Option\_\_c
 * Relationship: Loop\_\_Related\_Object\_\_c

4. Execute PopulateGuids.apex script to set values in DDP\_Migrator\_Id\_\_c field in all four objects
5. Deploy triggers and a class stored in **metadata** directory.

It is recommended to populate GUIDs/external IDs in Production org first
and then push IDs to lower environments using **push-ids** command.

How to package Windows executable
---------------------------------
Install PyInstaller and then run the following command:
```sh
pyinstaller --onefile ddpmigrator.py
```
Resulting exe file can be found in dist directory.

## Usage

### Export

The tool exports data and documents into the directory specified by --source-dir switch.
It creates loop directory in the source directory and data and documents sub-directories.

**Export all DDPs**
```sh
ddpmigrator.py --sandbox --username user@domain.com.sandbox_name --password Secret --source-dir .. export
```

**Export two DDPs**
```sh
ddpmigrator.py --sandbox --username user@domain.com.sandbox_name --password Secret --source-dir .. export --ddp "DDP one" "DDP two"
```

### Push external IDs

This command is useful during rollout of DDP migrator to make sure external IDs in production and all sandboxes match.
It will push external IDs stored in the source directory to a sandbox.

**Update all external IDs**
```sh
ddpmigrator.py --sandbox --username user@domain.com.sandbox_name --password Secret --source-dir .. push-ids
```

### Import
The tool expects the source directory to be a Git repository so it can run 'git diff' to find changes.

**Import DDPs changed since certain commit**
```sh
ddpmigrator.py --sandbox --username user@domain.com.sandbox_name --password Secret --source-dir .. --baseline 6771fbc7 import
```