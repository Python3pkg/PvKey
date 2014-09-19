PvKey
=====
PvKey is a pipeline that work with Tumor-Normal matched samples. It calls somatic variants using Mutect and structural variants using SVDetect.
It can handle genome, exome and targeted samples (TruSeq Custom Amplicon). It is implemented and made possible by the Cosmos workflow management system.
Components include:
* BWA aln + GATK Data Preprocessing + Mutect + SVDetect.
* Download data from a bucket S3.


Configuration
-------------
PvKey is configured in wga_settings.py where it points to the correct paths to the GATK bundle, reference genome, and binaries 
* Mutect should be added to the /WGA/tools directory 
* SVDetect should be added to the /WGA/tools directory
* b37_cosmic_v54_120711.vcf should be added to the /WGA/bundle/current directory 
* dbsnp_132_b37.leftAligned.vcf should be added to the /WGA/bundle/current directory 
* hg19.len should be added to the /WGA/bundle/current directory

Note: on Orchestra the files are placed in the right order, and the WGA directory is available currently under /groups/cbi/02.Public.data/WGA/, it will be moved to /groups/lpm/WGA.

Usage
-----
Inside the PvKey directory, execute:

cli -h

BWA aln + GATK Data Preprocessing + Mutect + SVDetect
------------------------------------------
* genomekey json_somatic -n "My Tumor/Normal Workflow” -i /path/to/json

.. code-block:: json
    
    [
        {
            'chunk': 001,
            'library': 'LIB-1216301779A',
            'platform': 'ILLUMINA',
            'platform_unit': 'C0MR3ACXX.001', 
            'sample_name': 'BC18-06-2013_LyT_S5_L001',
            'rgid': 'BC18-06-2013',
            'pair': 0, #0 or 1
            'path': '/path/to/fastq',
            'sample_tye' : 'tumor' or 'normal'
        },
        {..}
    ]
    
Note: If you are working on target resequencing data generated with TruSeq Custom Amplicon assay, add -target True (mark duplicates will not be performed because all the reads are duplicates)

Download data from a bucket S3
------------------------------
* genomekey upload_s3 -n "Download to ephimeral from s3” -b “Bucket Name” -p “Bucket folder” -out_dict  /path/to/download/directory

Note: It requires boto plugin


Download from BaseSpace
=======================
This python script interact with the ILLUMINA repository of ngs data (BaseSpace) to download all the sequenced sample within a project. To make it work you have to import BaseSpacePy.
https://github.com/basespace/basespace-python-sdk.git

BaseSpacePy is a Python based SDK to be used in the development of Apps and scripts for working with Illumina's BaseSpace cloud-computing solution for next-gen sequencing data analysis. 
The primary purpose of the SDK is to provide an easy-to-use Python environment enabling developers to authenticate a user, retrieve data, and upload data/results from their own analysis to BaseSpace.

