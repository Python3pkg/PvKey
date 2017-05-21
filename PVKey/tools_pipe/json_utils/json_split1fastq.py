#!/usr/bin/env python
"""
Chunks a fastq file
TODO implement a producer/consumer pattern
.. note:: This is nicer than the unix split command because everything is gzipped which means less scratch space and i/o (at the cost of CPU)
.. note:: The closest unix split command equivalent is `split -a 3 -l 2 -d input.txt /tmp/input_`
"""
import re
import os
import logging as log
import gzip
from os.path import isfile, join
from cosmos.utils.helpers import confirm
from itertools import islice
from .json_param import setting
import argparse

def main(input_fastq,output_dir,output_json,chunksize,buffersize):
    print(input_fastq)
    #Create Output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)   
    file_out=open(output_json,'w')
    file_out.write('[\n')
    if isfile(input_fastq): 
        #print(files)
        """
        Chunks a large fastq file into smaller pieces.
        """
        chunk = 0
        log.info('Opening {0}'.format(input_fastq))
        if input_fastq.endswith('.gz'):     infile = gzip.open(input_fastq)
        else:                               infile = open(input_fastq,'r')
            
        output_prefix = os.path.basename(input_fastq)
        output_prefix = re.search("(.+?)(_001)*\.(fastq|fq)(\.gz)*",output_prefix).group(1)
        rgid=""
        if re.search("(_R1)",os.path.basename(input_fastq)):
            pair="1"
            rgid=output_prefix
            rgid=rgid.replace("_R1", "")
        elif re.search("(_R2)",os.path.basename(input_fastq)):
            pair="2"
            rgid=output_prefix
            rgid=rgid.replace('_R2', "")
        else:
            print("WARNING: "+os.path.basename(input_fastq)+" is not formatted properly")
            
        eof = False
        while not eof:
            chunk += 1
            # generate output paths
            new_filename = '{0}_{1:0>3}'.format(output_prefix,chunk)
            output_path = os.path.join(output_dir,new_filename+'.fastq.gz')
            #print output_path
            '''
            if os.path.exists(output_path):
                if not confirm('{0} already exists!  Are you sure you want to overwrite the file?'.format(output_path), timeout=0):
                    return
            '''
            log.info('Reading {0} lines and writing to: {1}'.format(chunksize,output_path))
            file_out.write('  {\n'+
                           '    \"chunk\": '+'\"'+'{0:0>3}'.format(chunk)+'\",\n'+
                           '    \"library\": '+'\"'+setting['library']+'\",\n'+
                           '    \"platform\": '+'\"'+setting['platform']+'\",\n'+
                           '    \"platform_unit\": '+'\"'+setting['platform_unit']+'\",\n'+
                           '    \"rgid\": '+'\"'+rgid+'\",\n'+
                           '    \"sample_name\": '+'\"'+rgid+'\",\n'+
                           '    \"pair\": '+'\"'+pair+'\",\n'+
                           '    \"path\": '+'\"'+output_path+'\",\n'+
                           '  },\n')
            #Read/Write
            total_read=0
            outfile = gzip.open(output_path,'wb')
            while total_read < chunksize and not eof:
                data = list(islice(infile,buffersize)) #read data
                dataLen = len(data)
                if dataLen != buffersize: 
                    eof = True
                if dataLen == 0:
                    break 
                # Write what was read
                outfile.writelines(data)
                log.info('wrote {0} lines'.format(len(data)))
                del(data)
                total_read += dataLen
            outfile.close()            
                 
        infile.close()
        log.info('Done')
    file_out.write(']')
    file_out.close()

#main("/home/ettore/Scrivania/Analisi5_6_7_8_10_11/Run11_Fastq/","/home/ettore/Scrivania/Cosmos/Prova_Chunk/prova",100000,100000)

if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser(description='SplitFastqs')
    parser.add_argument('input_fastq',type=str,help='')
    parser.add_argument('output_dir',type=str,help='')
    parser.add_argument('output_json',type=str,help='')
    parser.add_argument('-c','--chunksize',type=int,help='Number of reads per fastq chunk, default is 1 million',default=400000)
    parser.add_argument('-b','--buffersize',type=int,help='Number of reads to keep in RAM, default is 100k',default=400000)
    parsed_args = parser.parse_args()
    kwargs = dict(parsed_args._get_kwargs())
    main(**kwargs)


