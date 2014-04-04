import argparse
import re
import os
from os.path import isfile, join
def main():
    parser = argparse.ArgumentParser(description='Parse Log File')
    parser.add_argument('-log',type=str,help='Log File',required=True)
    parser.add_argument('-out',type=str,help='Output file',required=True)
    parser.add_argument('-nt',type=str,help='NumThreads',required=True)
    parser.add_argument('-sv_type',type=str,help='Type of the structural variations to detect',required=True) 
    parser.add_argument('-mates_orientation',type=str,help='Orientation of Mate',required=True) 
    parser.add_argument('-read1_length',type=str,help='Read 1 length',required=True)
    parser.add_argument('-read2_length',type=str,help='Read 2 length',required=True)
    parser.add_argument('-mates_file',type=str,help='Full path to the abnormal mate-pair input data file',required=True)
    parser.add_argument('-cmap_file',type=str,help='Full path to the chromosome length file',required=True)
    parser.add_argument('-output_dir',type=str,help='Output directory location',required=True)
    parser.add_argument('-mates_file_ref',type=str,help='Full path to the abnormal mate-pair input data file',required=True)
    parser.add_argument('-split_link_file',type=str,help='Flag to split the original link file per chromosome for parallel computing',required=True)
    parser.add_argument('-nb_pairs_threshold',type=str,help='Minimum number of pairs in a cluster',required=True)
    parser.add_argument('-strand_filtering',type=str,help='Flag to run the strand orientation filtering.',required=True)
    parser.add_argument('-insert_size_filtering',type=str,help='Flag to run the filtering on the separation distance between paired-end reads for intra-chromosomal links only',required=True)
    parser.add_argument('-chr',type=str,help='Chr',required=True)
    
    args = parser.parse_args()
    args= vars(args)
    
    if not os.path.exists(join(args["output_dir"],"CHR_len")):
        os.makedirs(join(args["output_dir"],"CHR_len"))
        
    with open(args["cmap_file"]) as f:
        for line in f:
            line=line.rstrip()
            campi=line.split('\t')
            if campi[0] == args["chr"]:
                stringa="hg19_chr"+args["chr"]+".len"
                file_cmap=join(args["output_dir"],"CHR_len",stringa)
                file_out=open(join(args["output_dir"],"CHR_len",stringa),'w')
                file_out.write("1\t"+campi[1]+"\t"+campi[2])
    file_out.close
        
    with open(args["log"]) as f:
        for line in f:
            match=re.match('(^-- mu length = )(\d+)(, sigma length = )(\d+)',line)
            if match is not None:
                mu=int(match.group(2))
                sigma=int(match.group(4))
                break
        file_out=open(args["out"],"w")
        stringa=""
        stringa+="<general>\n"
        stringa+="input_format=bam\n"
        stringa+="sv_type="+args["sv_type"]+"\n"
        stringa+="mates_orientation="+args["mates_orientation"]+"\n"
        stringa+="read1_length="+args["read1_length"]+"\n"
        stringa+="read2_length="+args["read2_length"]+"\n"
        stringa+="mates_file="+args["mates_file"]+"\n"
        stringa+="cmap_file="+file_cmap+"\n"
        stringa+="output_dir="+args["output_dir"]+"\n"
        stringa+="tmp_dir="+args["output_dir"]+"/temp"+"\n"
        stringa+="num_threads="+args["nt"]+"\n"
        stringa+="</general>\n"
        stringa+="<detection>\n"
        stringa+="split_mate_file=1\n"
        stringa+="window_size="+str(mu+(2*sigma))+"\n"
        stringa+="step_length="+str((mu+(2*sigma))/3)+"\n"
        stringa+="mates_file_ref="+args["mates_file_ref"]+"\n"
        stringa+="</detection>\n"
        stringa+="<filtering>\n"
        stringa+="split_link_file="+args["split_link_file"]+"\n"
        stringa+="nb_pairs_threshold="+args["nb_pairs_threshold"]+"\n"
        stringa+="strand_filtering="+args["strand_filtering"]+"\n"
        stringa+="insert_size_filtering="+args["insert_size_filtering"]+"\n"
        stringa+="mu_length="+str(mu)+"\n"
        stringa+="sigma_length="+str(sigma)+"\n"
        stringa+="</filtering>\n"
        file_out.write(stringa)

main()