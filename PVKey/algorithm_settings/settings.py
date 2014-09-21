import os
import sys

def bwa_aln_settings():
    settings = {
        'maxDiff'                       : 0.04,
        'maxGapO'                       : 1,
        'maxGapE'                       : -1,       
        'nDelTail'                      : 16,  
        'nIndelEnd'                     : 5,
        'maxSeedDiff'                   : 2,
        'nThrds'                        : 4,
        'misMsc'                        : 3,
        'gapOsc'                        : 11,
        'gapEsc'                        : 4,
        'trimQual'                      : 0                    
        }
    return '-n '+str(settings['maxDiff'])+' -o '+str(settings['maxGapO'])+' -e '+str(settings['maxGapE'])+\
           ' -d '+str(settings['nDelTail'])+' -i '+str(settings['nIndelEnd'])+' -k '+str(settings['maxSeedDiff'])+\
           ' -t '+str(settings['nThrds'])+' -M '+str(settings['misMsc'])+' -O '+str(settings['gapOsc'])+\
           ' -E '+str(settings['gapEsc'])+' -q '+str(settings['trimQual'])

def sv_settings():
    settings = {
        'sv_type'                       : 'all',
        'mates_orientation'             : 'FR',
        'read1_length'                  : 250,
        'read2_length'                  : 250,
        'split_link_file'               : 1,
        'nb_pairs_threshold'            : 5,
        'strand_filtering'              : 1,
        'insert_size_filtering'         : 0                  
        }
    return '-sv_type '+str(settings['sv_type'])+' -mates_orientation '+str(settings['mates_orientation'])+\
           ' -read1_length '+str(settings['read1_length'])+' -read2_length '+str(settings['read2_length'])+\
           ' -split_link_file '+str(settings['split_link_file'])+' -nb_pairs_threshold '+str(settings['nb_pairs_threshold'])+\
           ' -strand_filtering '+str(settings['strand_filtering'])+' -insert_size_filtering '+str(settings['insert_size_filtering'])
