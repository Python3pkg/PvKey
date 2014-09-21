import re
import logging as log
import argparse
from os.path import join

def main(input,output_file):
    with open(input, 'r') as content_file:
        content = content_file.read()
    output_prefix = re.findall("\{(.*?)\}",content)
    
    
    out2=open(output_file,'w')
    out2.write('[\n')
    
    
    for element in output_prefix:
        print element
        lista=element.split(',')
        lista=lista[:-1]
        out2.write('  {\n')
        for el_list in lista:
            if el_list==lista[-1]:
                out2.write('    '+el_list+'\n')
            else:
                out2.write('    '+el_list+',\n')
        if element==output_prefix[-1]:
            out2.write('  }\n')
        else:
            out2.write('  },\n')
            
    out2.write(']')
    out2.close()

if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser(description='Parse_Json')
    parser.add_argument('input',type=str,help='Input json uncorrect')
    parser.add_argument('output_file',type=str,help='Output Json')
    parsed_args = parser.parse_args()
    kwargs = dict(parsed_args._get_kwargs())
    main(**kwargs)