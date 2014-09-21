import os
from os.path import join
def json_out(dict,output_dict):

    file_json = os.path.join(dict,'input.json')
    file_out=open(file_json,'w')
    file_out.write('[\n')
    dirList=os.listdir(dict)
    gz_file=[]
    name=[]
    for files in dirList:
        input_fastq=join(dict,files)
        if input_fastq.endswith('.gz'):
            gz_file.append(input_fastq)
            name.append(files)

    for i in range(len(gz_file)):
        file_out.write('  {\n'+
                       '    \"gz_output_dir\": '+'\"'+output_dict+'\",\n'+
                       '    \"gz_path\": '+'\"'+gz_file[i]+'\",\n'+
                       '    \"gz_name\": '+'\"'+name[i]+'\"\n')
        if i==len(gz_file)-1:
            file_out.write('  }\n')
        else:
            file_out.write('  },\n')
            
    file_out.write(']')
    file_out.close()
    return file_json