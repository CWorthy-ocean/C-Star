#!/usr/bin/env python

# usage: makedep.py $(DEP_FILE) $(OBJ_DIR) $(SRC_DIR) [$(SRC_DIR2)]

# Generate $DEP_FILE in $OBJ_DIR (arguments 1 and 2, respectively)
# Read in every file in $SRC_DIR and $SRC_DIR2 (arguments 3 and 4)
# Only depend on modules located in $SRC_DIR or $SRC_DIR2

import os, sys, re, logging
logger = logging.getLogger(__name__)
logging.basicConfig(format='(makedep.py): %(message)s', level=logging.DEBUG)

try:
  dep_file = sys.argv[1]
except:
  dep_file = "depends.d"

try:
  obj_dir = sys.argv[2]
except:
  obj_dir = '.'

try:
  src_dir = sys.argv[3]
except:
  src_dir = '.'

try:
  src_dir2 = sys.argv[4]
except:
  src_dir2 = src_dir

try:
  inc_dir = sys.argv[5]
  files_in_inc_dir = os.listdir(inc_dir)
except:
  inc_dir = 'NONE'

fout = open(dep_file, 'w')
files_in_src_dir =  os.listdir(src_dir)
if src_dir != src_dir2:
  files_in_src_dir.extend(os.listdir(src_dir2))

for src_file in files_in_src_dir:
#for src_file in ['diagnostics.F',]:  
  file_name, file_ext = os.path.splitext(src_file)
  if file_ext in ['.F','.opt']:
    try:
      fin = open(os.path.join(src_dir, src_file),"r")
    except:
      fin = open(os.path.join(src_dir2, src_file),"r")

    # (1) dependency list from current file should be empty
    depends = ['']
    for line in fin:
      # (2) look for statement that starts with "use"
      #     (case insensitive)
      if re.match('^ *[Uu][Ss][Ee]',line):
        line_array = line.split()
        # (3) statements are usually "use module, only : subroutine"
        #     so we need to strip away the , to get the module name
        try:
          file_used = line_array[1].split(',')[0]
          if file_used+'.F' in files_in_src_dir:
            # (4) if file hasn't previously been used, add it to list
            if file_used not in depends:
              depends.append(file_used)
              #logging.info(file_name+'.o depends on '+file_used+'.o')
              #fout.write(file_name+'.o: '+file_used+'.o\n')

          else:
            if inc_dir != 'NONE':
              if file_used+'.mod' in files_in_inc_dir:
                'nothing happens here'
                #logging.info(file_name+'.o depends on '+file_used+'.mod')
                #fout.write(file_name+'.o: '+file_used+'.mod\n')
              else:
                # Check for upper case
                file_used = file_used.upper()
                if file_used+'.mod' in files_in_inc_dir:
                  'nothing happens here'
                  #logging.info(file_name+'.o depends on '+file_used+'.mod')
                  #fout.write(file_name+'.o: '+file_used+'.mod\n')
        except:
          print('something went screwy with '+file_name+' on line '+line)          
################################################################################
      # (2) look for statement that starts with "#include"
      #     (case insensitive)
      elif re.match('^ *[#][iI][nN][cC][lL][uU][dD][eE]',line):
        line_array = line.split()
        # (3) statements are usually `include "X.opt"`
        #     so we need to strip away the " to get the file name
        try:
          file_used = line_array[1].split('"')[1]
          if file_used in files_in_src_dir:
            # (4) if file hasn't previously been used, add it to list
            if file_used not in depends:
              depends.append(file_used)
              #logging.info(file_name+'.o depends on '+file_used)
              #fout.write(file_name+'.o: '+file_used+'.o\n')

          else:
            if inc_dir != 'NONE':
              if file_used+'.mod' in files_in_inc_dir:
                'nothing happens here'
                #logging.info(file_name+'.o depends on '+file_used+'.mod')
                #fout.write(file_name+'.o: '+file_used+'.mod\n')
              else:
                # Check for upper case
                file_used = file_used.upper()
                if file_used+'.mod' in files_in_inc_dir:
                  'nothing happens here'
                  #logging.info(file_name+'.o depends on '+file_used+'.mod')
                  #fout.write(file_name+'.o: '+file_used+'.mod\n')
        except:
          print('something went screwy with '+file_name+' on line '+line)
################################################################################
    DEPSTR=file_name+'.o : '+src_file+' '+\
               ''.join([(depends[i]+'.o ') for i in range(1,len(depends))])
    DEPSTR=DEPSTR.replace('.opt.o','.opt')
    DEPSTR=DEPSTR.replace('.h.o','.h')
    DEPSTR=DEPSTR+'\n'
    fout.write(DEPSTR)
    fin.close
fout.close()
