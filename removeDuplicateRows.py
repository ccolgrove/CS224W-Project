import sys
import string

if (len(sys.argv) > 1):
  for fileName in sys.argv[1:]:
    try:
      file = open(fileName, 'r')
    except IOError:
      print fileName + ' could not be opened.'
      continue
    periodInd = string.find(fileName, '.')
    outfileName = fileName[:periodInd] + '_noDuplicates' + fileName[periodInd:]
    outfile = open(outfileName, 'w')
    lines = {}
    for line in file:
      if not line in lines:
        lines[line] = True
        outfile.write(line)
    file.close()
    outfile.close()
else:
  print 'You must specify at least one file to process.'