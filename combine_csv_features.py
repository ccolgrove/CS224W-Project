import csv
import sys

def combine_two_csv_files(fa_csv, fb_csv):
    """
        Combines two csv files and outputs the results
        
        args:
            fa_csv - the first csv file
            fb_csv - the second csv file
    """
    header = fa_csv.next()
    header.extend(fb_csv.next()[1:])
    
    combined_lines = {}
    for line in fa_csv:
        if len(line) > 0: #Fix for empty lines
            combined_lines[line[0]] = line #insert the whole line to grab the id
    
    for line in fb_csv:
        #This will throw an exception if an _id doesn't exist in the other
        #Trim of the id since it's already there
        if len(line) > 0: #Fix for empty lines
            combined_lines[line[0]].extend(line[1:]) 
    
    #output the csv file
    f_combine = csv.writer(open("combined.csv", 'w'), delimiter=',')
    f_combine.writerow(header)
    for line in combined_lines.values():
        f_combine.writerow(line)
        
if __name__ == "__main__":

    if len(sys.argv) < 3:
        print "Usage: features_a.csv features_b.csv"
        print "Combines the two csv feature files"
        print "Assumes that the first column is the id"
        exit()  
    
    fa_csv = csv.reader(open(sys.argv[1], 'r'), delimiter=',')
    fb_csv = csv.reader(open(sys.argv[2], 'r'), delimiter=',')

    combine_two_csv_files(fa_csv, fb_csv)
    
    