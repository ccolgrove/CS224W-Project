import pymongo

SERVER = 'localhost' #'ec2-50-112-32-119.us-west-2.compute.amazonaws.com'
PORT = 1001 #1000

db = pymongo.Connection(SERVER, PORT).wp

def calculate_cool_network_feature(param):
    '''
    Calculates cool network feature and then updates the db.
    
    args:
        param - some really awesome param
    '''
    
    #Finding a document in the database
    example_page = db.pages.find_one({"_id": 12})
    print example_page
    
    #Updating a document in the database
    #db.pages.update({"_id": 12}, {"$set": { "field" : "value" } }}
    

if __name__ == "__main__":
    calculate_cool_network_feature(None)

