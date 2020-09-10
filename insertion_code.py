import mysql.connector 
import csv 
class Passenger(object): 
    def __init__(self, id, survival, socialStatus, lName, fName, sex, age, sibling_spouse, parent_child, ticketNo, fare, cabinNo, embarked): 
        self.id = id 
        self.survival = survival 
        self.socialStatus = socialStatus 
        self.lName = lName 
        self.fName = fName 
        self.sex = sex 
        self.age = age 
        self.sibling_spouse = sibling_spouse 
        self.parent_child = parent_child 
        self.ticketNo = ticketNo 
        self.fare = fare 
        self.cabinNo = cabinNo 
        self.embarked = embarked 

passengersList = [] 

with open('titanic-passengers3.csv', 'r') as file: 
    reader = csv.reader(file) 
    for index, row in enumerate(reader): 
        if index != 0:
            id = int(row[0].strip()) 
            survival = row[1].strip() 
            socialStatus = int(row[2].strip()) 
            lName = row[3].strip() 
            fName = row[4].strip() 
            sex = row[5].strip() 
            age = -1 

            if len(row[6]) != 0: 
                age = float(row[6].strip())
             
            sibling_spouse = int(row[7].strip()) 
            parent_child = int(row[8].strip()) 
            ticketNo = row[9].strip() 
            fare = float(row[10].strip()) 
            cabinNo = "None"

            if len(row[11]) != 0: 
                cabinNo = row[11].strip() 

            embarked = "Unknown" 
            if len(row[12]) != 0: 
                embarked = row[12].strip() 
                
            passenger = Passenger(id, survival, socialStatus, lName, fName, sex, age, sibling_spouse, parent_child, ticketNo, fare, cabinNo, embarked) 
            passengersList.append(passenger) 

mydb = mysql.connector.connect( 
    host = "localhost", 
    user = "root", 
    password = "root", 
    database = 'titanic', 
    auth_plugin = 'mysql_native_password' 
    ) 
    
unique_ticket = []
unique_cabinNo = [] 
unique_composite = [] # a combination of the ticket number and cabin number to ensure the entries are unique

mycursor = mydb.cursor() 

for passenger in passengersList: 
    sql = "INSERT INTO ticket (ticketNo, fare, embarked) VALUES (%s, %s, %s)" 
    val = (passenger.ticketNo, passenger.fare, passenger.embarked) 
    if passenger.ticketNo not in unique_ticket: 
        mycursor.execute(sql, val) 
    unique_ticket.append(passenger.ticketNo)
    sql = "INSERT INTO passenger (passengerId, firstName, lastName, age, sex, ticketNo, class, parch, sibsp, survival) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" 
    val = (passenger.id, passenger.fName, passenger.lName, passenger.age, passenger.sex, passenger.ticketNo, passenger.socialStatus, passenger.parent_child, passenger.sibling_spouse, passenger.survival) 
    mycursor.execute(sql, val) 
    sql = "INSERT INTO cabin (ticketNo, cabinNo) VALUES (%s, %s)" 
    val = (passenger.ticketNo, passenger.cabinNo) 
    if passenger.cabinNo != "None" and passenger.ticketNo + passenger.cabinNo not in unique_composite: 
        mycursor.execute(sql, val) 
    unique_composite.append(passenger.ticketNo + passenger.cabinNo) 
    mydb.commit() 
    
mycursor.close() 
mydb.close() 
file.close()