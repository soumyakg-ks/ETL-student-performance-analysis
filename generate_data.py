import pandas as pd
import random

def get_grade(m, p, c):
    avg = (m + p + c) / 3
    if avg >= 90:   return 'A+', 'Excellent Performance'
    elif avg >= 80: return 'A',  'Very Good Achivement'
    elif avg >= 70: return 'B+', 'Good Pursuance'
    elif avg >= 60: return 'B',  'Average Performance'
    elif avg >= 50: return 'C',  'Below Average Achivement'
    elif avg >= 35: return 'D',  'Poor Pursuance'
    else:           return 'F',  'Failed'

first = ['James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','William','Barbara',
'David','Susan','Richard','Jessica','Joseph','Sarah','Thomas','Karen','Charles','Lisa',
'Christopher','Nancy','Daniel','Betty','Matthew','Margaret','Anthony','Sandra','Mark','Ashley',
'Donald','Dorothy','Steven','Kimberly','Paul','Emily','Andrew','Donna','Joshua','Michelle',
'Kenneth','Carol','Kevin','Amanda','Brian','Melissa','George','Deborah','Timothy','Stephanie',
'Ronald','Rebecca','Edward','Sharon','Jason','Laura','Jeffrey','Cynthia','Ryan','Kathleen',
'Jacob','Amy','Gary','Angela','Nicholas','Shirley','Eric','Anna','Jonathan','Brenda',
'Stephen','Pamela','Larry','Emma','Justin','Nicole','Scott','Helen','Brandon','Samantha',
'Benjamin','Katherine','Samuel','Christine','Raymond','Debra','Gregory','Rachel','Frank','Carolyn',
'Alexander','Janet','Patrick','Catherine','Jack','Maria','Dennis','Heather','Jerry','Diane',
'Tyler','Julie','Aaron','Joyce','Jose','Victoria','Adam','Kelly','Nathan','Christina',
'Henry','Lauren','Douglas','Joan','Zachary','Evelyn','Peter','Olivia','Kyle','Judith',
'Walter','Megan','Ethan','Cheryl','Jeremy','Martha','Harold','Andrea','Terry','Frances',
'Sean','Hannah','Austin','Jacqueline','Gerald','Ann','Carl','Gloria','Keith','Jean']

last = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin',
'Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts',
'Turner','Phillips','Evans','Collins','Stewart','Morris','Morales','Murphy','Cook','Rogers',
'Gutierrez','Ortiz','Morgan','Cooper','Peterson','Bailey','Reed','Kelly','Howard','Ramos',
'Kim','Cox','Ward','Richardson','Watson','Brooks','Chavez','Wood','James','Bennett',
'Gray','Mendoza','Ruiz','Hughes','Price','Alvarez','Castillo','Sanders','Patel','Myers',
'Long','Ross','Foster','Jimenez','Powell','Jenkins','Perry','Russell','Sullivan','Bell',
'Coleman','Butler','Henderson','Barnes','Gonzales','Fisher','Vasquez','Simmons','Romero','Jordan',
'Patterson','Alexander','Hamilton','Graham','Reynolds','Griffin','Wallace','Moreno','West','Cole',
'Hayes','Bryant','Herrera','Gibson','Ellis','Tran','Medina','Aguilar','Stevens','Murray',
'Ford','Castro','Marshall','Owens','Harrison','Fernandez','Mcdonald','Woods','Washington','Kennedy']

schools = ['Martin Luther School','Lincoln High School','Jefferson Academy',
           'Washington Middle School','Roosevelt Institute','Kennedy Public School',
           'Edison Learning Center','Franklin Academy','Adams High School','Monroe School']

states  = ['CA','TX','NY','FL','IL','PA','OH','GA','NC','MI','NJ','VA','WA','AZ','MA',
           'TN','IN','MO','MD','WI','CO','MN','SC','AL','LA','KY','OR','OK','CT','UT']

streets = ['Main St','Oak Ave','Maple Dr','Cedar Ln','Pine Rd','Elm St',
           'Park Ave','Lake Dr','Hill Rd','River Rd','Forest Ave','Valley Dr']

df = pd.read_csv('dataset/student_dataset.csv')
df.columns = df.columns.str.strip()
print('Original rows:', len(df))

used_rolls  = set(df['Roll No.'].tolist())
used_phones = set(df['Phone_No.'].tolist())

random.seed(42)
rows = []
attempts = 0
while len(rows) < 41000:
    attempts += 1
    fn = random.choice(first)
    ln = random.choice(last)
    ph = random.randint(9000000000, 9999999999)
    if ph in used_phones:
        continue
    used_phones.add(ph)
    roll = random.randint(700000, 999999)
    if roll in used_rolls:
        continue
    used_rolls.add(roll)
    m = random.randint(10, 100)
    p = random.randint(10, 100)
    c = random.randint(10, 100)
    g, cm = get_grade(m, p, c)
    addr = (str(random.randint(1, 9999)) + ' ' + random.choice(streets) +
            ', ' + fn + 'ville, ' + random.choice(states) +
            ' ' + str(random.randint(10000, 99999)))
    rows.append({
        'Student_Names': fn + ' ' + ln,
        'Phone_No.':     ph,
        'Math':          m,
        'Physics':       p,
        'Chemistry':     c,
        'Grade':         g,
        'Comment':       cm,
        'Roll No.':      roll,
        'School Name':   random.choice(schools),
        'Student Address': addr
    })
    if len(rows) % 5000 == 0:
        print(f'Generated {len(rows)} rows...')

df_new   = pd.DataFrame(rows)
df_final = pd.concat([df, df_new], ignore_index=True)
df_final.to_csv('dataset/student_dataset.csv', index=False)
print('Total rows saved:', len(df_final))
print(df_final['Grade'].value_counts().to_string())
