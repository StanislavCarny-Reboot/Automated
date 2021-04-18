import psycopg2

# define connection
con = psycopg2.connect(
    host ='ec2-54-72-155-238.eu-west-1.compute.amazonaws.com',
    database = 'd77vu5pdkc80c5',
    user = 'ziverzcxjkukah',
    password = '86be3e356b4890952d6dddefc0580644b8e262797eacd19390e90c55b6b66d9b',
    port = '5432'
    
)

# create cursor to enable to execute SQL commands
cur = con.cursor()

# excecute SQL command

cur.execute(
"""
INSERT INTO accounts (user_id,username,password,email,created_on)
VALUES(2, 'tes2','pass1','etes@emal1.com','2020-01-02')
"""
)

# only for selecting data
#rows = cur.fetchall()

# commit is only for insert statements
con.commit()

#print(rows)

    
cur.close()

con.close()
