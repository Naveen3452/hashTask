


Flask & Neo4j CSV Uploader

This project is a Flask web application that allows users to upload a CSV file. The data is then processed and stored in a Neo4j database by creating nodes and relationships based on the CSV contents.

Features
Upload CSV: Upload a CSV file with information such as name, email, college, degree, year of pass out, and skills.
Neo4j Integration: Automatically creates nodes for Person, College, Degree, Year of Passout, and Skill, with relationships between them:
STUDIED_AT: Links Person to College
HAS_DEGREE: Links Person to Degree
PASSED_OUT: Links Person to YearOfPassout
HAS_SKILL: Links Person to Skill
View Data: Fetch and display the stored data from Neo4j.

Setup
Requirements
Python 3.x
Flask
Neo4j
Pandas
Neo4j Python Driver


Usage
Upload a CSV file with the following columns:

Name
Email
College
Year of Passout
Degree
Skills (comma-separated)
After uploading, data will be stored in the Neo4j database.

View the data on the /data page.


python program for connect neo4j and csv file to sore and fetch the data


from flask import Flask, request, render_template, redirect, url_for
import pandas as pd
from neo4j import GraphDatabase

app = Flask(__name__)

# Configure Neo4j connection
uri = "neo4j+s://41c92d28.databases.neo4j.io:7687"  # or your Neo4j cloud URI
username = "neo4j"              # your Neo4j username
password = "Ails-B9U4X62rw2_nckOkN6SDRMTjERi0vZGuZ7fWrk"   

# Initialize Neo4j Driver
neo4j_driver = GraphDatabase.driver(uri, auth=(username, password))

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)

    # Process the CSV file
    df = pd.read_csv(file)

    with neo4j_driver.session() as session:
        for index, row in df.iterrows():
            # Create or merge College Node
            session.run(
                "MERGE (c:College {name: $college})",
                college=row['College']
            )

            # Create or merge Year of Passout Node
            session.run(
                "MERGE (y:YearOfPassout {year: $year})",
                year=int(row['Year of Passout'])
            )

            # Create or merge Degree Node
            session.run(
                "MERGE (d:Degree {name: $degree})",
                degree=row['Degree']
            )

            # Create or merge Skills Nodes (handling multiple skills if needed)
            skills = row['Skills'].split(',')  # Assuming skills are comma-separated
            for skill in skills:
                session.run(
                    "MERGE (s:Skill {name: $skill})",
                    skill=skill.strip()  # Trim whitespace
                )

            # Create or merge Person Node and Relationships
            session.run(
                """
                MERGE (p:Person {name: $name, email: $email})
                WITH p, $college AS college, $year AS year, $degree AS degree
                MATCH (c:College {name: college}), (y:YearOfPassout {year: year}), (d:Degree {name: degree})
                MERGE (p)-[:STUDIED_AT]->(c)
                MERGE (p)-[:PASSED_OUT]->(y)
                MERGE (p)-[:HAS_DEGREE]->(d)
                WITH p, $skills AS skills
                UNWIND skills AS skill
                MATCH (s:Skill {name: skill})
                MERGE (p)-[:HAS_SKILL]->(s)
                """,
                name=row['Name'],
                email=row['Email'],
                college=row['College'],
                year=int(row['Year of Passout']),
                degree=row['Degree'],
                skills=[skill.strip() for skill in row['Skills'].split(',')]  # Ensure skills are a list
            )

    return redirect(url_for('show_data'))


#for showing the result this code is used

@app.route('/data')
def show_data():
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (n:Person)-[:STUDIED_AT]->(c:College)
            RETURN n.name AS name, n.email AS email, c.name AS college
        """)
        data = [{"name": record["name"], "email": record["email"], "college": record["college"]} for record in result]
    return render_template('data.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)

    
