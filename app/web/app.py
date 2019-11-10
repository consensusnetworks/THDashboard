from flask import Flask, jsonify, request
from flask_restful import Api, Resource
import json
from kafka import SimpleProducer, KafkaClient, KafkaConsumer
import psycopg2

app = Flask(__name__)
api = Api(app)

class Condition(Resource):
    def post(self):
        #Step 1 get the posted data
        postedData = request.get_json()
        print(postedData)
        #Step 2 is to read the data
        condition = postedData["condition"]
        print(condition)

        con = psycopg2.connect("host='localhost' dbname='conditions' user='connorsmith' password='smith95'")
        sql = "CREATE TABLE " + str(condition) + """ (
                    id INT PRIMARY KEY NOT NULL, 
                    created_at TIMESTAMPTZ NOT NULL,
                    source TEXT NOT NULL, 
                    original_text TEXT NOT NULL,
                    clean_text TEXT NOT NULL,
                    sentiment NUMERIC NOT NULL,
                    polarity NUMERIC NOT NULL,
                    subjectivity NUMERIC NOT NULL,
                    lang TEXT NOT NULL,
                    favorite_count INT NOT NULL,
                    retweet_count INT NOT NULL, 
                    original_author TEXT NOT NULL,
                    possibly_sensitive VARCHAR(255) NOT NULL,
                    hashtags VARCHAR(255),
                    user_mentions VARCHAR(255),
                    place VARCHAR(255), 
                    place_coord_boundaries VARCHAR(255)
                )"""
        cur = con.cursor()
        cur.execute(sql)
        con.commit()
        cur.close()
            

        kafka = KafkaClient("localhost:9092")
        producer = SimpleProducer(kafka, value_serializer=lambda x: json.dumps(x).encode('utf-8'))      
        producer.send_messages('Conditions', json.dumps(condition).encode('utf-8'))
        print('Sending Condition to Mempool!')



        retJSON = {
            'Message': condition + " successfully added!",
            'Status Code': 200
        }
        return jsonify(retJSON)


api.add_resource(Condition, '/condition')
@app.route('/')
def hello_world():
    return "Hello World!"
if __name__=="__main__":
    app.run(host='0.0.0.0')