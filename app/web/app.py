from flask import Flask, jsonify, request
from flask_restful import Api, Resource
import json
from kafka import SimpleProducer, KafkaClient, KafkaConsumer

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