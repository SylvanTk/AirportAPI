import uuid
import jsonschema
import simplejson
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from flask import Flask, request
from flask_restful import Resource, Api

Base = declarative_base()

class Timetable(Base):
    __tablename__ = 'timetable'
    id = Column(String, primary_key = True, nullable = False)
    company = Column(String)
    flight_number = Column(String)
    destination = Column(String)
    departure_time = Column(String)
    arrival_time = Column(String)
    validity_start = Column(String)
    validity_end = Column(String)
    airplane_type = Column(String)
    mo = Column(Integer, nullable = False, default = 0)
    tu = Column(Integer, nullable = False, default = 0)
    we = Column(Integer, nullable = False, default = 0)
    th = Column(Integer, nullable = False, default = 0)
    fr = Column(Integer, nullable = False, default = 0)
    sa = Column(Integer, nullable = False, default = 0)
    su = Column(Integer, nullable = False, default = 0)

SQL_Engine = create_engine('sqlite:///database.db')
Base.metadata.create_all(SQL_Engine)
# вынеси путь до базы в конфиг.
Base.metadata.bind = SQL_Engine
DBSession = sessionmaker(bind = SQL_Engine)
session = DBSession()

#------JSON schemas validator create ------
with open('POST_schema.json','r') as f:
    schema_data = f.read()
POST_schema = simplejson.loads(schema_data)
f.close()

with open('PUT_schema.json','r') as f:
    schema_data = f.read()
PUT_schema = simplejson.loads(schema_data)
f.close()

with open('DELETE_schema.json','r') as f:
    schema_data = f.read()
DELETE_schema = simplejson.loads(schema_data)
f.close()
#------------------------------------------

app = Flask(__name__)
api = Api(app)


class Flights(Resource):
    def get(self):
        all_flights = session.query(Timetable).all()
        parsed_flights = {'flights': []}
        for flight_data in all_flights:
            parsed_flights['flights'].append(
            {
                'id': flight_data.id,
                'company' : flight_data.company,
                'flight_number': flight_data.flight_number,
                'destination' : flight_data.destination,
                'departure_time' : flight_data.departure_time,
                'arrival_time' : flight_data.arrival_time,
                'validity_start' : flight_data.validity_start,
                'validity_end' : flight_data.validity_end,
                'airplane_type' : flight_data.airplane_type,
                'mo' : flight_data.mo,
                'tu' : flight_data.tu,
                'we' : flight_data.we,
                'th' : flight_data.th,
                'fr' : flight_data.fr,
                'sa' : flight_data.sa,
                'su' : flight_data.su 
            }
            )
        SQL_Engine.dispose()
        return parsed_flights

    def post(self):
        json_data = request.get_json(force=True)
        try:
            jsonschema.validate(json_data, POST_schema)
        except jsonschema.exceptions.ValidationError:
            return 400
        data = json_data['data'] 
        for flight_data in data:
            unique_number = str(uuid.uuid4().fields[0])
            flight_data['id'] = unique_number
        session.bulk_insert_mappings(
            Timetable, data
        )
        session.commit()
        return 201

    def put(self):
        json_data = request.get_json(force=True)
        try:
            jsonschema.validate(json_data, POST_schema)
        except jsonschema.exceptions.ValidationError:
            return 400
        data = json_data['data']
        flight_id=data['id']
        row = session.query(Timetable).get(flight_id)
        for k,v in data.items():
            setattr(row, k, v) 
        session.commit()
        return 202

    def delete(self):
        json_data = request.get_json(force=True)
        try:
            jsonschema.validate(json_data, POST_schema)
        except jsonschema.exceptions.ValidationError:
            return 400
        data = json_data['data']
        flight_id = data['id']
        session.query(Timetable).filter(Timetable.id == flight_id).delete(synchronize_session = False)
        return 202
 
api.add_resource(Flights, '/api/flights')

if __name__ == '__main__':
     app.run()
