import os
import jwt
import json
import logging
import requests
from flask import Flask, request, jsonify
from typing import Optional
from dataclasses import dataclass, asdict

HASURA_URL = "http://ec2-54-90-184-209.compute-1.amazonaws.com:8080/v1/graphql"
HASURA_HEADERS = {"X-Hasura-Admin-Secret": "Welcome123"}

################
# GRAPHQL CLIENT
################

@dataclass
class Client:
    url: str
    headers: dict

    def run_query(self, query: str, variables: dict={}, extract=False):
        request = requests.post(
            self.url,
            headers=self.headers,
            json={"query": query, "variables": variables},
        )
        assert request.ok, f"Failed with code {request.status_code}"
        return request.json()

    create_user = lambda self, name, team: self.run_query(
        """
            mutation insert_training_employee_one($name: String!, $team: String!) {
                insert_training_employee_one(object: {name: $name, team: $team}) {
                    id
                    name
                    team
                }
            }
        """,
        {"name": name, "team": team}
    )

    get_user_by_name = lambda self, name: self.run_query(
        """
            query get_emaployee_by_name($name: String) {
                training_employee(where: {name: {_eq: $name}}, limit: 1) {
                    id
                    name
                    team
                }
            }
        """,
        {"name": name}
    )

    create_course = lambda self, course_name, duration_in_days: self.run_query(
        """
            mutation insert_training_course_one($course_name: String!, $duration_in_days: Int!) {
                insert_training_course_one(object: {course_name: $course_name, duration_in_days: $duration_in_days}) {
                    course_name
                    duration_in_days
                    id
                }
            }
        """,
        {"course_name": course_name, "duration_in_days": duration_in_days}
    )

    get_course = lambda self: self.run_query(
        """
            query get_course {
                training_course {
                    course_name
                    duration_in_days
                    id
                }
            }
        """
    )

    get_course_by_name = lambda self, course_name: self.run_query(
        """
            query get_course_by_name($course_name: String = "") {
                training_course(where: {course_name: {_eq: $course_name}}) {
                    course_name
                    duration_in_days
                    id
                }
            }
        """,
        {"course_name": course_name}
    )

    create_employee_course_mapping = lambda self, employee_id, course_id: self.run_query(
        """
            mutation create_employee_course_mapping($course_id: Int!, $employee_id: Int!) {
                insert_training_employee_course_mapping_one(object: {course_id: $course_id, employee_id: $employee_id}) {
                    id
                    employee_id
                    course_id
                }
            }
        """,
        {"employee_id": employee_id, "course_id": course_id}
    )


#############
# DATA MODELS
#############

@dataclass
class RequestMixin:
    @classmethod
    def from_request(cls, request):
        """
        Helper method to convert an HTTP request to Dataclass Instance
        """
        return cls(**request)

    def to_json(self):
        return json.dumps(asdict(self))

@dataclass
class CreateUserOutput(RequestMixin):
    id: int
    name: str
    team: str

@dataclass
class UserTeam(RequestMixin):
    name: str
    team: str

@dataclass
class CreateCourseOutput(RequestMixin):
    id: int
    course_name: str
    duration_in_days: str

@dataclass
class Course(RequestMixin):
    course_name: str
    duration_in_days: str

@dataclass
class EmployeeCourse(RequestMixin):
    name: str
    course_name: str

##############
# MAIN SERVICE
##############

app = Flask(__name__)

@app.route("/employees", methods=["POST"])
def add_employee():
    args = UserTeam.from_request(request.get_json())
    client = Client(url=HASURA_URL, headers=HASURA_HEADERS)
    user_response = client.create_user(args.name, args.team)
    if user_response.get("errors"):
        return {"message": user_response["errors"][0]["message"]}, 400
    else:
        user = user_response["data"]["insert_training_employee_one"]
        return CreateUserOutput(**user).to_json()

@app.route("/employee", methods=["GET"])
def get_employee():
    name = request.args.get('name', default = 1, type = str)
    client = Client(url=HASURA_URL, headers=HASURA_HEADERS)
    user_response = client.get_user_by_name(name)
    if user_response.get("errors"):
        return {"message": user_response["errors"][0]["message"]}, 400
    else:
        user = user_response["data"]["training_employee"]
        return jsonify(user)

@app.route("/courses", methods=["POST"])
def add_course():
    args = Course.from_request(request.get_json())
    client = Client(url=HASURA_URL, headers=HASURA_HEADERS)
    course_response = client.create_course(args.course_name, args.duration_in_days)
    if course_response.get("errors"):
        return {"message": course_response["errors"][0]["message"]}, 400
    else:
        course = course_response["data"]["insert_training_course_one"]
        return CreateCourseOutput(**course).to_json()

@app.route("/courses", methods=["GET"])
def get_courses():
    client = Client(url=HASURA_URL, headers=HASURA_HEADERS)
    course_response = client.get_course()
    if course_response.get("errors"):
        return {"message": course_response["errors"][0]["message"]}, 400
    else:
        course = course_response["data"]["training_course"]
        return jsonify(course)

@app.route("/employeeCourseMapping", methods=["POST"])
def add_employee_course_mapping():
    args = EmployeeCourse.from_request(request.get_json())
    client = Client(url=HASURA_URL, headers=HASURA_HEADERS)
    user_response = client.get_user_by_name(args.name)
    print(user_response)
    if user_response.get("errors"):
        return {"message": user_response["errors"][0]["message"]}, 400
    else:
        course_response = client.get_course_by_name(args.course_name)
        print(course_response)
        if course_response.get("errors"):
            return {"message": course_response["errors"][0]["message"]}, 400
        else:
            employee_course_response = client.create_employee_course_mapping(user_response["data"]["training_employee"][0]["id"], course_response["data"]["training_course"][0]["id"])
            print(employee_course_response)
            if employee_course_response.get("errors"):
                return {"message": employee_course_response["errors"][0]["message"]}, 400
            else:
                employee_course_mapping = employee_course_response["data"]["insert_training_employee_course_mapping_one"]
                return jsonify(employee_course_mapping)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")