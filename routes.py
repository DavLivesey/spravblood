from flask import Flask, render_template, Blueprint, url_for
from psql_commans import DBCommands
import collections

db = DBCommands()

main = Blueprint('main', __name__, template_folder='templates')

class Worker:
    def __init__(self, name, department, position, phone, email):
        self.name = name
        self.department = department
        self.position = position
        self.phone = phone
        self.email = email
    
    def get_data(self):
         return (self.name, self.department, self.position, self.phone, self.email)



@main.route("/")
async def index():
    print(url_for('main.index'))
    result_departments = await db.get_dep_list()
    list_departments = []    
    departments = []
    for result in result_departments:
        list_departments.append(result[0])
    for department in list_departments:
        worker_positions = await db.get_workers_of_dep(department)
        workers_list = []
        for worker in worker_positions:
                if worker['name'] in workers_list:
                    for tmp_element in departments:
                        index = departments.index(tmp_element)
                        element = list(tmp_element)
                        if element[0] == worker['name']:
                            if element[2] != worker['position']:
                                positions = [element[2], worker['position']]
                                element[2] = positions
                            else:
                                pass
                            if element[3] != worker['phone']:
                                phones = [element[3], worker['phone']]
                                element[3] = phones
                    departments[index] = element
                else:
                    new_worker = Worker(worker['name'], department, worker['position'], worker['phone'], worker['email'])
                    department_workers = {'worker': ''}
                    workers_list.append(worker['name'])
                    department_workers['worker'] = (department, worker['name'], worker['position'], worker['phone'], worker['email'])
                    departments.append(new_worker.get_data())
    return render_template('index.html', departments=list_departments, workers=departments)


