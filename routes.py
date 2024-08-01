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
    departments = []    
    list_departments = [dep[0] for dep in result_departments]
    for department in list_departments:
        worker_positions = await db.get_workers_of_dep(department)
        workers_list = []
        employer_data = []
        pos_index = 2
        phone_index = 4
        #print(f"Все сотрудники отдела {department}: {worker_positions}")
        for worker in sorted(worker_positions, key=lambda x: x['name']):                
                #print(f"Работаем над {worker['name']}") 
                if worker['name'] in employer_data:
                    #print(f"Нашли {worker['name']} в списке предыдущих обработанных") 
                    if worker['position'] in employer_data:
                         if worker['phone'] in employer_data:
                              continue
                         else:
                              employer_data.insert(phone_index, worker['phone'])
                    else:
                         employer_data.insert(pos_index, worker['position'])
                else:
                    employer_data = []
                    employer_data.append(worker['id'])
                    employer_data.append(worker['name'])
                    employer_data.append(worker['position'])                    
                    employer_data.append(worker['phone'])
                    if worker['email'] == None:
                         worker['email'] = ''
                    employer_data.append(worker['email'])                    
                    workers_list.append(employer_data)
        for worker in workers_list:
            phone_list = []
            index_list = []
            pos_list = []
            for phone in worker[3:-1:]:
                try:
                    phone_int = int(phone)
                    index_list.append(worker.index(phone))
                    phone_list.append(phone)                        
                except ValueError:
                     pass
            for pos in worker[2:index_list[0]:]:
                pos_list.append(pos)
            worker_data = {'id': worker[0], 'name': worker[1], 'department': department,'position': pos_list, 'phone': phone_list, 'email': worker[-1]}
            departments.append(worker_data)
    return render_template('index.html', departments=list_departments, workers=departments)

@main.route("/profile/<id>")
async def view_employer_card(id):
    person = await db.get_worker_card(int(id))
    print(person)
    access_dict = {}
    person_list = []
    access_dict['1С-Аптека'] = person[2]
    access_dict['1С_Диетпитание'] = person[6]
    access_dict['1С_ЗКГУ'] = person[3]
    access_dict['1С_БГУ 1.0'] = person[4]
    access_dict['1С_БГУ 2.0'] = person[5]
    access_dict['СЭД'] = person[9]
    access_dict['МИС'] = person[7]
    access_dict['ТИС'] = person[8]
    for key, value in access_dict.items():                                                 #Если в словаре access_dict значение было изменено,
        if value:                                                                          #добавляет в список название ключа
            person_list.append(key)
    return render_template('profile.html', worker=person, accesses=person_list)