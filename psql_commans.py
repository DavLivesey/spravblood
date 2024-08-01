#Здесь код взаимодействия непосредственно с БД
from psql import DataBase
from logging import getLogger
from datetime import datetime as dt
from config import ADMIN_CHAT

LOG = getLogger()

class DBCommands:
    #Блок забора информации из старой версии базы в новую
    GET_ALL_WORKERS = 'SELECT fullname FROM workers'
    CHECK_OLD_MAILBOXES = 'SELECT mailbox FROM workers w WHERE w.fullname = $1'
    ADD_MAILBOX_OLD = 'INSERT INTO workmails (worker_id, mail_id) VALUES ((select id from workers w where w.fullname=$1), '\
                        '(select id FROM mailbox m WHERE m.mailbox_name=$2))'
    #Блок получения информации о работниках
    GET_LIST_WORKERS = 'SELECT w.fullname, w.email, w.ad FROM workers w'
    GET_LIST_DEPARTMENTS = 'SELECT dep_name FROM departments ORDER BY dep_name'
    GET_WORKERS_IN_DEP = 'SELECT w.id, w.fullname, p.pos_name, ph.phone_number, w.email, w2.employment from workers w ' \
                            'join workplaces w2 on w.id  = w2.worker_id '\
                            'join departments d on w2.dep_id  = d.id '\
                            'join positions p on w2.pos_id = p.id '\
                            'join connections c on w.id = c.worker_id '\
                            'join phones ph on c.phone_id = ph.id '\
                            'WHERE d.dep_name LIKE $1'
    #Блок изменения информации о работниках
    ADD_NEW_WORKER = 'INSERT INTO workers (fullname) VALUES ($1)'
    ADD_DEP = 'UPDATE workers w SET "department"=$2 WHERE id=$1'
    DELETE_WORKER = 'DELETE FROM workers WHERE id=$1'
    VIEW_WORKER = 'SELECT * FROM workers w WHERE w.fullname LIKE $1' 
    VIEW_WORKER_ON_ID = 'SELECT * FROM workers WHERE id=$1'
    VIEW_WORKER_POSITIONS = 'SELECT w.fullname, d.dep_name, p.pos_name FROM workplaces wp ' \
                            'JOIN workers w ON wp.worker_id = w.id ' \
                            'JOIN departments d ON wp.dep_id =d.id ' \
                            'JOIN positions p ON wp.pos_id =p.id ' \
                            'WHERE w.fullname = $1'
    VIEW_WORKER_POSITIONS_ID = 'SELECT w.fullname, d.dep_name, p.pos_name FROM workplaces wp ' \
                            'JOIN workers w ON wp.worker_id = w.id ' \
                            'JOIN departments d ON wp.dep_id =d.id ' \
                            'JOIN positions p ON wp.pos_id =p.id ' \
                            'WHERE w.id = $1'    
    CHECK_WORKER = 'SELECT EXISTS (SELECT * FROM workers w WHERE w.fullname=$1)'
    FIND_WORKER = 'SELECT fullname FROM workers WHERE id = $1'
    EDIT_WORKER_FIO = 'UPDATE workers w SET "fullname"=$2 WHERE id=$1'
    ADD_APTEKA = "UPDATE workers SET APTEKA='Да' where id=$1"
    ADD_HR = "UPDATE workers SET ZKGU='Да' where id=$1"
    ADD_BGU_1 = "UPDATE workers SET BGU_1='Да' where id=$1"
    ADD_BGU_2 = "UPDATE workers SET BGU_2='Да' where id=$1"
    ADD_DIETA = "UPDATE workers SET DIETA='Да' where id=$1"
    ADD_MIS = "UPDATE workers SET MIS='Да' where id=$1"
    ADD_TIS = "UPDATE workers SET TIS='Да' where id=$1"
    ADD_SED = "UPDATE workers SET SED='Да' where id=$1"
    DELETE_APTEKA = "UPDATE workers SET APTEKA='Нет' where id=$1"
    DELETE_HR = "UPDATE workers SET ZKGU='Нет' where id=$1"
    DELETE_BGU_1 = "UPDATE workers SET BGU_1='Нет' where id=$1"
    DELETE_BGU_2 = "UPDATE workers SET BGU_2='Нет' where id=$1"
    DELETE_DIETA = "UPDATE workers SET DIETA='Нет' where id=$1"
    DELETE_MIS = "UPDATE workers SET MIS='Нет' where id=$1"
    DELETE_TIS = "UPDATE workers SET TIS='Нет' where id=$1"
    DELETE_SED = "UPDATE workers SET SED='Нет' where id=$1"
    EDIT_EMAIL = 'UPDATE workers SET EMAIL=$2 where id=$1'
    ADD_NEW_PHONE = 'INSERT INTO phones (phone_number) VALUES ($1)'
    ADD_PHONE = 'INSERT INTO connections (worker_id, phone_id) '\
                'VALUES ($1, (select id FROM phones WHERE phones.phone_number=$2))'
    GET_PHONE = 'SELECT p.phone_number FROM connections c '\
                  'JOIN workers w ON c.worker_id = w.id '\
                  'JOIN phones p ON c.phone_id =p.id '\
                  'WHERE w.fullname = $1'
    CHECK_PHONE = 'SELECT EXISTS (SELECT p.phone_number FROM connections c '\
                  'JOIN workers w ON c.worker_id = w.id '\
                  'JOIN phones p ON c.phone_id =p.id '\
                  'WHERE w.id = $1)'
    CHECK_PHONE_LIST = 'SELECT EXISTS (SELECT p.id FROM phones p WHERE p.phone_number = $1)'
    REMOVE_PHONE = 'DELETE FROM connections c WHERE c.worker_id=$1 and '\
                        'c.phone_id=(SELECT id FROM phones p WHERE p.phone_number=$2)'
    ADD_MAILBOX = 'INSERT INTO workmails (worker_id, mail_id) '\
                'VALUES ($1, (select id FROM mailbox m WHERE m.mailbox_name=$2))'
    GET_MAILBOX = 'SELECT m.mailbox_name FROM workmails wm '\
                  'JOIN workers w ON wm.worker_id = w.id '\
                  'JOIN mailbox m ON wm.mail_id =m.id '\
                  'WHERE w.fullname = $1'
    CHECK_MAILBOX = 'SELECT EXISTS (SELECT m.mailbox_name FROM workmails wm '\
                  'JOIN workers w ON wm.worker_id = w.id '\
                  'JOIN mailbox m ON wm.mail_id =m.id '\
                  'WHERE w.id = $1'
    CHECK_MAILBOX_LIST = 'SELECT EXISTS (SELECT m.id FROM mailbox m WHERE m.mailbox_name= $1)'
    REMOVE_MAILBOX = 'DELETE FROM workmails wm WHERE wm.worker_id=$1 and '\
                        'wm.mail_id=(SELECT id FROM mailbox m WHERE m.mailbox_name=$2)'
    ADD_AD = 'UPDATE workers SET "ad"=$2 WHERE id=$1'
    ADD_NEW_POSITION = 'INSERT INTO positions (pos_name) VALUES ($1)'
    JOIN_POSITION = 'INSERT INTO workplaces (worker_id, pos_id, dep_id) ' \
                    'VALUES ($1, (select id FROM positions p WHERE p.pos_name=$2), '\
                    '(SELECT id FROM departments d WHERE d.dep_name=$3))'
    CHECK_IS_POSITION = 'SELECT EXISTS (SELECT * FROM positions p WHERE p.pos_name=$1)'
    LEAVE_POSITION = 'DELETE FROM workplaces wp WHERE wp.worker_id=$1 and '\
                        'wp.pos_id=(SELECT id FROM positions p WHERE p.pos_name=$2) and '\
                        'wp.dep_id=(SELECT id FROM departments d WHERE d.dep_name=$3)'
    ADD_NEW_DEP = 'INSERT INTO departments (dep_name) VALUES ($1)'
    CHECK_IS_DEP = 'SELECT EXISTS (SELECT * FROM departments WHERE dep_name LIKE $1)'
    
    #Блок работы с сертификатами
    ADD_NEW_SERT = 'INSERT INTO sertificates (worker_id, center_name, serial_number, date_start, date_finish)' \
                        'VALUES ($1, $2, $3, $4, $5)'
    CHECK_SERT = 'SELECT * FROM sertificates s JOIN workers w ON w.id=$1 WHERE s.worker_id=$1'
    CHECK_SERT_FIN = "SELECT worker_id, center_name, serial_number, DATE_FINISH FROM sertificates "\
                     "WHERE current_date + interval '30 day' > DATE_FINISH"    
    PASSED_FLESH = 'UPDATE sertificates SET "presence"=false where worker_id=$1'
    #Блок работы с пользователями
    ADD_NEW_USER = 'INSERT INTO users (first_name, last_name, username, user_id, role) VALUES ($1, $2, $3, $4, $5)'
    CHECK_USER = 'SELECT EXISTS (SELECT * FROM users u WHERE u.user_id=$1)'
    CHECK_USER_ROLE = 'SELECT role from users u WHERE u.user_id=$1'
    UPDATE_USER_ROLE = 'UPDATE users u SET "role"=$2 where u.user_id=$1'
    BAN_USER = 'UPDATE users u SET "ban"=true where u.user_id=$1'
    UNBAN_USER = 'UPDATE users u SET "ban"=false where u.user_id=$1'
    CHECK_USER_BAN = 'SELECT ban from users u WHERE u.user_id=$1'
    
    async def get_all_workers(self):
        workers_list = await DataBase.execute(self.GET_ALL_WORKERS, fetch=True)
        return workers_list
    
    async def get_dep_list(self):
        dep_list = await DataBase.execute(self.GET_LIST_DEPARTMENTS, fetch=True)
        return dep_list
    
    async def get_workers_of_dep(self, dep):
        workers_positions = await DataBase.execute(self.GET_WORKERS_IN_DEP, dep, fetch=True)
        list_result = []
        num = 0
        for worker in workers_positions:
            result = {'id': '', 'name': '', 'position': '', 'phone': '', 'email': '', 'employment': ''}   
            result['id'] = worker[0]
            result['name'] = worker[1]
            result['position'] = worker[2]
            result['phone'] = worker[3]
            result['email'] = worker[4]
            result['employment'] = worker[5]            
            list_result.append(result)
            num += 1
        return list_result
    
    async def get_list_workers(self):
        workers_list = await DataBase.execute(self.GET_LIST_WORKERS, fetch=True)
        return workers_list
    
    async def get_old_mailbox(self, fullname):
        command = self.CHECK_OLD_MAILBOXES
        arg = fullname
        mailbox = await DataBase.execute(command, arg, fetch=True)
        if mailbox[0][0] != None:
            for mail in mailbox:
                result_mail = mail[0].split("\n")
                if len(result_mail) > 1:
                    for result in result_mail:
                        args = (fullname, result)
                        command = self.ADD_MAILBOX_OLD
                        await DataBase.execute(command, *args, execute=True)
                args = (fullname, result_mail[0])
                command = self.ADD_MAILBOX_OLD
                await DataBase.execute(command, *args, execute=True)

    async def read_worker(self, person):
        #Функция получает лист информации о работнике и формирует несколько списков:
        #1. person - тот же список, что входит в функцию
        #3.list_sert - список сертификатов работника
        #5.sec_list - список закрытых контактов (учетка в компьютер, участие в почтовых рассылках)
            phone_arg = person[1]
            phone_command = self.GET_PHONE
            phone_list = []
            telephones = await DataBase.execute(phone_command, phone_arg, fetch=True)
            mailbox_arg = person[1]
            mailbox_command = self.GET_MAILBOX
            mailbox_list = []
            mailboxes = await DataBase.execute(mailbox_command, mailbox_arg, fetch=True)
            for phone in telephones:
                phone_list.append(phone[0])
            for mail in mailboxes:
                mailbox_list.append(mail[0])
            list_sert = []
            arg_sert = int(person[0])
            command_sert = self.CHECK_SERT
            worker_sert = await DataBase.execute(command_sert, arg_sert, fetch=True)                    #Проверяем сертификаты работника
            sert_list = []                                                                              #Список сертификатов работника
            sert_num = 1
            for sert in worker_sert:                                                                    #Для каждого сертификта формируем отдельную читабельную карточку
                if sert[6]==True:
                    date_start = dt.strftime(sert[4], '%d-%m-%Y')
                    date_finish = dt.strftime(sert[5], '%d-%m-%Y')
                    if sert[5] < dt.today().date():
                        date_item = '❌'
                    else:
                        date_item = '✅'
                    sert_list.append(f'{date_item}№{sert_num}')
                    sert_list.append(f'УЦ - {sert[2]}')
                    sert_list.append(f'серийный номер - {sert[3]}')
                    sert_list.append(f'Начало действия - {date_start}')
                    sert_list.append(f"Окончание действия - {date_finish}\n")
                    sert_num += 1
            list_sert = '\n'.join(sert_list)
            return (person, list_sert, phone_list, mailbox_list)
            
    async def add_new_worker(self, fullname, user_id):
        #Создание записи о работнике
        arg = fullname
        command_1 = self.CHECK_WORKER
        command_2 = self.ADD_NEW_WORKER
        worker_boolean = await DataBase.execute(command_1, arg, fetchval=True)
        if worker_boolean:
            await self.view_worker(fullname, user_id)
            return False
        else:
            await DataBase.execute(command_2, arg, execute=True)
            return True

    async def check_worker(self, fullname, user):
        #Проверка наличия записи о сотруднике по ФИО
        arg = f"%{fullname}%"
        command = self.VIEW_WORKER
        result = await DataBase.execute(command, arg, fetch=True)
        worker = result
        if len(worker) > 1:
             pass
        else:
            return worker[0]

    async def view_worker(self, fullname):
        #Просмотр карточки работника без ID
        arg = f"%{fullname}%"
        view_command = self.VIEW_WORKER
        position_command = self.VIEW_WORKER_POSITIONS
        worker = await DataBase.execute(view_command, arg, fetch=True) 
        if len(worker) > 0:         
            for work in worker:
                position = await DataBase.execute(position_command, work[1], fetch=True)
                reading_result = await self.read_worker(work)
                return (reading_result, position)
    
    async def get_worker_card(self, id):
        #Просмотр карточки уволенного сотрудника
        arg = id
        command = self.VIEW_WORKER_ON_ID
        position_command = self.VIEW_WORKER_POSITIONS_ID
        worker = await DataBase.execute(command, arg, fetch=True)
        position = await DataBase.execute(position_command, arg, fetch=True)
        reading_result = await self.read_worker(worker[0])
        return worker[0]


    async def view_worker_with_id(self, fullname, user, role):
        #Просмотр карточки работника с ID. Требуется для функций редактирования
        arg = f"%{fullname}%"
        view_command = self.VIEW_WORKER
        position_command = self.VIEW_WORKER_POSITIONS
        worker = await DataBase.execute(view_command, arg, fetch=True)         
        if len(worker) > 0:
            persons_id_list = []
            for work in worker:
                position = await DataBase.execute(position_command, work[1], fetch=True)        
                reading_result = await self.read_worker(work)
                message = await self.make_answer(reading_result[0], reading_result[1], reading_result[2], \
                                                 reading_result[3], reading_result[4], reading_result[5],\
                                                     reading_result[6], role, position)
                persons_id_list.append(reading_result[0][0])
            return persons_id_list
        else:
            return False
        
    async def view_worker_for_edition(self, id):
        #Просмотр карточки работника по ID
        arg = int(id)
        command = self.VIEW_WORKER_ON_ID
        worker = await DataBase.execute(command, arg, fetch=True)
        person_data = {'name': worker[0][1], '1С-Аптека': worker[0][2], '1С_ЗКГУ': worker[0][3], '1С_БГУ 1.0': worker[0][4],\
                       '1С_БГУ 2.0': worker[0][5], '1С_Диетпитание': worker[0][6], 'МИС': worker[0][7],'ТИС': worker[0][8],\
                        'СЭД': worker[0][9]}
        return person_data

#Далее идет блок добавления/удаления информации о наличии доступа к ИС
    async def plus_MIS(self, id):
        arg = int(id)
        command = self.ADD_MIS
        await DataBase.execute(command, arg, execute=True)

    async def plus_TIS(self, id):
        arg = int(id)
        command = self.ADD_TIS
        await DataBase.execute(command, arg, execute=True)

    async def plus_SED(self, id):
        arg = int(id)
        command = self.ADD_SED
        await DataBase.execute(command, arg, execute=True)

    async def plus_apteka(self, id):
        arg = int(id)
        command = self.ADD_APTEKA
        await DataBase.execute(command, arg, execute=True)

    async def plus_zkgu(self, id):
        arg = int(id)
        command = self.ADD_HR
        await DataBase.execute(command, arg, execute=True)

    async def plus_bgu1(self, id):
        arg = int(id)
        command = self.ADD_BGU_1
        await DataBase.execute(command, arg, execute=True)

    async def plus_bgu2(self, id):
        arg = int(id)
        command = self.ADD_BGU_2
        await DataBase.execute(command, arg, execute=True)

    async def plus_dieta(self, id):
        arg = int(id)
        command = self.ADD_DIETA
        await DataBase.execute(command, arg, execute=True)

    async def del_MIS(self, id):
        arg = int(id)
        command = self.DELETE_MIS
        await DataBase.execute(command, arg, execute=True)

    async def del_TIS(self, id):
        arg = int(id)
        command = self.DELETE_TIS
        await DataBase.execute(command, arg, execute=True)

    async def del_SED(self, id):
        arg = int(id)
        command = self.DELETE_SED
        await DataBase.execute(command, arg, execute=True)

    async def del_apteka(self, id):
        arg = int(id)
        command = self.DELETE_APTEKA
        await DataBase.execute(command, arg, execute=True)

    async def del_zkgu(self, id):
        arg = int(id)
        command = self.DELETE_HR
        await DataBase.execute(command, arg, execute=True)

    async def del_bgu1(self, id):
        arg = int(id)
        command = self.DELETE_BGU_1
        await DataBase.execute(command, arg, execute=True)

    async def del_bgu2(self, id):
        arg = int(id)
        command = self.DELETE_BGU_2
        await DataBase.execute(command, arg, execute=True)

    async def del_dieta(self, id):
        arg = int(id)
        command = self.DELETE_DIETA
        await DataBase.execute(command, arg, execute=True)
#Конец блока о доступе к ИС

#Блок работы с информацией о работнике
    async def edit_email(self, id, email):
        args = int(id), email
        command = self.EDIT_EMAIL
        await DataBase.execute(command, *args, execute=True)
    
    async def edit_fio(self, worker_id, fio):
        command = self.EDIT_WORKER_FIO
        args = (int(worker_id), fio)
        await DataBase.execute(command, *args, execute=True)

    async def add_department(self, worker_id, dep):
        command = self.ADD_DEP
        args = (int(worker_id), dep)
        await DataBase.execute(command, *args, execute=True)

    async def add_telephone(self, worker_id, telephone):
        try:
            int(telephone)
            check_com = self.CHECK_PHONE_LIST
            check_arg = telephone
            add_com = self.ADD_NEW_PHONE
            add_arg = telephone
            command = self.ADD_PHONE
            args = (int(worker_id), telephone)
            check_result = await DataBase.execute(check_com, check_arg, fetchval=True)
            if check_result == False:
                await DataBase.execute(add_com, add_arg, execute=True)
            await DataBase.execute(command, *args, execute=True)
            return True
        except ValueError:
            return False

    
    async def remove_telephone(self, worker_id, telephone):
        command = self.REMOVE_PHONE
        args = (int(worker_id), telephone)
        await DataBase.execute(command, *args, execute=True)
        return True
        

    async def add_ad(self, worker_id, ad):
        command = self.ADD_AD
        args = (int(worker_id), ad)
        await DataBase.execute(command, *args, execute=True)
    
    async def add_mailbox(self, worker_id, mailbox):
        check_command = self.CHECK_MAILBOX_LIST
        command = self.ADD_MAILBOX
        args = (int(worker_id), mailbox)
        check_arg = (mailbox)
        mail_exist = await DataBase.execute(check_command, check_arg, fetchval=True)
        if mail_exist:
            await DataBase.execute(command, *args, execute=True)

    
    async def remove_mailbox(self, worker_id, mailbox):
        command = self.REMOVE_MAILBOX
        args = (int(worker_id), mailbox)
        await DataBase.execute(command, *args, execute=True)

    async def del_worker(self, id):
        arg = int(id)
        command = self.DELETE_WORKER
        await DataBase.execute(command, arg, execute=True)
        return True
    
    async def check_position(self, pos_name):
        arg = pos_name
        command = self.CHECK_IS_POSITION
        result = await DataBase.execute(command, arg, fetchval=True)
        return result
    
    async def check_dep(self, dep_name):
        arg = f"%{dep_name}%"
        command = self.CHECK_IS_DEP
        result = await DataBase.execute(command, arg, fetchval=True)
        return result

    async def join_position(self, worker_id, pos_name, dep_name):
        # Функция добавляет сотруднику должность в подразделении, если таковые существуют
        existing = 0
        pos_exist = await self.check_position(pos_name)
        dep_exist = await self.check_dep(dep_name)
        if pos_exist:
            if dep_exist:
                args = (int(worker_id), pos_name, dep_name)
                command = self.JOIN_POSITION
                await DataBase.execute(command, *args, execute=True)
            else:
                existing = 1    
        else:
            existing = 2
        return existing
    
    async def leave_position(self, worker_id, pos_name, dep_name):
        # Функция убирает сотруднику должность в подразделении, если таковые существуют
        existing = 0
        pos_exist = await self.check_position(pos_name)
        dep_exist = await self.check_dep(dep_name)
        if pos_exist:
            if dep_exist:
                args = (int(worker_id), pos_name, dep_name)
                command = self.LEAVE_POSITION
                await DataBase.execute(command, *args, execute=True)
            else:
                existing = 1
                return existing
        else:
            existing = 2
        return existing
    
    async def add_new_dep(self, dep_name):
        arg = dep_name
        command = self.ADD_NEW_DEP
        dep_exist = self.check_dep(dep_name)
        if not dep_exist:
            await DataBase.execute(command, arg, execute=True)
            return True
        else:
            return False
        
    async def add_new_pos(self, pos_name):
        arg = pos_name
        command = self.ADD_NEW_POSITION
        dep_exist = self.check_dep(pos_name)
        if not dep_exist:
            await DataBase.execute(command, arg, execute=True)
            return True
        else:
            return False

#Блок функций работы с сертификатами
    async def add_new_sert(self, worker_id, center_name, serial_number, date_start, date_finish):
        args = (int(worker_id), center_name, serial_number, date_start, date_finish)
        command = self.ADD_NEW_SERT
        await DataBase.execute(command, *args, execute=True)

    '''async def sert_ends(self, user):
        worker_command = self.CHECK_SERT_FIN
        sertificates = await DataBase.execute(worker_command, fetch=True)
        await bot.send_message(chat_id=user, text=f'Сертификаты со скорым окончанием сроком действия:')
        for sert in sertificates:
            arg = sert[0]
            command = self.FIND_WORKER
            result = await DataBase.execute(command, arg, fetch=True)
            worker = result
            date_finish = dt.strftime(sert[3], '%d-%m-%Y')
            if sert[3] < dt.today().date():
                date_item = '❌'
            else:
                date_item = '✅'
            await bot.send_message(chat_id=user, text=f'{worker[0][0]}\nЦентр сертификации - {sert[1]} \n' \
                                   f'Серийный номер - {sert[2]}\nДата окончания - {date_item}{date_finish}')
'''
    async def pass_ecp(self, worker_id):
        command = self.PASSED_FLESH
        arg = worker_id
        await DataBase.execute(command, arg, execute=True)

#Блок функций работы с пользователями бота
    async def add_user(self, first_name, last_name, username, user_id, role):
        command = self.ADD_NEW_USER
        args = (first_name, last_name, username, user_id, role)
        await DataBase.execute(command, *args, execute=True)
    
    async def check_user(self, user_id):
        arg = user_id
        command = self.CHECK_USER
        user_boolean = await DataBase.execute(command, arg, fetchval=True)
        if user_boolean:
            return True
    
    async def check_user_role(self, user_id):
        arg = user_id
        command = self.CHECK_USER_ROLE
        role = await DataBase.execute(command, arg, fetchval=True)
        return role
    
    async def ban_user(self, user_id):
        arg = user_id
        command = self.BAN_USER
        await DataBase.execute(command, arg, execute=True)
    
    async def unban_user(self, user_id):
        arg = user_id
        command = self.UNBAN_USER
        await DataBase.execute(command, arg, execute=True)
    
    async def check_ban(self, user_id):
        arg = user_id
        command = self.CHECK_USER_BAN
        return await DataBase.execute(command, arg, fetchval=True)
    
    async def edit_user_role(self, user_id, role):
        args = (user_id, role)
        command = self.UPDATE_USER_ROLE
        await DataBase.execute(command, *args, execute=True)