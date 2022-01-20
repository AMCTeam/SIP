import tornado.web
import json
import os
from datetime import datetime
import csv
import xlrd
from queries import SQLqueries
from messages import success
from settings import Settings as set
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from os.path import basename
Settings = set()
from pymysql.err import IntegrityError
import re
query_obj = SQLqueries()
success_obj = success()
from datetime import date
from datetime import timedelta
import threading
import pymysql
from settings import MYSQLConfiguration
import schedule
import time
import traceback
from logger import LoggerClass
from time import strptime
from Crypto.Cipher import AES
import base64
import binascii
import urllib.parse
from Crypto import Random
import hashlib
db_obj = MYSQLConfiguration()
   
def run_in_thread(fn):
    def run(*k, **kw):
        t = threading.Thread(target=fn, args=k, kwargs=kw)
        t.start()
    return run

class CustomStaticFileHandler(tornado.web.StaticFileHandler):
    def set_default_headers(self):
        self.set_header('X-Content-Type-Options', 'nosniff')

class CommonBaseHandler(tornado.web.RequestHandler):

    # def __init__(self, req):
    #     tornado.web.RequestHandler.__init__(self, req)
    #     print("Common base constructor")

    def initialize(self):
        self.set_headerlist()
        logger_ob = LoggerClass()
        logger_ob.create_logger()

    def return_self(self, val):
        return val

    @property
    def db(self):
        return self.application.db

    @property
    def rsa_keys(self):
        return self.application.rsa_keys

    def clr_cookie(self, cname):
        self.clear_cookie(cname)
        self.set_secure_cookie(cname, 'None',  httponly=True)

    def set_headerlist(self):
        self.set_header('X-FRAME-OPTIONS', 'Deny')
        self.set_header('X-Content-Type-Options', 'nosniff')
        self.set_header('X-XSS-Protection', '1; mode=block')
        # self.set_header('Content-Type', 'application/json; charset=UTF-8')
        # self.set_header("Access-Control-Allow-Origin", "http://myraatechnologies.com:8003")
        # self.set_header("Access-Control-Allow-Origin", "http://52.66.201.73:8003")
        # self.set_header("Access-Control-Allow-Origin", "http://localhost:8003")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    # def log_creation(self, fun, id=0, in_parameter=0):
    #     try:
    #         write_data = [str(id)]
    #         write_data.append(str(fun))
    #         write_data.append(self.get_date_time())
    #         if isinstance(in_parameter, list) or isinstance(in_parameter, tuple):
    #             write_data.append('|'.join([str(ele) for ele in in_parameter]))
    #         elif isinstance(in_parameter, dict):
    #             write_data.append('|'.join([str(ele) for ele in in_parameter.items()]))
    #         elif isinstance(in_parameter, str):
    #             write_data.append(in_parameter)
    #         elif isinstance(in_parameter, int) or isinstance(in_parameter, float):
    #             write_data.append(str(in_parameter))
    #         else:
    #             pass

    #         with open('/home/ubuntu/logs/' + str(id) + '.log', 'a') as log_file:
    #             log_file.write('|'.join(write_data))
    #             log_file.write('\n')
    #     except Exception as e:
    #         pass

    #     return True

    # def get_date_time(self):
    #     now = datetime.now()
    #     return '-'.join([str(now.day), str(now.month), str(now.year), str(now.hour), str(now.minute), str(now.second)])

    def import_xlsx(self, xlsx_path):
        total_rows = []
        with open(xlsx_path) as f:
            reader = csv.reader(f)
            for row in reader:
                total_rows.append(row)
        return total_rows
        pass

    def open_excel(self, to_read):
        """
        Open an Excel workbook and read rows from first sheet into sublists
        """
        workbook = xlrd.open_workbook(to_read)
        sheet = workbook.sheet_by_index(0)
        plu = []
        for row in range(sheet.nrows):
            plu.append([sheet.cell(row, col).value for col in range(sheet.ncols)])
        return plu

    def change_format(self, data):
        dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(data) - 2)
        print("***********************************")
        print(dt)
        return dt

    def write_data(self,xlsx_name, table_name='employee_rl'):
        rows = self.open_excel( os.getcwd() + '/' + xlsx_name)
        columns = [ele.lower().strip().replace(' ', '_').replace('-', '_') for ele in rows[0]]
        columns.append("created_at")
        columns.append("updated_at")
        rows.pop(0)
        
        placeholders = "LAST_DAY(STR_TO_DATE(%s, '%%Y-%%m-%%d %%T')), "
        placeholders += ", ".join(['%s'] * (len(rows[0]) + 1))
        date_str = [columns.index(ele) for ele in columns if 'date' in ele]
        columns = ', '.join(columns)
        print(columns)
        cursor = self.db.cursor()
        print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
        print(date_str)
        for ele in range(len(rows)):
            for ind in range(len(date_str)):
                try:
                    print("uuuuuuuuuuuuuuuuuuuuuuuuuuuuuu")
                    rows[ele][date_str[ind]] = self.change_format(rows[ele][date_str[ind]])
                except Exception as e:
                    continue
            try:

                rows[ele].append(datetime.now())
                rows[ele].append(datetime.now())
                # emp_adid = rows[ele][2]
                # # emp_dob_date_str = rows[ele][12].split(-)[0]
                # # emp_dob_month_str = rows[ele][12].split(-)[1]
                # emp_dob_day = ""
                # if(len(str(rows[ele][12].split('-')[0])) < 2):
                #     emp_dob_day = self.day_string_to_number(str(rows[ele][12].split('-')[0]))
                # else:
                #     emp_dob_day = str(rows[ele][12].split('-')[0])
                # emp_pass = str(rows[ele][1])+'@'+str(emp_dob_day)+str(self.month_string_to_number(rows[ele][12].split('-')[1]))
                # rows[ele].append(emp_pass)
                email_sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (pymysql.escape_string(table_name), pymysql.escape_string(columns), placeholders)
                # print(email_sql)
                print(rows[ele])
                cursor.execute(email_sql, rows[ele])
            except UnicodeError as e:
                print("Unicode Error: "+ str(e))
                self.db.rollback()
                traceback.print_exc()
                return {
                        'status' : 0,
                        'message': "Unicode Decode error, please contact the system administrator for diagnostics and details pertaining to the error."
                    }
            except IntegrityError as e:
                # print("heloooooooooooooooo")
                # print(rows[ele][0])
                # print(rows[ele][1])
                # print(rows[ele][2])
                # print(rows[ele][3])
                # print(rows[ele][4])
                # print(rows[ele][5])
                # print(rows[ele][6])
                # print(rows[ele][7])
                # print(rows[ele][8])
                # print(rows[ele][9])
                # print(rows[ele][10])
                # print(rows[ele][11])
                # print(rows[ele][12])
                # print(rows[ele][13])
                # print(rows[ele][14])
                # print(rows[ele][15])
                # print(rows[ele][16])
                # print(rows[ele][17])
                employee_update_sql = "UPDATE employee_rl SET record_date = LAST_DAY('%s'), work_location = '%s',"\
                "emp_code = '%s', name = '%s',product = '%s',"\
                "cat = '%s',target_no_of_disbursals = '%s',target_disbursal_value = '%s',"\
                "target_net_fee_income = '%s',target_cross_sell = '%s',actual_no_of_disbursals = '%s',"\
                "actual_disbursal_value = '%s',actual_avg_yield_percent = '%s',"\
                "actual_net_fee_income = '%s',actual_cross_sell = '%s',"\
                "non_tech_bounces = '%s',npa_mob = '%s',updated_at = 'NOW()' "\
                "WHERE record_date = LAST_DAY(STR_TO_DATE('%s','%%Y-%%m-%%d %%T')) AND emp_code = '%s'" %(
                rows[ele][0],
                rows[ele][1],
                rows[ele][2],
                rows[ele][3],
                rows[ele][4],
                rows[ele][5],
                rows[ele][6],
                rows[ele][7],
                rows[ele][8],
                rows[ele][9],
                rows[ele][10],
                rows[ele][11],
                rows[ele][12],
                rows[ele][13],
                rows[ele][14],
                rows[ele][15],
                rows[ele][16],
                rows[ele][0],
                pymysql.escape_string(rows[ele][2]))
                print(employee_update_sql)
                cursor = self.db.cursor()
                # print(employee_update_sql)
                cursor.execute(employee_update_sql)
                self.db.commit()
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                pass
                return {
                    'status' : 0,
                    'message': "There was an problem while uploading, please try again after some time."
                }
            finally:
                pass
        self.db.commit()
        cursor.close()
        return {
            'status' : 1,
            'message': "Data written succssfully"
        }

    def get_unique_department(self):
        all_dep_sql = query_obj.get_all_department
        data = self.get_this_data(all_dep_sql)
        return data

    def get_this_data(self, sql):
        data = ""
        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql)
                user_desc = cursor.description
                this_data = cursor.fetchall()
                # cursor.close()
            column_names = [col[0] for col in user_desc]
            data = [dict(zip(column_names, row)) for row in this_data]
        except Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
            if self.db.open:
                self.db.close() 
            self.db.connect()
        finally:
            pass
        if data:
            return data
        else:
            return None

    def create_job(self):
        access_level_data = self.check_level(self.get_secure_cookie('cops_user').decode())[0]
        hire_type = self.get_argument("hire_type", default="", strip=False)
        replacement_for = self.get_argument("replacement_for", default="", strip=False)
        unit = self.get_argument("unit", default="", strip=False)
        department = self.get_argument("department", default="", strip=False)
        sub_department = self.get_argument("sub_department", default="", strip=False)
        role = self.get_argument("role", default="", strip=False)
        job_band = self.get_argument("job_band", default="", strip=False)
        city = self.get_argument("city", default="", strip=False)
        branch = self.get_argument("branch", default="", strip=False)
        reporting_manager = self.get_argument("reporting_manager", default="", strip=False)
        reporting_manager_id = self.get_argument("reporting_manager_id", default="", strip=False)
        it_asset_type = self.get_argument("it_asset_type", default="", strip=False)
        job_type = self.get_argument("job_type", default="", strip=False)
        role_code = self.get_argument("role_code", default="", strip=False)
        hrbp_name = self.get_argument("hrbp", default="", strip=False)
        zone = self.get_argument("zone", default="", strip=False)
        hrbp_email_id = ""
        job_id = ""
        try: 
            # hrbp_email_id = self.get_hrbp_mail(job_details_db[0]['unit'],job_details_db[0]['department'])
            hrbp_email_id = self.get_hrbp_mail(unit, department,sub_department,zone)

        except Exception as e:
            pass
            if hrbp_email_id :
                pass
            else:
                #### New_mail
                pass
        response_data = ''
        create_job_cursor =''
        try:
            create_job_sql = query_obj.create_job % (pymysql.escape_string(hire_type),pymysql.escape_string(replacement_for),pymysql.escape_string(unit),pymysql.escape_string(department),pymysql.escape_string(sub_department),pymysql.escape_string(role),pymysql.escape_string(job_band),pymysql.escape_string(city),pymysql.escape_string(branch),pymysql.escape_string(reporting_manager),pymysql.escape_string(it_asset_type),pymysql.escape_string(job_type),pymysql.escape_string(self.get_secure_cookie("cops_user").decode()),datetime.now(),pymysql.escape_string(hrbp_name),pymysql.escape_string(role_code),pymysql.escape_string(reporting_manager_id))
            create_job_cursor = self.db.cursor()
            create_job_cursor.execute(create_job_sql)
            self.db.commit()
            job_id = create_job_cursor.lastrowid
            create_job_cursor.close()
        except Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
            self.db.rollback()
            create_job_cursor.close()
        finally:
            pass
        try:
            '''
                    Mail :- Mail when requisition is raised by hiring manager
                    Subjective Variables :- Hiring_Manager,  Functional_Designation,  Sub_Department,  City
                    Variables :- Hiring_Manager,  Functional_Designation,  Sub_Department,  City
                    
                    '''
            
            
            #to Respective HRBP
            self.email_send(1,  email_to_cc = '', email_to=hrbp_email_id, Link='', Table='', Candidate_Name='',
                            Candidate_First_Name='', Functional_Designation=role, Sub_Department=sub_department, Fixed_Pay='',
                            Target_Pay='',
                            Variable_Pay='', City=city, Location='', Unit='', Job_Code='',
                            Reporting_Manager_Functional_Designation='', Hiring_Manager_Name=access_level_data['name'], publish_url='',
                            reject_url='',
                            accept_url='', Applicant_ID='', Referral_ID='', Offer_Release_Date='',
                            Offer_Acceptance_Date='',
                            OTP='', Today_Date='', x=0, Document_List='')
            
            #to Hiring Manager
            
            #listener for activity log
            name_activity = str(access_level_data['name'])
            subject_activity = "Job Requisition"
            sentence_activity = name_activity + " posted a job requisition for post of " + str(role) +" -#JC"+str(job_id)
            time_activity =  datetime.now().strftime("%H:%M, %d-%m-%y")
            message_activity = [name_activity, time_activity, subject_activity, sentence_activity]
            self.update_activity_loggy(message_activity)

            self.email_send(2,  email_to_cc = '', email_to= access_level_data['email'], Link='', Table='', Candidate_Name='',
                            Candidate_First_Name='', Functional_Designation=role, Sub_Department=sub_department, Fixed_Pay='',
                            Target_Pay='',
                            Variable_Pay='', City=city, Location='', Unit='', Job_Code='',
                            Reporting_Manager_Functional_Designation='', Hiring_Manager_Name=access_level_data['name'], publish_url='',
                            reject_url='',
                            accept_url='', Applicant_ID='', Referral_ID='', Offer_Release_Date='',
                            Offer_Acceptance_Date='',
                            OTP='', Today_Date='', x=0, Document_List='')

            response_data = json.dumps({
                'success': {
                    'code': success_obj.success_code_created,
                    'message': success_obj.success_job_create
                }
            })

        except Exception as e:
            pass
        return response_data

    def get_dep_sub_dep(self, unique_department):
        master_dept = {}
        for ele in range(len(unique_department)):
            if unique_department[ele]['department'] in master_dept:
                master_dept[unique_department[ele]['department']].append(unique_department[ele]['sub_department'])
            else:
                master_dept[unique_department[ele]['department']] = [unique_department[ele]['sub_department']]
        return master_dept

    def check_level(self,emp_id):
        emp_data = self.get_emp_data_by_empid(emp_id)
        is_RM_data = self.get_data_list(query_obj.is_RM % str(emp_id))
        if emp_data and 'level' in emp_data[0]:
            emp_access_level = { "access_level":emp_data[0]['level'] }
            emp_data.append(emp_access_level)
            if is_RM_data:
                emp_data[0]["is_RM"] = True
            else: emp_data[0]["is_RM"] = None
        else:
            print("Possible 500 error due to employee data returning null. Redirecting ....")
            traceback.print_exc()
            emp_data = None
            self.redirect("/admin/login")
        return emp_data

    def get_live_job_data(self,job_id):
        live_job_sql = query_obj.get_live_job_data % int(job_id)
        data = self.get_this_data(live_job_sql)
        return data

    def get_dep_sub_dep_hrbp(self, unique_department):
        master_dept = {}
        for ele in range(len(unique_department)):
            if unique_department[ele]['department'] in master_dept:
                if unique_department[ele]['sub_department'] in master_dept[unique_department[ele]['department']]:
                    master_dept[unique_department[ele]['department']][unique_department[ele]['sub_department']].append(
                        unique_department[ele]['reporting_manager'] +"//"+unique_department[ele]['emp_code_of_reporting_manager'])
                else:
                    master_dept[unique_department[ele]['department']][unique_department[ele]['sub_department']] = {}
                    if unique_department[ele]['reporting_manager']+"//"+unique_department[ele]['emp_code_of_reporting_manager'] in master_dept[unique_department[ele]['department']][
                        unique_department[ele]['sub_department']]:
                        master_dept[unique_department[ele]['department']][
                            unique_department[ele]['sub_department']].append(
                            unique_department[ele]['reporting_manager']+"//"+unique_department[ele]['emp_code_of_reporting_manager'])
                    else:
                        master_dept[unique_department[ele]['department']][unique_department[ele]['sub_department']] = [
                            unique_department[ele]['reporting_manager']+"//"+unique_department[ele]['emp_code_of_reporting_manager']]
            else:
                master_dept[unique_department[ele]['department']] = {}
                master_dept[unique_department[ele]['department']][unique_department[ele]['sub_department']] = [
                    unique_department[ele]['reporting_manager']+"//"+unique_department[ele]['emp_code_of_reporting_manager']]
        return master_dept

    @run_in_thread
    def email_send(self, mail_type, email_to_cc='', email_to='', Link='', Table='', Candidate_Name='',
                   Candidate_First_Name='', Functional_Designation='', Sub_Department='', Fixed_Pay='',
                   Target_Pay='',
                   Variable_Pay='', City='', Location='', Unit='', Job_Code='',
                   Reporting_Manager_Functional_Designation='', Hiring_Manager_Name='', publish_url='',
                   reject_url='',
                   accept_url='', Applicant_ID='', Referral_ID='', Offer_Release_Date='', Offer_Acceptance_Date='',
                   OTP='', Today_Date='', x=0, Document_List='', d1='', d2='', attachment='',full_unit='',Joining_Bonus='',
                   Job_Band='',Candidate_Age='',Age_Deviation='', Joining_Bonus_Authority_Name='',JB_Refund_Duration='',Department='',
                   Final_Interview_Date='',Final_Interview_Venue='',Final_Interview_Time=''):
        

        Candidate_Name = Candidate_Name.title()
        Hiring_Manager_Name = Hiring_Manager_Name.title()
        Candidate_First_Name = Candidate_First_Name.title()
        

        if mail_type == 0:
            print('Email type 0')
            traceback.print_exc()
        else:
            if mail_type < 17:
                email_to = self.is_employee_email(email_to)
                if not email_to:
                    return

            mess = (mail_obj.mailing_dictionary[mail_type]['message'])

            message = mess.format(mail_type=mail_type, email_to_cc='', email_to=email_to, Link=Link, Table=Table,
                                  Candidate_Name=Candidate_Name, Candidate_First_Name=Candidate_First_Name,
                                  Functional_Designation=Functional_Designation, Sub_Department=Sub_Department,
                                  Fixed_Pay=Fixed_Pay, Target_Pay=Target_Pay, Variable_Pay=Variable_Pay, City=City,
                                  Location=Location, Unit=Unit, Job_Code=Job_Code,
                                  Reporting_Manager_Functional_Designation=Reporting_Manager_Functional_Designation,
                                  Hiring_Manager_Name=Hiring_Manager_Name, publish_url=publish_url,
                                  reject_url=reject_url,
                                  accept_url=accept_url, Applicant_ID=Applicant_ID, Referral_ID=Referral_ID,
                                  Offer_Release_Date=Offer_Release_Date, Offer_Acceptance_Date=Offer_Acceptance_Date,
                                  OTP=OTP, Today_Date=Today_Date, x=x, Document_List=Document_List, d1=d1, d2=d2,
                                  attachment=attachment,Joining_Bonus=Joining_Bonus, Job_Band = Job_Band,Candidate_Age=Candidate_Age,
                                  Age_Deviation=Age_Deviation,Joining_Bonus_Authority_Name=Joining_Bonus_Authority_Name,JB_Refund_Duration=JB_Refund_Duration,
                                  Final_Interview_Date=Final_Interview_Date,Final_Interview_Venue=Final_Interview_Venue,Final_Interview_Time=Final_Interview_Time)

            msg = MIMEMultipart()
            subject = (mail_obj.mailing_dictionary[mail_type]['title'])
            msg['Subject'] = subject.format(mail_type=mail_type, email_to_cc=email_to_cc, email_to=email_to, Link='',
                                            Table='',
                                            Candidate_Name=Candidate_Name, Candidate_First_Name=Candidate_First_Name,
                                            Functional_Designation=Functional_Designation,
                                            Sub_Department=Sub_Department,
                                            Fixed_Pay=Fixed_Pay, Target_Pay=Target_Pay, Variable_Pay=Variable_Pay,
                                            City=City, Location=Location, Unit=Unit, Job_Code=Job_Code,
                                            Reporting_Manager_Functional_Designation=Reporting_Manager_Functional_Designation,
                                            Hiring_Manager_Name=Hiring_Manager_Name, publish_url=publish_url,
                                            reject_url=reject_url, accept_url=accept_url, Applicant_ID=Applicant_ID,
                                            Referral_ID=Referral_ID, Offer_Release_Date=Offer_Release_Date,
                                            Offer_Acceptance_Date=Offer_Acceptance_Date, OTP=OTP, Today_Date=Today_Date,
                                            x=x, Document_List=Document_List, d1=d1, d2=d2, attachment=attachment, full_unit=full_unit, Job_Band = Job_Band,Department=Department)

            msg['From'] = Settings.configs['default_mail_from']

            if isinstance(email_to, list):
                email_to = ", ".join(email_to)
            if isinstance(email_to_cc, list):
                email_to_cc = ", ".join(email_to_cc)

            if 'birlacapital' in email_to or 'birlacapital' in email_to_cc:
                email_to = Settings.configs['default_mail_to']
                email_to_cc = Settings.configs['default_mail_cc']

            # msg['To'] = email_to
            # msg['Cc'] = email_to_cc

            msg['To'] = Settings.configs['default_mail_to']
            msg['Cc'] = Settings.configs['default_mail_cc']

            msg.attach(MIMEText(message, 'html'))

            for a in attachment:
                with open(a, "rb") as fil:
                    part = MIMEApplication(
                        fil.read(),
                        Name=basename(a)
                    )
                part['Content-Disposition'] = 'attachment; filename="%s"' % basename(a)
                msg.attach(part)

            fp = open('BackEnd/ab_fl_email_header2.PNG', 'rb')
            msgImage = MIMEImage(fp.read())
            fp.close()
            msgImage.add_header('Content-ID', '<banner>')
            msg.attach(msgImage)
            if mail_type == 42:
                fp = open('BackEnd/main_banner.jpg', 'rb')
                msgImage = MIMEImage(fp.read())
                fp.close()
                msgImage.add_header('Content-ID', '<job-mailer>')
                msg.attach(msgImage)

            s = Settings.get_smtp_settings()
            s.send_message(msg)
            s.quit()

    def get_hrbp_mail(self, unit, department,sub_department, zone = 'east'):
        # change
        # zone = 'east'
        try:
            hrbp_id = self.get_this_data(query_obj.get_hrbp_details_by_dept_sub_dept % (pymysql.escape_string(unit),pymysql.escape_string(sub_department), pymysql.escape_string(department)))[0][zone]
            hrbp_mail = self.get_this_data(query_obj.get_employee_data_by_id % int(hrbp_id))[0]['email']
            return hrbp_mail
        except  Exception as e:    
            print("Error: "+ str(e))
            traceback.print_exc()

    def get_latest_activity_log_filename(self):        
        day = datetime.now().strftime("%A")
        today = date.today()
        last_monday = today - timedelta(days=today.weekday())
        if day == "Monday":
           last_monday = today
        last_monday = last_monday.strftime("%Y-%m-%d")
        file_name_org = "activity_log-"+last_monday+".txt"
        return file_name_org

    def update_activity_loggy(self, strg=[]):
        file_paths = os.path.dirname(__file__) + '/logs/'
        file_name_org = self.get_latest_activity_log_filename()
        file_name_temp = "activity_log_temp.txt"
        with open(file_paths + file_name_temp,"w+")  as f1, open(file_paths + file_name_org,"r") as f2:
            for i in range (0, len(strg)):
                f1.write (strg[i] + "\n")
            s = f2.readline()
            while s:
                f1.write(s)
                s = f2.readline()
                
        os.remove(file_paths + file_name_org)
        os.rename(file_paths + file_name_temp, file_paths + file_name_org)

    def is_local(self):
        if Settings.configs['host'] == "http://localhost:8080/" :
            return True
        else:
            return False

    def get_data_list(self, sql):
        r = []
        try:
            cursor_list = self.db.cursor()
            data = cursor_list.execute(sql)
            r = [dict((cursor_list.description[i][0], str(value)) for i, value in enumerate(row)) for row in
                 cursor_list.fetchall()]
            cursor_list.close()
        except  Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
        finally:
            pass
        return r


    def is_employee_email(self, email_id):
        emails = self.get_this_data(query_obj.get_emp_email % email_id)

        if emails:
            return emails[0]['email']
        return

    def write_this_data(self,csv_name, table_name):
        rows = self.import_csv(csv_name)
        columns = [ele.lower().strip().replace(' ', '_').replace('-', '_').replace('.', '') for ele in rows[0]]
        rows.pop(0)
        placeholders = ', '.join(['%s'] * (len(rows[0])))
        columns = ', '.join(columns)
        cursor = self.db.cursor()
        for ele in range(len(rows)):
            try:
                email_sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (pymysql.escape_string(table_name), pymysql.escape_string(columns), pymysql.escape_string(placeholders))
                cursor.execute(email_sql, rows[ele])
                
            except UnicodeError as e:
                print("Unicode Error: "+ str(e))
                self.db.rollback()
                traceback.print_exc()
                return {
                        'status' : 0,
                        'message': "Unicode Decode error, please contact the system administrator for diagnostics and details pertaining to the error."
                    }
            except Exception as e:
                print("Error: "+ str(e))
                self.db.rollback()
                traceback.print_exc()
                return {
                        'status' : 0,
                        'message': "There was an problem while uploading, please try again after some time."
                    }
            finally:
                pass
        self.db.commit()
        cursor.close()
        return {
            'status' : 1,
            'message': "Data written succssfully"
        }
    

    def csv_validation(self, csv_name, table_name):
        temporary = csv_name.split(".")
        temp_file_name = temporary[0] + "_temp.csv"
        with open(csv_name, 'r', encoding='utf-8-sig') as input_file, open( temp_file_name , 'wb') as output_file:
            output_file.write(input_file.read().encode('utf-8'))
        csv_name = temp_file_name
        fieldnames = {
        'jd':{"sr_no", "role_code", "unit", "role_title", "short_title", "reporting_manager", "function", "sub_function", "department", "sub_department", "job_band", "job_band_code", "poornata_department",'jd',"status"},
        'employee':{'id', 'employee_id', 'adid', 'ad', 'e_id', 'prefix', 'gender', 'name', 'date_of_joining', 'confirmation_date', 'confirmation_status', 'abfsg_joining_date', 'abg_joining_date', 'birth_date', 'unit', 'function', 'sub_function', 'department', 'sub_department', 'functional_designation', 'job_band', 'job_band_range', 'emp_code_of_reporting_manager', 'reporting_manager', 'zone', 'state', 'city', 'branch', 'branch_code', 'email', 'bhr', 'bhr_email_id', 'employment_status', 'jd_status', 'jd_code', 'created_at', 'updated_at', 'deleted_at', 'department_category', 'password', 'level', 'password_reset_status'},
        'branch': {"sr_no", "unit", "branch_code", "branch_name", "zone", "state", "city", "address"},
        'hrbp' : {"unit", "department", "east", "north", "south", "west", "sub_department"},
        'consultant' : {"consultant_code", "consultant_name"}
        }
        csv_name = self.get_file_sans_header( csv_name, table_name )
        fieldnames_file = self.get_first_row(csv_name)
        if csv_name and fieldnames_file:
            for f in fieldnames_file:
                if f not in fieldnames[table_name]:
                    return
        return csv_name

    def get_file_sans_header(self, csv_name,table_name ):
        fieldnames = {
        'jd':{"sr_no", "role_code", "unit", "role_title", "short_title", "reporting_manager", "function", "sub_function", "department", "sub_department", "job_band", "job_band_code", "poornata_department",'jd'},
        'employee':{'id', 'employee_id', 'adid', 'ad', 'e_id', 'prefix', 'gender', 'name', 'date_of_joining', 'confirmation_date', 'confirmation_status', 'abfsg_joining_date', 'abg_joining_date', 'birth_date', 'unit', 'function', 'sub_function', 'department', 'sub_department', 'functional_designation', 'job_band', 'job_band_range', 'emp_code_of_reporting_manager', 'reporting_manager', 'zone', 'state', 'city', 'branch', 'branch_code', 'email', 'bhr', 'bhr_email_id', 'employment_status', 'jd_status', 'jd_code', 'created_at', 'updated_at', 'deleted_at', 'department_category', 'password', 'level', 'password_reset_status'},
        'branch': {"sr_no", "unit", "branch_code", "branch_name", "zone", "state", "city", "address"},
        'hrbp' : {"unit", "department", "east", "north", "south", "west", "sub_department"},
        'consultant' : {"consultant_code", "consultant_name"}}
        seek = 0
        while seek < 7:
            columns = self.get_first_row(csv_name)
            if columns:
                if columns[0] in fieldnames[table_name]:
                    return csv_name    
            else :
                seek += 1
                csv_name = self.re_write_file(csv_name, seek)
        
        return

    def get_first_row(self, csv_path):
        all_rows = []
        try:
            with open(csv_path) as f:
                reader = csv.reader(f)
                i = 0
                for row in reader:
                    i += 1
                    if i > 1:
                        break
                    all_rows.append(row)
                    columns = [ele.lower().strip().replace(' ', '_').replace('-', '_').replace('.', '') for ele in all_rows[0]]
        except Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
            return
        return columns


    def re_write_file(self, csv_name, seek):
        temporary = csv_name.split(".")
        temp_file_name = temporary[0] + "_temp.csv"
        with open(csv_name, 'rb') as input_file, open( temp_file_name , 'wb') as output_file:
            input_file.seek(seek)
            output_file.write(input_file.read())
        return temp_file_name

    def get_this_data_count(self, sql):
        data = ""
        count = 0
        total_rows = 0
        try:
            cursor_this_data = self.db.cursor()
            with cursor_this_data as cursor:
                count = cursor.execute(sql)
                user_desc = cursor_this_data.description
                this_data = cursor_this_data.fetchall()
                cursor.execute("SELECT FOUND_ROWS()")
                (total_rows,)= cursor_this_data.fetchone()
                cursor_this_data.close()
                column_names = [col[0] for col in user_desc]
                data = [dict(zip(column_names, row)) for row in this_data]
        except Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
        finally:
            pass
        if data:
            return count,total_rows, data
        else:
            return count,total_rows, None

    def date_handler(self,obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            json.JSONEncoder.default(self,obj)

    def check_password_validity(self,password):
        is_valid_password = re.match('(^(?=\S{8,}$)(?=.*?\d)(?=.*?[a-z])(?=.*?[A-Z])(?=.*?[^A-Za-z\s0-9]))', password)
        return is_valid_password

    def get_emp_data_by_empid(self, emp_id):
        conn = pymysql.connect(db_obj.host, db_obj.user, db_obj.password, db_obj.db, charset='utf8', use_unicode=True)
        cursor_this_data = conn.cursor()
        data = ""
        try:
            with cursor_this_data as cursor:
                cursor.execute(query_obj.get_emp_data % int(emp_id))
                user_desc = cursor_this_data.description
                cursor_this_data.close()
                column_names = [col[0] for col in user_desc]
                this_data = cursor_this_data.fetchall()
                data = [dict(zip(column_names, row)) for row in this_data]
        except Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
        return data
        pass

    # def escape_commas(self, csv_name):
    #     temporary = csv_name.split(".")
    #     temp_file_name = temporary[0] + "_temp.csv"
    #     with open(csv_name, 'r', encoding='utf-8-sig') as input_file, open( temp_file_name , 'wb') as output_file:
    #         output_file.write(input_file.read().replace('"','\"'))
    #         output_file.write(input_file.read().replace("'","\'"))

    #     return temp_file_name

    
    def offer_reminder_mails(self):
        candidates_to_remind = self.get_this_data(query_obj.get_all_candidates_by_status)
        if candidates_to_remind:
            for candidate_details_db in candidates_to_remind:
                job_details_db = self.get_this_data(query_obj.get_job_by_job_code % int(candidate_details_db['job_code']))
                candidate_name_db = candidate_details_db['candidate_name'].split(" ")
                if candidate_details_db['offer_deadline'] != "0000-00-00" and date.today() > candidate_details_db['offer_deadline']:
                    try:
                        cursor = self.db.cursor()
                        insert_sql = query_obj.update_candidate_offer_status % ( "revoked", int(candidate_details_db['candidate_no']) )
                        cursor.execute(insert_sql)
                        self.db.commit()
                        cursor.close()
                        self.email_send(mail_type=33,  email_to_cc = '', email_to=candidate_details_db['candidate_email'], Link='', Table='',
                                        Candidate_Name=candidate_details_db['candidate_name'],
                                                Candidate_First_Name= candidate_name_db[0], Functional_Designation= job_details_db[0]['role'], Sub_Department= job_details_db[0]['sub_department'],
                                        Fixed_Pay= '',
                                        Target_Pay= '',
                                        Variable_Pay= '', City= job_details_db[0]['city'], Location=job_details_db[0]['city'], Unit= job_details_db[0]['unit'], Job_Code= int(candidate_details_db['job_code']),
                                        Reporting_Manager_Functional_Designation=job_details_db[0]['reporting_manager'], Hiring_Manager_Name=job_details_db[0]['hrbp'],
                                        publish_url='',
                                        reject_url= '',
                                        accept_url= '', Applicant_ID= '', Referral_ID= '', Offer_Release_Date=  candidate_details_db['selection_date'].strftime("%d-%m-%y"),
                                        Offer_Acceptance_Date= '',
                                        OTP='', Today_Date=date.today().strftime("%d-%m-%y"), x=0, Document_List='')
                    except  Exception as e:
                        print("Error: "+ str(e))
                        traceback.print_exc()
                        db.rollback()
                
                elif ( ((( datetime.now() - candidate_details_db['selection_date'] ).days)%2 == 0) and (candidate_details_db['selection_date'] is not date.today())):
                    
                    # candidate_url = / + "candidate_response/" + str(candidate_details_db['job_code']) + "/" + str(candidate_details_db['candidate_no']) + "/"
                    encrypted_job_id = self.encrypt(str(candidate_details_db['job_code']),db_obj.salt) 
                    encrypted_candidate_no = self.encrypt(str(candidate_details_db['candidate_no']),db_obj.salt)
                    candidate_url = Settings.configs["host_candidate"] + "/candidate_response/" + self.url_encode(encrypted_job_id) + "/" + self.url_encode(encrypted_candidate_no) + "/"
                    self.email_send(mail_type=26,  email_to_cc = '', email_to=candidate_details_db['candidate_email'], Link='', Table='',
                                        Candidate_Name=candidate_details_db['candidate_name'],
                                                Candidate_First_Name= candidate_name_db[0], Functional_Designation= job_details_db[0]['role'], Sub_Department= job_details_db[0]['sub_department'],
                                        Fixed_Pay= '',
                                        Target_Pay= '',
                                        Variable_Pay= '', City= job_details_db[0]['city'], Location=job_details_db[0]['city'], Unit= job_details_db[0]['unit'], Job_Code= int(candidate_details_db['job_code']),
                                        Reporting_Manager_Functional_Designation=job_details_db[0]['reporting_manager'], Hiring_Manager_Name=job_details_db[0]['hrbp'],
                                        publish_url='',
                                        reject_url= candidate_url+ "rejected",
                                        accept_url= candidate_url+ "accepted", Applicant_ID= '', Referral_ID= '', Offer_Release_Date=  '',
                                        Offer_Acceptance_Date= candidate_details_db['selection_date'].strftime("%d-%m-%y"),
                                        OTP='', Today_Date='', x=0, Document_List='')

    @run_in_thread
    def scheduled_mails_function(self):
        schedule.clear('reminder_mails')
        # schedule.every(5).seconds.do(self.offer_reminder_mails).tag('reminder_mails', 'candidate')
        schedule.every().day.at("10:30").do(self.offer_reminder_mails).tag('reminder_mails', 'candidate')
        while True:
            schedule.run_pending()
            time.sleep(1)

    def month_string_to_number(self, month_str):
        m = {'jan':'01',
            'feb':'02',
            'mar':'03',
            'apr':'04',
            'may':'05',
            'jun':'06',
            'jul':'07',
            'aug':'08',
            'sep':'09',
            'oct':'10',
            'nov':'11',
            'dec':'12'
        }
        s = month_str.strip()[:3].lower()

        try:
            out = m[s]
            return out
        except:
            raise ValueError('Not a month')
    
    def unpad(self,s):
        return s[:-ord(s[len(s) - 1:])]

    def pad(self,s):
        BLOCK_SIZE = 16
        return s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)


    def encrypt(self,raw, salt):
        try:
            private_key = hashlib.sha256(salt.encode("utf-8")).digest()
            raw = self.pad(raw)
            iv = Random.new().read(AES.block_size)
            cipher = AES.new(private_key, AES.MODE_CBC, iv)
            return bytes.decode(base64.b64encode(iv + cipher.encrypt(raw)))
        except  Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
            return 0
        
    def decrypt(self,enc, salt):
        try:
            private_key = hashlib.sha256(salt.encode("utf-8")).digest()
            enc = base64.b64decode(enc)
            iv = enc[:16]
            cipher = AES.new(private_key, AES.MODE_CBC, iv)
            return self.unpad(cipher.decrypt(enc[16:]))    
        except  Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
            return 0
        

    def url_encode(self,val):
        encoded = urllib.parse.urlencode({'': val})
        return encoded[1:len(encoded)]

    def url_decode(self,val):
        val = '=' + val
        decoded = urllib.parse.parse_qs(val)
        return decoded[""][0]

    def day_string_to_number(self, day_str):
        m = {'1':'01',
            '2':'02',
            '3':'03',
            '4':'04',
            '5':'05',
            '6':'06',
            '7':'07',
            '8':'08',
            '9':'09'
        }
        
        try:
            out = m[day_str]
            return out
        except:
            raise ValueError('Not a day')

    def scheduled_job_dispatcher(self):
        schedule.clear('job dispatcher mails')
        schedule.every().tuesday.at("10:30").do(self.job_dispatcher_mails).tag('job_dispatcher', 'candidate')
        schedule.every().thursday.at("10:30").do(self.job_dispatcher_mails).tag('job_dispatcher', 'candidate')
        while True:
            schedule.run_pending()
            time.sleep(1)
        # self.job_dispatcher_mails()

    def job_dispatcher_mails(self):
        candidates_email_data = self.get_data_list(query_obj.get_all_candidate_emails)
        published_job_details = self.get_data_list(query_obj.job_dispatcher_job_details)
        table_html = """<table border="0"; cellpadding="4"; cellspacing="1";><tr><th style='background-color: #C7C9CE;color: #CD2033;'>JN</th><th style='background-color: #C7C9CE;color: #CD2033;'>Job Title</th><th style='background-color: #C7C9CE;color: #CD2033;'>Department</th><th style='background-color: #C7C9CE;color: #CD2033;'>Unit</th><th style='background-color: #C7C9CE;color: #CD2033;'>Location</th><th style='background-color: #C7C9CE;color: #CD2033;'>Job Band</th><th style='background-color: #C7C9CE;color: #CD2033;'>Reporting Manager</th></tr>"""
        for i in range(len(published_job_details)):
            tr_color = ""
            if (i % 2 == 0):
                tr_color = "#F6F6F6"
            else:
                tr_color = "#DEDFE1"
            table_html += "<tr style='background-color: "+tr_color+";'>"
            table_html += "<td>JC"+published_job_details[i]['id']+"</td>"
            table_html += "<td>"+published_job_details[i]['role']+"</td>"
            table_html += "<td>" + published_job_details[i]['department'] +"</td>"
            table_html += "<td>" + published_job_details[i]['unit'] +"</td>"
            table_html += "<td>" + published_job_details[i]['city'] +"</td>"
            table_html += "<td>" + published_job_details[i]['job_band'] +"</td>"
            table_html += "<td>" + published_job_details[i]['reporting_manager'] +"</td>"
            table_html += "</tr>"
        table_html += "</table>"
        for i in range(len(candidates_email_data)):
            self.email_send(mail_type=42, email_to_cc='', email_to=candidates_email_data[i]['email'], Link=Settings.configs["host"],
                            Table=table_html,
                            Candidate_Name='',
                            Candidate_First_Name='', Functional_Designation='',
                            Sub_Department='',
                            Fixed_Pay='',
                            Target_Pay='',
                            Variable_Pay='', City='', Location='',
                            Unit='', Job_Code='',
                            Reporting_Manager_Functional_Designation='',
                            Hiring_Manager_Name='',
                            publish_url='',
                            reject_url='',
                            accept_url='', Applicant_ID='', Referral_ID='', Offer_Release_Date='',
                            Offer_Acceptance_Date='',
                            OTP='', Today_Date='', x=0, Document_List='')

    def calculate_age(self, born):
        today = date.today()
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

    def get_this_data_with_args(self, sql, args):
        data = ""
        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, args)
                user_desc = cursor.description
                this_data = cursor.fetchall()
                # cursor.close()
            column_names = [col[0] for col in user_desc]
            data = [dict(zip(column_names, row)) for row in this_data]
        except Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
            if self.db.open:
                self.db.close() 
            self.db.connect()
        finally:
            pass
        if data:
            return data
        else:
            return None

    def calculate_age_deviation(self, candidate_age, job_band):
        deviation = 0
        if(job_band == "7"): 
            deviation = candidate_age - 35
        elif(job_band == "8"):
            deviation = candidate_age - 35
        elif(job_band == "9"):
            deviation = candidate_age - 30
        elif(job_band == "10"):
            deviation = candidate_age - 27
        elif(job_band == "11" or job_band == "11B"):
            deviation = candidate_age - 25
        else:
            deviation = 0
        return deviation

    def get_cops_cookies(self):
        cookie_dict = dict()
        cookie_dict["cops_user"] = self.get_secure_cookie("cops_user")
        cookie_dict["cops_level"] = self.get_secure_cookie("cops_level")
        cookie_dict["access_token"] = self.get_secure_cookie("access_token")
        return cookie_dict

    def clear_cops_cookies(self):
        self.clear_cookie("cops_user")
        self.clear_cookie("cops_level")
        self.clear_cookie("access_token")
        self.clear_cookie("cops_user_reset_password")
        self.clear_cookie("cops_level_reset_password")
        self.set_secure_cookie("cops_user", 'None',  httponly=True)
        self.set_secure_cookie("cops_level", 'None',  httponly=True)
        self.set_secure_cookie("cops_user_reset_password", 'None',  httponly=True )
        self.set_secure_cookie("cops_level_reset_password", 'None',  httponly=True)

    def calculate_incentive_api(self, master_plan_id, from_duration, to_duration):
        master_plan_details = self.get_this_data(query_obj.get_master_plan_details % int(master_plan_id))
        base_metric_plan_details = self.get_this_data(query_obj.get_base_metric_plan_details % int(master_plan_id))
        rider_plan_details = self.get_this_data(query_obj.get_rider_details % int(master_plan_id))
        
        new_rider_plan = {}
        for plan in rider_plan_details:
            if plan['rider_name'] not in new_rider_plan.keys():
                new_rider_plan[plan['rider_name']] = []
            new_rider_plan[plan['rider_name']].append({
                'min_value': plan['min_range'],
                'Max_value': plan['max_range'],
                'reduction_value': plan['reduction_value']
            })

        payout_plan_details = self.get_this_data(query_obj.get_payout_details % int(master_plan_id))
        print(query_obj.get_emp_perf_data % (pymysql.escape_string(master_plan_details[0]["product"]), from_duration, to_duration))
        emp_details = self.get_this_data(query_obj.get_emp_perf_data % (pymysql.escape_string(master_plan_details[0]["product"]), from_duration, to_duration))
        payout_slabs = json.loads(payout_plan_details[0]["payout_slab"])
        all_emp_final_dict = []

        # print("-------------------")
        # pprint(base_metric_plan_details)
        # print("-------------------")
        # # pprint(rider_plan_details)
        # print("-------------------")
        # pprint(payout_plan_details)
        # print("-------------------")
        # print(payout_slabs)
        response_data = {}
        rider_plan_details = {}
        # table_header_list = [];
        if(emp_details):
            try:
                for i in range(len(emp_details)):
                    
                    emp_dict = {}
                    final_achievement = 0

                    disbursal_value_percent = 0.00
                    avg_yield_percent = 0.00  
                    net_fee_income_percent = 0.00 
                    cross_sell_percent = 0.00 

                    non_tech_bounce_percent = 0.00
                    npa_mob_percent = 0.00

                    emp_dict["work_location"] = emp_details[i]["work_location"]
                    emp_dict["emp_code"] = emp_details[i]["emp_code"]
                    emp_dict["name"] = emp_details[i]["name"]
                    emp_dict["product"] = emp_details[i]["product"]
                    emp_dict["cat"] = emp_details[i]["cat"]
                    emp_dict["target_no_of_disbursals"] = emp_details[i]["target_no_of_disbursals"]


                    target_disbursal_value = float(emp_details[i]["target_disbursal_value"])
                    emp_dict["target_no_of_disbursals"] = round(target_disbursal_value,2)

                    target_net_fee_income = float(emp_details[i]["target_net_fee_income"])*100
                    emp_dict["target_net_fee_income"] = round(target_net_fee_income,2)

                    target_cross_sell = float(emp_details[i]["target_cross_sell"])
                    emp_dict["target_cross_sell"] = round(target_cross_sell,2)

                    actual_no_of_disbursals = float(emp_details[i]["actual_no_of_disbursals"])
                    emp_dict["actual_no_of_disbursals"] = round(actual_no_of_disbursals,2)

                    actual_disbursal_value = float(emp_details[i]["actual_disbursal_value"])
                    emp_dict["actual_disbursal_value"] = round(actual_disbursal_value,2)

                    actual_avg_yield_percent = float(emp_details[i]["actual_avg_yield_percent"])*100
                    emp_dict["actual_avg_yield_percent"] = round(actual_avg_yield_percent,2)

                    actual_net_fee_income = float(emp_details[i]["actual_net_fee_income"])*100
                    emp_dict["actual_net_fee_income"] = round(actual_net_fee_income,2)

                    actual_cross_sell = float(emp_details[i]["actual_cross_sell"])
                    emp_dict["actual_cross_sell"] = round(actual_cross_sell,2)

                    # non_tech_bounce_percent = abs(float(emp_details[i]["non_tech_bounces"])) * 100
                    # emp_dict["non_tech_bounces"] = non_tech_bounce_percent

                    # no_of_bounces = 0
                    # if non_tech_bounce_percent == 5:
                    #     no_of_bounces = 1
                    # elif non_tech_bounce_percent == 10:
                    #     no_of_bounces = 2

                    # npa_cases = 0
                    # npa_mob_percent = abs(float(emp_details[i]["npa_mob"])) * 100
                    # emp_dict["npa_mob"] = npa_mob_percent

                    # if npa_mob_percent == 5:
                    #     npa_cases = 1
                    # elif npa_mob_percent == 10:
                    #     npa_cases = 2

                    non_tech_bounce_percent = 0
                    
                    no_of_bounces = int(emp_details[i]["non_tech_bounces"])
                    emp_dict["non_tech_bounces"] = no_of_bounces
                    if no_of_bounces != 0:
                        condition_list = new_rider_plan['non_tech_bounces']
                        for condition in condition_list:
                            if condition['min_value'] == '':
                                max_val = int(condition['Max_value'])
                                if no_of_bounces <= max_val:
                                    non_tech_bounce_percent = int(condition['reduction_value'])
                            elif condition['Max_value'] == '':
                                min_val = int(condition['min_value'])
                                if no_of_bounces > min_val:
                                    non_tech_bounce_percent = int(condition['reduction_value'])

                    npa_mob_percent = 0
                    
                    npa_cases = int(emp_details[i]["npa_mob"])
                    emp_dict["npa_mob"] = npa_cases
                    if npa_cases != 0:
                        condition_list = new_rider_plan['npa_mob']
                        for condition in condition_list:
                            if condition['min_value'] == '':
                                max_val = int(condition['Max_value'])
                                if npa_cases <= max_val:
                                    npa_mob_percent = int(condition['reduction_value'])
                            elif condition['Max_value'] == '':
                                min_val = int(condition['min_value'])
                                if npa_cases > min_val:
                                    npa_mob_percent = int(condition['reduction_value'])


                    # disbursal_value_percent = 
                    for j in range(len(base_metric_plan_details)):
                        if base_metric_plan_details[j]["metric_name"] == "Disbursal_Value":
                            disbursal_value_percent = (float(actual_disbursal_value) / float(target_disbursal_value)) * float(base_metric_plan_details[j]["weight"])
                            emp_dict["Disbursal_Value"] = round(disbursal_value_percent,2)
                            # print("disbursal_value_percent : "+str(disbursal_value_percent))
                            final_achievement += disbursal_value_percent
                        
                        if base_metric_plan_details[j]["metric_name"] == "Average_Yield":
                            slab_details = json.loads(base_metric_plan_details[j]["slab_details"])
                            # slab weight/100 * metric_weight
                            for k in range(len(slab_details)):
                                if ((float(actual_avg_yield_percent) >= float(slab_details[k]["Min range"])) and (float(actual_avg_yield_percent) <= float(slab_details[k]["Max range"]))):
                                    avg_yield_percent = (float(slab_details[k]["Value"]) / 100) * float(base_metric_plan_details[j]["weight"])
                                    final_achievement += avg_yield_percent
                                    emp_dict["Average_Yield"] = round(avg_yield_percent,2)
                                    # print("avg_yield_percent 1 : "+str(avg_yield_percent))
                                    break;
                                elif(slab_details[k]["Min range"] == "0" and float(actual_avg_yield_percent) < float(slab_details[k]["Max range"])):
                                    avg_yield_percent = (float(slab_details[k]["Value"]) / 100) * float(base_metric_plan_details[j]["weight"])
                                    final_achievement += avg_yield_percent
                                    emp_dict["Average_Yield"] = round(avg_yield_percent,2)
                                    # print("avg_yield_percent 2 : "+str(avg_yield_percent))
                                    break;
                                elif(slab_details[k]["Max range"] == "0" and float(actual_avg_yield_percent) > float(slab_details[k]["Min range"])):
                                    avg_yield_percent = (float(slab_details[k]["Value"]) / 100) * float(base_metric_plan_details[j]["weight"])
                                    final_achievement += avg_yield_percent
                                    emp_dict["Average_Yield"] = round(avg_yield_percent,2)
                                    # print("avg_yield_percent 3 : "+str(avg_yield_percent))
                                    break;
                                    
                        if base_metric_plan_details[j]["metric_name"] == "Net_Fees":
                            net_fee_income_percent = float(actual_net_fee_income)/float(target_net_fee_income) * float(base_metric_plan_details[j]["weight"])
                            final_achievement += net_fee_income_percent
                            emp_dict["Net_Fees"] = round(net_fee_income_percent,2)
                            # print("Net_Fees : "+str(net_fee_income_percent))
                        if base_metric_plan_details[j]["metric_name"] == "Cross_Sell_Target":
                            cross_sell_percent = (float(actual_cross_sell)/float(target_cross_sell)) * float(base_metric_plan_details[j]["weight"])
                            final_achievement += cross_sell_percent
                            emp_dict["Cross_Sell_Target"] = round(cross_sell_percent,2)
                            # print("Cross_Sell_Target : "+str(cross_sell_percent))

                    # print(final_achievement)
                    final_achievement -= non_tech_bounce_percent
                    final_achievement -= npa_mob_percent
                    emp_dict["Achievement %"] = round(final_achievement,2)
                
                    final_payout_amount = 0.0
                    for p in range(len(payout_slabs)):
                        if(final_achievement < float(payout_plan_details[0]['min_achievement_percent'])):
                            final_payout_amount = 0.0
                            break;
                        elif((final_achievement >= float(payout_slabs[p]['Min limit']) and final_achievement <= float(payout_slabs[p]['Max limit']))):
                            final_payout_amount = float(payout_slabs[p]['Amount'])
                            break;
                        elif((final_achievement >= float(payout_slabs[p]['Min limit']) and payout_slabs[p]['Max limit']) == ""):
                            final_payout_amount = float(payout_slabs[p]['Amount'])
                            break;
                    emp_dict["Incentive earned"] = round(final_payout_amount,2)
                    tranche_1_amount = (float(payout_plan_details[0]["tranche_1_payout_percent"]) * float(final_payout_amount))/100
                    tranche_2_amount = (float(payout_plan_details[0]["tranche_2_payout_percent"]) * float(final_payout_amount))/100
                    emp_dict["Tranche 1 payout"] = round(tranche_1_amount,2)
                    emp_dict["Tranche 2 payout"] = round(tranche_2_amount,2)
                    
                    emp_dict = {x.replace("_"," "): v  
                        for x, v in emp_dict.items()} 

                    all_emp_final_dict.append(emp_dict)
                response_data = json.dumps({
                    'success': {
                        'code': '200',
                        'message': "Success",
                        'data': all_emp_final_dict
                    }
                })
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                response_data = json.dumps({
                    'error': {
                        'code': '500',
                        'message': 'Metric calculation failed',
                    }
                })
        else:
            response_data = json.dumps({
                'error': {
                    'code': '500',
                    'message': 'Employee records not found',
                }
            })
        return response_data

    def DB_write_operation(self, sql_query):
        print(sql_query)
        cursor = self.db.cursor()
        cursor.execute(sql_query)
        self.db.commit()
        cursor.close()

    def success_response(self, success_code, success_message):
        return json.dumps({
                'success': {
                    'code': success_code,
                    'message': success_message
                }
            },default=self.date_handler)

    def error_response(self, error_code, error_message):
        return json.dumps({
            'error': {
                'code': error_code,
                'message': error_message
            }
        },default=self.date_handler)

    def metrics_write_data(self,xlsx_name, table_name='master_metrics'):
        rows = self.open_excel( os.getcwd() + '/' + xlsx_name)
        columns = [ele.lower().strip().replace(' ', '_').replace('-', '_') for ele in rows[0]]
        columns.append("created_at")
        columns.append("updated_at")
        rows.pop(0)

        placeholders = ", ".join(['%s'] * (len(rows[0]) + 2))
        date_str = [columns.index(ele) for ele in columns if 'date' in ele]
        columns = ', '.join(columns)
        cursor = self.db.cursor()
        for ele in range(len(rows)):
            for ind in range(len(date_str)):
                try:
                    rows[ele][date_str[ind]] = self.change_format(rows[ele][date_str[ind]])
                except Exception as e:
                    continue
            try:

                rows[ele].append(datetime.now())
                rows[ele].append(datetime.now())
                email_sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (pymysql.escape_string(table_name), pymysql.escape_string(columns), placeholders)
                cursor.execute(email_sql, rows[ele])
            except UnicodeError as e:
                print("Unicode Error: "+ str(e))
                self.db.rollback()
                traceback.print_exc()
                return {
                        'status' : 0,
                        'message': "Unicode Decode error, please contact the system administrator for diagnostics and details pertaining to the error."
                    }
            except IntegrityError as e:
                employee_update_sql = "UPDATE master_metrics SET business_unit = '%s', department = '%s',"\
                "sub_department = '%s', role = '%s',metric_name = '%s',"\
                "plan_type= '%s', metric_type = '%s',item_type = '%s',unit = '%s',"\
                "updated_at = NOW() "\
                "WHERE business_unit = '%s' AND department = '%s' AND sub_department = '%s' AND role = '%s' AND metric_name = '%s'" %(
                pymysql.escape_string(rows[ele][0]), #unit
                pymysql.escape_string(rows[ele][1]), #department
                pymysql.escape_string(rows[ele][2]), #sub_department
                pymysql.escape_string(rows[ele][3]), #role
                pymysql.escape_string(rows[ele][4]), #metric_name
                pymysql.escape_string(rows[ele][5]), #plan type
                pymysql.escape_string(rows[ele][6]), #metric_type
                pymysql.escape_string(rows[ele][7]), #item_type
                pymysql.escape_string(rows[ele][8]), #unit
                pymysql.escape_string(rows[ele][0]), #unit
                pymysql.escape_string(rows[ele][1]), #department
                pymysql.escape_string(rows[ele][2]), #sub_department
                pymysql.escape_string(rows[ele][3]), #role
                pymysql.escape_string(rows[ele][4])) #unit
                cursor = self.db.cursor()
                cursor.execute(employee_update_sql)
                self.db.commit()
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                pass
                return {
                    'status' : 0,
                    'message': "There was an problem while uploading, please try again after some time."
                }
            finally:
                pass
        self.db.commit()
        cursor.close()
        return {
            'status' : 1,
            'message': "Data written succssfully"
        }