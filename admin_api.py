import requests
import json
import random
from messages import success
from messages import errors
from queries import SQLqueries
from datetime import datetime, timedelta, date
from pprint import pprint
from common import CommonBaseHandler
import os
#import time
from settings import Settings as sett
from settings import MYSQLConfiguration
import shutil, errno, re
import zipfile
#import csv
import pwd
import grp
import traceback
import humanize
import pymysql
# from unique_dept import UniqueDept
from logger import LoggerClass
import string
import base64
import rsa
import glob
from zipfile import ZipFile
import tornado.escape
import ast
from core_functions import CoreFunctions

import pandas as pd
import statistics
core_fun = CoreFunctions()

query_obj = SQLqueries()
success_obj = success()
error_obj = errors()

logger_ob = LoggerClass()
logger_ob.create_logger()

Settings = sett()
db_obj = MYSQLConfiguration()

# class NotFound(CommonBaseHandler):
#     @logger_ob.log_er
#     @logger_ob.with_sys_logging
#     def get(self, *args):
#         print(args)
#         self.redirect("/admin/login")

class MasterPlanGenerator(CommonBaseHandler):
    @logger_ob.log_er
    @logger_ob.with_sys_logging
    def get(self):
        print("Landing")
        self.render("master_plan_generate.html",settings=Settings.configs,error=None)
        
class ManagerUploads(CommonBaseHandler):
    @logger_ob.log_er
    @logger_ob.with_sys_logging
    def get(self):
        self.render("metric_uploads.html",settings=Settings.configs,error=None)

class EmployeeViewIncentive(CommonBaseHandler):
    @logger_ob.log_er
    @logger_ob.with_sys_logging
    def get(self):
        self.render("view_employee_performance.html",settings=Settings.configs,error=None)

class PlanSimulator(CommonBaseHandler):
    @logger_ob.log_er
    @logger_ob.with_sys_logging
    def get(self):
        self.render("plan_simulator.html",settings=Settings.configs,error=None)

############
class DevMasterPlanGenerator(CommonBaseHandler):
    @logger_ob.log_er
    @logger_ob.with_sys_logging
    def get(self):
        print("Landing")
        metrics_details = self.get_data_list(query_obj.get_metric_details)
        self.render("dev_master_plan_generate.html",settings=Settings.configs,error=None, metrics_details=json.dumps(metrics_details, default=self.date_handler))

    def post(self):
        unit = self.get_argument("unit", default="", strip=False)
        department = self.get_argument("department", default="", strip=False)
        sub_department = self.get_argument("sub_department", default="", strip=False)
        role = self.get_argument("role", default="", strip=False)
        product = self.get_argument("product", default="", strip=False)
        from_duration = self.get_argument("from_duration", default="", strip=False)
        to_duration = self.get_argument("to_duration", default="", strip=False)
        plan_name = self.get_argument("plan_name", default="", strip=False)

        base_metric_obj = json.loads(self.get_argument("base_metric_object", default="", strip=False))
        rider_obj = json.loads(self.get_argument("rider_object", default="", strip=False))
        payout_object = json.loads(self.get_argument("payout_object", default="", strip=False))
        response_data = {}
        master_plan_id = 0

        payout_term = payout_object["payout_term"]
        tranche_1_payout_percent = payout_object["tranche_1_payout_percent"]
        tranche_2_payout_percent = payout_object["tranche_2_payout_percent"]
        min_achievement_percent = payout_object["min_achievement_percent"]
        annual_payout_cap_percent = payout_object["annual_payout_cap_percent"]
        applicability_factor = payout_object["applicability_factor"]
        payout_slab = payout_object["payout_slab"]

        try:
            print(unit)
            print(department)
            print(sub_department)
            print(role)
            print(product)
            print(from_duration)
            print(to_duration)
            print(plan_name)
            print("---------------")
            print(base_metric_obj)
            print("---------------")
            print(rider_obj)
            print("---------------")
            print(payout_object)
            print("---------------")
            create_master_plan_cursor =""
            create_base_metric_plan_cursor = ""
            create_payout_plan_cursor = ""
            create_rider_plan_cursor = ""
            try:
                create_master_plan_sql = query_obj.create_master_plan % (pymysql.escape_string(plan_name),pymysql.escape_string(unit),
                    pymysql.escape_string(department),pymysql.escape_string(sub_department),pymysql.escape_string(role),
                    pymysql.escape_string(product),pymysql.escape_string(from_duration),pymysql.escape_string(to_duration))
                create_master_plan_cursor = self.db.cursor()
                create_master_plan_cursor.execute(create_master_plan_sql)
                self.db.commit()
                master_plan_id = create_master_plan_cursor.lastrowid
                create_master_plan_cursor.close()
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                self.db.rollback()
            finally:
                pass

            try:
                for i in base_metric_obj.values():
                    # print(i["metric_name"])
                    # print(i["weight"])
                    # print(i["scale"])
                    # print(i["scale_max_cap"])
                    # print(i["slab_details"])
                    # print("*******************")
                    # print(master_plan_id)
                    # print("*******************")                
                    create_base_metric_plan_sql = query_obj.create_base_metric_plan % (pymysql.escape_string(i["metric_name"]),pymysql.escape_string(i["weight"]),
                    pymysql.escape_string(i["scale"]),pymysql.escape_string(json.dumps(i["slab_details"])), pymysql.escape_string(i["scale_max_cap"]), int(master_plan_id))
                    create_base_metric_plan_cursor = self.db.cursor()
                    create_base_metric_plan_cursor.execute(create_base_metric_plan_sql)
                    self.db.commit()
                    create_base_metric_plan_cursor.close()
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                self.db.rollback()

            try:
                if(len(rider_obj) > 0):
                    for i in rider_obj:
                        create_rider_plan_sql = query_obj.create_rider_plan % (pymysql.escape_string(i["Rider name"]),pymysql.escape_string(i["Min limit"]),
                        pymysql.escape_string(i["Max limit"]),pymysql.escape_string(i["Reduction value"]), int(master_plan_id))
                        create_rider_plan_cursor = self.db.cursor()
                        create_rider_plan_cursor.execute(create_rider_plan_sql)
                        self.db.commit()
                        create_rider_plan_cursor.close()
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                self.db.rollback()

            try:
                create_payout_plan_sql = query_obj.create_payout_plan % (pymysql.escape_string(payout_term),pymysql.escape_string(tranche_1_payout_percent),pymysql.escape_string(tranche_2_payout_percent),
                pymysql.escape_string(min_achievement_percent),pymysql.escape_string(annual_payout_cap_percent),pymysql.escape_string(applicability_factor),
                pymysql.escape_string(json.dumps(payout_slab)), int(master_plan_id))
                create_payout_plan_cursor = self.db.cursor()
                create_payout_plan_cursor.execute(create_payout_plan_sql)
                self.db.commit()
                create_payout_plan_cursor.close()
                        
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                self.db.rollback()

            response_data = json.dumps({
                'success': {
                    'code': "200",
                    'message': "Plan created successfully",
                    'data' : master_plan_id
                }
            })

        except Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
            response_data = json.dumps({
                'error': {
                    'code': 0,
                    'message': 'Plan generation failed',
                }
            })
        self.write(response_data)
        self.finish()



class DevManagerUploads(CommonBaseHandler):
    @logger_ob.log_er
    @logger_ob.with_sys_logging
    def get(self):
        self.render("dev_metric_uploads.html",settings=Settings.configs,error=None)


class DevPlanSimulator(CommonBaseHandler):
    @logger_ob.log_er
    @logger_ob.with_sys_logging
    def get(self):
        master_plan_details = self.get_this_data(query_obj.get_all_master_plan_details)
        self.render("dev_plan_simulator.html",settings=Settings.configs,error=None, master_plan_details=json.dumps(master_plan_details, default=self.date_handler))


class DevEmployeePerformanceUploads(CommonBaseHandler):
    # @logger_ob.log_er
    # @logger_ob.with_sys_logging
    def sql_dump(self, file):
        return self.write_data(file)

    def post(self):
        file1 = self.request.files['file1'][0]
        original_fname = str(file1['filename'])
        file_location = "BackEnd/ninja/emp_csv_uploads/" + original_fname
        response_data = {}

        with open(file_location, 'wb') as output_file:
            output_file.write(file1['body'])
        status = self.sql_dump(file_location)
        if status['status'] == 0:
            response_data = json.dumps({
                'error': {
                    'code': 0,
                    'message': status["message"],
                }
            })
        else:
            response_data = json.dumps({
            'success': {
                'code': 1,
                'message': "Success",
            }
        })

        self.write(response_data)
        self.finish()

class ViewUpdateSinglePlan(CommonBaseHandler):
    @logger_ob.log_er
    @logger_ob.with_sys_logging
    def get(self,master_plan_id):
        master_plan_details = self.get_this_data(query_obj.get_master_plan_details % int(master_plan_id))
        base_metric_plan_details = self.get_this_data(query_obj.get_base_metric_plan_details % int(master_plan_id))
        rider_plan_details = self.get_this_data(query_obj.get_rider_details % int(master_plan_id))
        payout_plan_details = self.get_this_data(query_obj.get_payout_details % int(master_plan_id))
        
        print(master_plan_details)
        print("--------------")
        print(base_metric_plan_details)
        print("--------------")
        print(rider_plan_details)
        print("--------------")
        print(payout_plan_details)

        print("===========================")

        self.render("dev_view_master_plan.html",settings=Settings.configs,master_plan_details=json.dumps(master_plan_details, default=self.date_handler),
            base_metric_plan_details=json.dumps(base_metric_plan_details, default=self.date_handler),rider_plan_details=json.dumps(rider_plan_details, default=self.date_handler),
            payout_plan_details=json.dumps(payout_plan_details, default=self.date_handler),error=None, master_plan_id=master_plan_id)


    def post(self,master_plan_id):
        unit = self.get_argument("unit", default="", strip=False)
        department = self.get_argument("department", default="", strip=False)
        sub_department = self.get_argument("sub_department", default="", strip=False)
        role = self.get_argument("role", default="", strip=False)
        product = self.get_argument("product", default="", strip=False)
        from_duration = self.get_argument("from_duration", default="", strip=False)
        to_duration = self.get_argument("to_duration", default="", strip=False)
        plan_name = self.get_argument("plan_name", default="", strip=False)
        plan_status = self.get_argument("plan_status", default="", strip=False)
        base_metric_obj = json.loads(self.get_argument("base_metric_object", default="", strip=False))
        rider_obj = json.loads(self.get_argument("rider_object", default="", strip=False))
        payout_object = json.loads(self.get_argument("payout_object", default="", strip=False))
        response_data = {}

        payout_term = payout_object["payout_term"]
        tranche_1_payout_percent = payout_object["tranche_1_payout_percent"]
        tranche_2_payout_percent = payout_object["tranche_2_payout_percent"]
        min_achievement_percent = payout_object["min_achievement_percent"]
        annual_payout_cap_percent = payout_object["annual_payout_cap_percent"]
        applicability_factor = payout_object["applicability_factor"]
        payout_slab = payout_object["payout_slab"]

        try:
            print(unit)
            print(department)
            print(sub_department)
            print(role)
            print(product)
            print(from_duration)
            print(to_duration)
            print(plan_name)
            print("---------------")
            print(base_metric_obj)
            print("---------------")
            print(rider_obj)
            print("---------------")
            print(payout_object)
            print("---------------")
            update_master_plan_cursor =""
            update_base_metric_plan_cursor = ""
            update_payout_plan_cursor = ""
            update_rider_plan_cursor = ""
            
            #master_plan
            try:
                update_master_plan_sql = query_obj.update_master_plan % (pymysql.escape_string(plan_name),pymysql.escape_string(unit),
                    pymysql.escape_string(department),pymysql.escape_string(sub_department),pymysql.escape_string(role),
                    pymysql.escape_string(product),pymysql.escape_string(from_duration),pymysql.escape_string(to_duration),pymysql.escape_string(plan_status), int(master_plan_id))
                update_master_plan_cursor = self.db.cursor()
                update_master_plan_cursor.execute(update_master_plan_sql)
                self.db.commit()
                update_master_plan_cursor.close()
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                self.db.rollback()
            finally:
                pass

            #base_metric_plan
            try:
                delete_base_metric_plan_sql = query_obj.delete_base_metric_plan % int(master_plan_id)
                delete_base_metric_plan_cursor = self.db.cursor()
                delete_base_metric_plan_cursor.execute(delete_base_metric_plan_sql)
                self.db.commit()
                delete_base_metric_plan_cursor.close()

                for i in base_metric_obj.values():
                    # print(i["metric_name"])
                    # print(i["weight"])
                    # print(i["scale"])
                    # print(i["scale_max_cap"])
                    # print(i["slab_details"])
                    # print("*******************")
                    # print(master_plan_id)
                    # print("*******************")                
                    create_base_metric_plan_sql = query_obj.create_base_metric_plan % (pymysql.escape_string(i["metric_name"]),pymysql.escape_string(i["weight"]),
                    pymysql.escape_string(i["scale"]),pymysql.escape_string(json.dumps(i["slab_details"])), pymysql.escape_string(i["scale_max_cap"]), int(master_plan_id))
                    create_base_metric_plan_cursor = self.db.cursor()
                    create_base_metric_plan_cursor.execute(create_base_metric_plan_sql)
                    self.db.commit()
                    create_base_metric_plan_cursor.close()
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                self.db.rollback()

            #riders
            try:
                delete_rider_plan_sql = query_obj.delete_rider_plan % int(master_plan_id)
                delete_rider_plan_cursor = self.db.cursor()
                delete_rider_plan_cursor.execute(delete_rider_plan_sql)
                self.db.commit()
                delete_rider_plan_cursor.close()

                if(len(rider_obj) > 0):
                    for i in rider_obj:
                        create_rider_plan_sql = query_obj.create_rider_plan % (pymysql.escape_string(i["Rider name"]),pymysql.escape_string(i["Min limit"]),
                        pymysql.escape_string(i["Max limit"]),pymysql.escape_string(i["Reduction value"]), int(master_plan_id))
                        create_rider_plan_cursor = self.db.cursor()
                        create_rider_plan_cursor.execute(create_rider_plan_sql)
                        self.db.commit()
                        create_rider_plan_cursor.close()
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                self.db.rollback()

            #payout
            try:
                update_payout_plan_sql = query_obj.update_payout_plan % (pymysql.escape_string(payout_term),pymysql.escape_string(tranche_1_payout_percent),pymysql.escape_string(tranche_2_payout_percent),
                pymysql.escape_string(min_achievement_percent),pymysql.escape_string(annual_payout_cap_percent),pymysql.escape_string(applicability_factor),
                pymysql.escape_string(json.dumps(payout_slab)), int(master_plan_id))
                update_payout_plan_cursor = self.db.cursor()
                update_payout_plan_cursor.execute(update_payout_plan_sql)
                self.db.commit()
                update_payout_plan_cursor.close()
                        
            except Exception as e:
                print("Error: "+ str(e))
                traceback.print_exc()
                self.db.rollback()

            response_data = json.dumps({
                'success': {
                    'code': "200",
                    'message': "Plan updated successfully",
                }
            })

        except Exception as e:
            print("Error: "+ str(e))
            traceback.print_exc()
            response_data = json.dumps({
                'error': {
                    'code': 0,
                    'message': 'Plan updation failed',
                }
            })

        self.write(response_data)
        self.finish()

class DevEmployeeViewIncentive(CommonBaseHandler):
    # @logger_ob.log_er
    # @logger_ob.with_sys_logging
    def get(self):
        master_plan_details = self.get_this_data(query_obj.get_all_master_plan_details)
        self.render("dev_view_employee_performance.html",settings=Settings.configs,error=None, master_plan_details=json.dumps(master_plan_details, default=self.date_handler))

    def post(self):
        print("hello")
        master_plan_id = self.get_argument("master_plan_id", default="", strip=False)
        master_plan_name = self.get_argument("master_plan_name", default="", strip=False)

        from_duration = self.get_argument("from_duration", default="", strip=False)
        to_duration = self.get_argument("to_duration", default="", strip=False)

        response_data = self.calculate_incentive_api(master_plan_id, from_duration, to_duration)
        self.write(response_data)
        self.finish()

class DevEmployeeViewIncentiveEmp(CommonBaseHandler):
    @logger_ob.log_er
    @logger_ob.with_sys_logging
    def get(self, emp_id):
        employee_performance = self.get_this_data(query_obj.get_single_emp_performance % pymysql.escape_string(emp_id))
        print(employee_performance)
        print(query_obj.get_single_emp_performance % pymysql.escape_string(emp_id))
        master_plan_details = self.get_this_data(query_obj.get_all_master_plan_details)
        self.render("dev_view_employee_performance_emp.html",settings=Settings.configs,error=None, employee_performance=json.dumps(employee_performance, default=self.date_handler), master_plan_details=json.dumps(master_plan_details, default=self.date_handler))

    def post(self, emp_id):
        print("hello")
        master_plan_id = self.get_argument("master_plan_id", default="", strip=False)
        master_plan_name = self.get_argument("master_plan_name", default="", strip=False)
        
        actual_average_yield = self.get_argument("actual_average_yield", default="", strip=False)
        actual_disbursal_value = self.get_argument("actual_disbursal_value", default="", strip=False)
        actual_cross_sell = self.get_argument("actual_cross_sell", default="", strip=False)
        actual_net_fee_income = self.get_argument("actual_net_fees", default="", strip=False)

        npa_cases = self.get_argument("npa_mob_cases", default="", strip=False)
        non_tech_bounces = self.get_argument("non_tech_bounces", default="", strip=False)

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

        emp_details = self.get_this_data(query_obj.get_single_emp_performance % pymysql.escape_string(emp_id))
        print(query_obj.get_single_emp_performance % pymysql.escape_string(emp_id))
        print(emp_details)
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

                actual_disbursal_value = float(actual_disbursal_value)
                emp_dict["actual_disbursal_value"] = round(actual_disbursal_value,2)

                actual_avg_yield_percent = float(actual_average_yield)*100
                emp_dict["actual_avg_yield_percent"] = round(actual_avg_yield_percent,2)

                actual_net_fee_income = float(actual_net_fee_income)*100
                emp_dict["actual_net_fee_income"] = round(actual_net_fee_income,2)

                actual_cross_sell = float(actual_cross_sell)
                emp_dict["actual_cross_sell"] = round(actual_cross_sell,2)

                # non_tech_bounce_percent = abs(float(emp_details[i]["non_tech_bounces"])) * 100
                # emp_dict["non_tech_bounces"] = non_tech_bounces

                # # non_tech_bounce_percent = 5
                # if int(non_tech_bounces) == 1:
                #     non_tech_bounce_percent = 5
                # elif int(non_tech_bounces) > 1:
                #     non_tech_bounce_percent = 10
                # else:
                #     non_tech_bounce_percent = 0


                # emp_dict["npa_mob"] = npa_cases
                # if int(npa_cases) == 1:
                #     npa_mob_percent = 5
                # elif int(npa_cases) > 1:
                #     npa_mob_percent = 10
                # else:
                #     npa_mob_percent = 0
                non_tech_bounce_percent = 0
                
                no_of_bounces = int(non_tech_bounces)
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
                
                npa_cases = int(npa_cases)
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
                        # print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                        # print(actual_net_fee_income)
                        # print(target_net_fee_income)
                        # print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                        net_fee_income_percent = (float(actual_net_fee_income)/float(target_net_fee_income)) * float(base_metric_plan_details[j]["weight"])
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
                    
                    


                    if(payout_slabs[p]['Min limit'] == ""):
                        # print("//////////////")
                        payout_slabs[p]['Min limit'] = "0"

                    if(payout_slabs[p]['Max limit'] == ""):
                        # print("99999999999999999999")
                        payout_slabs[p]['Max limit'] = "0"
                    
                    payout_slabs[p]['Min limit'] = ast.literal_eval(payout_slabs[p]['Min limit'])
                    payout_slabs[p]['Max limit'] = ast.literal_eval(payout_slabs[p]['Max limit'])
                    if(float(final_achievement) < float(payout_plan_details[0]['min_achievement_percent'])):
                        final_payout_amount = 0.0
                        break;
                    elif((float(final_achievement) >= float(payout_slabs[p]['Min limit']) and float(final_achievement) <= float(payout_slabs[p]['Max limit']))):
                        final_payout_amount = float(payout_slabs[p]['Amount'])
                        break;
                    elif((float(final_achievement) >= float(payout_slabs[p]['Min limit']) and float(payout_slabs[p]['Max limit']) == 0.0)):
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
        print(response_data)
        self.write(response_data)
        self.finish()


class SimulateMasterPlan(CommonBaseHandler):
    def post(self):
        # try:
        # self.set_header('Content-Type', 'application/json; charset=UTF-8')
        master_plan_id = self.get_argument("master_plan_id", default="", strip=False)
        master_plan_name = self.get_argument("master_plan_name", default="", strip=False)
        from_duration = self.get_argument("from_duration", default="", strip=False)
        to_duration = self.get_argument("to_duration", default="", strip=False)
        json_response = self.calculate_incentive_api(master_plan_id,from_duration,to_duration)
        incentive_obj = json.loads(json_response)

        print(json_response)
        response_data = {}
        if incentive_obj["success"]:
            incentive_data = incentive_obj["success"]["data"]
            data_f = pd.read_json(json.dumps(incentive_data, default=self.date_handler))
            print(os.getcwd())
            data_f.to_excel("BackEnd/uploads/"+master_plan_id+".xlsx")
            # response = requests.post("http://localhost:8016/get_incentive_details", data={"incentive_details": json.dumps(incentive_data, default=self.date_handler), "master_plan_name":master_plan_name})
            # self.redirect(Settings.configs["host"]+"SimulateHRA/"+master_plan_id)
            response_data = json.dumps({
                'success': {
                    'code': '200',
                    'message': "Success",
                    'data': {'master_plan_id' : str(master_plan_id),
                            'master_plan_name' : str(master_plan_name)}
                }
            })
        self.write(response_data)
        self.finish()
        
        # except Exception as e:
        #     print("Error: "+ str(e))
        #     traceback.print_exc()
        #     response_data = json.dumps({
        #         'error': {
        #             'code': '500',
        #             'message': 'Metric calculation failed',
        #         }
        #     })
        #     print(response_data)


class CreateIncentiveFile(CommonBaseHandler):
    @logger_ob.log_er
    def post(self):
        # incentive_data = self.get_argument("incentive_details", default="")
        # master_plan_name = self.get_argument("master_plan_name", default="")
        master_plan_id = self.get_argument("master_plan_name", default="")
        # print(incentive_data[0]["name"])
        try:
            data_f = pd.read_json(incentive_data)
            data_f.to_excel("uploads/"+master_plan_id+".xlsx")
            # self.redirect("SimulateHRA/"+)
        except Exception as e:
            print('Error:', e)
        self.finish()


class SimulateHRA(CommonBaseHandler):
    def get(self):
        print("rendered simulate HRA...........")
        master_plan_details = self.get_this_data(query_obj.get_all_master_plan_details)
        self.render("dev_hra_simulate_plan.html", error=None, settings=Settings.configs, master_plan_details=json.dumps(master_plan_details, default=self.date_handler))


class CompareUpload(CommonBaseHandler):
    def post(self):
        self.set_headerlist()
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        upload_type = self.get_argument('upload_type')
        dict_key = self.get_argument('dict_key')

        if upload_type == "cmp_upload":

            # user_file_name = self.get_argument('file_name')
            # side_name = self.get_argument('side_name')
            # select_tile = self.get_argument('select_tile')
            # slider = self.get_argument('select_slider')
            # month = self.get_argument("month", default="", strip=False)
            # year = self.get_argument("year", default="", strip=False)
            file1 = self.request.files['file'][0]
            original_fname = str(file1['filename']).replace(' ', '_')
            f = original_fname.split('.')
            f[0] = f[0] + '_' + datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d_%H_%M_%S')
            original_fname = '.'.join(f)
            file_location = "BackEnd/uploads/" + original_fname
            with open(file_location, 'wb') as output_file:
                output_file.write(file1['body'])

            # error_flag, error_type = self.check_file_validation(file_location, side_name, select_tile)
            # print(error_flag, error_type)
            # if not error_flag:
            #     if self.check_file_data_validity(ast.literal_eval(month), year, duration_column, file_location):
            uploaded_df = pd.read_excel(file_location)
            uploaded_file_cols = list(uploaded_df.columns)
            graph_cols = core_fun.get_columns()
            dirty_flag = uploaded_df.isnull().values.any()
            # if (all(x in graph_cols for x in uploaded_file_cols)):
            core_fun.set_dict_for_upload(dict_key, uploaded_df)
            # response_data = self.success_response(success_obj.success_code_Ok, success_obj.success_get_data)
            response_dict = {
                "success": {
                    "code": success_obj.success_code_Ok,
                    "message": success_obj.success_get_data
                }
            }
            if dirty_flag:
                response_dict["success"]["warning_flag"] = 'true'

            response_data = json.dumps(response_dict)
            # else:
            #     response_data = self.error_response(error_obj.error_code_unauthorized, error_obj.error_file_invalid)

        elif upload_type == "cmp_prev":
            file1 = self.get_argument('selected_file')
            try:
                selected_df = pd.read_excel('BackEnd/uploads/'+file1)
                selected_file_cols = list(selected_df.columns)
                graph_cols = core_fun.get_columns()
                # abcresult = set(graph_cols) - set(selected_file_cols)
                # abcresult1 = set(selected_file_cols) - set(graph_cols)
                # print('Present file extra columns:', abcresult)
                # print('Prev file extra columns:', abcresult1)
                # if (all(x in graph_cols for x in selected_file_cols)):
                core_fun.set_dict_for_upload(dict_key, selected_df)
                response_data = self.success_response(success_obj.success_code_Ok, success_obj.success_upload_successful)
                # else:
                #     response_data = self.error_response(error_obj.error_code_not_found, error_obj.error_file_invalid)
            except:
                response_data = self.error_response(error_obj.error_code, error_obj.error_file_unavailable)
        elif upload_type == "cmp_present":
            response_data = json.dumps({
                'success': {
                    'code': 200,
                    'message': success_obj.success_get_data
                }
            })

        self.write(response_data)
        self.finish()


class GetCompareParams(CommonBaseHandler):
    def post(self):
        self.set_headerlist()
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        try:
            url_file_name = self.get_argument('url_file_name')

            if url_file_name:
                side_name = core_fun.get_side_tile_name(url_file_name)
                date_columns = core_fun.get_all_date_columns()

                if date_columns:
                    date_min_max_columns = core_fun.get_all_min_max_date_values(date_columns)
                else: 
                    date_min_max_columns = []

                tile_index = core_fun.get_tile_index(url_file_name) + 1  # plus 1 because indexing starts from 0
                response_data = json.dumps({
                    'success': {
                        'code': success_obj.success_code_Ok,
                        'message': success_obj.success_get_data,
                        'data': {
                            'side_name': side_name,
                            'tile_name': [(url_file_name, tile_index)],
                            'date_data': date_min_max_columns
                        },
                    }
                },default=self.date_handler)
            else:
                response_data = json.dumps({
                    'error': {
                        'code': "400",
                        'message': error_obj.error_data_not_found 
                    }
                },default=self.date_handler)
        except:
            response_data = self.error_response(error_obj.error_code_not_found, error_obj.error_file_invalid)

        self.write(response_data)
        self.finish()


class GetComparisonDates(CommonBaseHandler):
    def post(self):
        try:
            self.set_headerlist()
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            cmp_type = self.get_argument("cmp_type")
            response_data = self.error_response(error_obj.error_code_not_found, error_obj.error_file_invalid)
            if cmp_type == "cmp_present":
                cmp_key = ""
            else:
                cmp_key = self.get_argument("dict_key")

            date_columns = core_fun.get_all_date_columns(cmp_key)
            if date_columns:
                date_min_max_columns = core_fun.get_all_min_max_date_values(date_columns, cmp_key=cmp_key)
            else: date_min_max_columns = []

            response_data = json.dumps({
                'success': {
                    'code': success_obj.success_code_Ok,
                    'message': success_obj.success_get_data,
                    'date_data': date_min_max_columns
                }
            },default=self.date_handler)
        except:
            response_data = self.error_response(error_obj.error_code, error_obj.error_file_invalid)

        self.write(response_data)


class GroupBy(CommonBaseHandler):
    @logger_ob.log_er
    def post(self):
        self.set_headerlist()
        # core_fun.read_excel()
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        x_axis = json.loads(self.get_argument('x_axis').replace("+", " "))
        y_axis_element = self.get_argument('y_axis_element')
        aggregation_type = self.get_argument('aggregation')
        drilled_att = json.loads(self.get_argument('drilled_att').replace("+", " "))
        start_date = self.get_argument('start_date', default="").replace("+", " ")
        end_date = self.get_argument('end_date', default="").replace("+", " ")
        date_col = self.get_argument('date_col', default="").replace("+", " ")
        graph_type = self.get_argument('graph_type')
        cmp_flag = False if self.get_argument('cmp_flag', default="") == 'false' else True
        dict_key = ""
        cmp_type = ""

        if cmp_flag:
            dict_key = self.get_argument('dict_key', default="")
            cmp_type = self.get_argument('cmp_type', default="")
            if cmp_type == 'cmp_present':
                core_fun.set_dict_for_present(dict_key)

        if isinstance(x_axis, str):
            x_axis = ast.literal_eval(json.loads(self.get_argument('x_axis')))
        else:
            pass
        
        if isinstance(drilled_att, str):
            drilled_att = ast.literal_eval(json.loads(self.get_argument('drilled_att')))
        else:
            pass

        # core_fun.replace_all_missing_values(miss='previous_value', cmp_key=dict_key)
        try:
            df_copy = core_fun.get_dataframe(cmp_key=dict_key)
            df_copy = core_fun.remove_missing_values(df_copy, x_axis)
            df_copy = core_fun.date_to_string(df_copy)

            if date_col:
                df_copy = core_fun.change_date(start_date, end_date, date_col, df_copy)
            else:
                pass

            if graph_type == 'pivot':
                print(x_axis)
                data = core_fun.get_pivot_data(x_axis, drilled_att, df_copy)
                response_data = json.dumps({
                            'success': {
                                'code': "200",
                                'message': "Got Dataframes",
                                'data': data
                            }
                        })

            else:
                x_axis_att, y_axis_att, data, count_data = core_fun.get_drilled_data(df_copy, x_axis, drilled_att)
                if aggregation_type == 'values':
                    # Below 'if' is to check whether the xaxis and yaxis selected will plot the graph of 'singular vs numerical' data
                    if core_fun.value_axes_validator(x_axis, y_axis_element, drilled_att, df_copy):
                        res = core_fun.get_key_values(x_axis, y_axis_element, drilled_att, df_copy)
                        x_axis_att = list(res.keys())
                        y_axis_att = list(res.values())
                        r = {
                            'success': {
                                'code': "200",
                                'message': "Got Data",
                                'x_axis_att': x_axis_att,
                                'y_axis_att': y_axis_att,
                                'data': data,
                                'x_axis_col': core_fun.get_columns(cmp_key=dict_key),
                                'data_count': list(count_data.values())
                            }
                        }
                        response_data = json.dumps(r)
                    else:
                        # self.set_status(400)
                        response_data = json.dumps({
                            'error': {
                                'code': "400",
                                'message': error_obj.error_value_axis
                                }
                        }, default=self.date_handler)
                elif aggregation_type == 'sum':
                    numeric_flag = core_fun.sum_axes_validator(y_axis_element, df_copy)
                    if numeric_flag:
                        if numeric_flag == 5:
                            df_copy[y_axis_element] = pd.to_numeric(df_copy[y_axis_element])
                        sum_data = core_fun.get_sum_data(x_axis, y_axis_element, df_copy, drilled_att)
                        x_axis_att = list(sum_data.keys())
                        y_axis_att = list(sum_data.values())
                        r = {
                            'success': {
                                'code': "200",
                                'message': "Got Data",
                                'x_axis_att': x_axis_att,
                                'y_axis_att': y_axis_att,
                                'data': data,
                                'x_axis_col': core_fun.get_columns(cmp_key=dict_key),
                                'data_count': list(count_data.values())
                            }
                        }
                        response_data = json.dumps(r)

                    else:
                        response_data = json.dumps({
                            'error': {
                                'code': "400",
                                'message': error_obj.error_sum_axis
                            }
                        }, default=self.date_handler)
                else:
                    response_data = json.dumps({
                        'success': {
                            'code': "200",
                            'message': "Got Data",
                            'x_axis_att': x_axis_att,
                            'y_axis_att': y_axis_att,
                            'data': data,
                            'x_axis_col': core_fun.get_columns(cmp_key=dict_key),
                            'data_count': list(count_data.values())
                        }
                    })
        except KeyError as e:
            chart_n = 'present'
            if cmp_flag:
                chart_n = 'comparison'
            response_data = json.dumps({
                "error":{
                    "code": "500",
                    "message": "The data required for plotting "+chart_n+" graph is not available"
                }
            })
        except Exception as e:
            print(e)
            response_data = json.dumps({
                "error": {
                    "code": 500,
                    "message": "Graph Cannot be plotted, kindly check data sanity through cleaning module"
                }
            })
        # pprint(json.loads(response_data))
        self.write(response_data)


class SelectDataFrame(CommonBaseHandler):
    @logger_ob.log_er
    def post(self):   ##################################
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        print('b1')
        sheets = ""
        file_picker_file_name = ""
        joined_col = self.get_argument('joined_col', default="")
        file_picker = self.get_argument('file_picker', default="")
        from_cleaning_mod = self.get_argument('from_cleaning_mod', default="")
        print(file_picker)
        if from_cleaning_mod != 'true':
            if file_picker:
                print('B2')
                path = "BackEnd/uploads/"
                file_picker_file_name = ""
                single_file_name =""
                # get_file_picker_data = self.get_data_list(query_obj.get_file_name_data % (pymysql.escape_string(file_picker)))
                # if get_file_picker_data:
                #     file_picker_file_name = get_file_picker_data[0]["file_location"]
                #     path = '/'.join(file_picker_file_name.split("/")[:-1]) + '/'
                #     single_file_name = file_picker_file_name.split("/")[-1]
                # sheets = core_fun.get_sheets(file_picker_file_name)
                single_file_name = file_picker+".xlsx"
                data = generateDataframes(path, [single_file_name])
                core_fun.set_dataframes_list(data)
                # joined_cols = [dc['Joined on'] for dc in data]
            dt = core_fun.set_dataframe(joined_col)
        else:
            dt = True
        all_columns = core_fun.get_columns()
        date_columns = core_fun.get_all_date_columns()
        date_min_max_columns = []
        if date_columns:
            date_min_max_columns = core_fun.get_all_min_max_date_values(date_columns)
        else:
            date_min_max_columns = []
        if dt:
            response_data = json.dumps({
                'success': {
                    'code': "200",
                    'message': 'data fetched successfully',
                    'be_data': dt,
                    'all_columns': all_columns,
                    'sheet_name': sheets[0] if sheets else "",
                    'file_name': file_picker_file_name,
                    'date_min_max_columns': date_min_max_columns
                }
            }, default=self.date_handler)
        else:
            response_data = json.dumps({
                'error': {
                    'code': "400",
                    'message': error_obj.error_data_not_found
                }
            }, default=self.date_handler)
        self.write(response_data)


def generateDataframes(path, files_list): #, mergeallcommon=True):
    ''' 
    This function has a keyword argument mergeallcommon which is True by default. If the user wants to merge on individual columns,
    user will have to set it to False.

    Changes done : removed the option to select how to merge the columns. Will get every dataframe in list
    '''

    # try:
    print(path)
    print(files_list)
    columnsList = []
    dfs = []
    fldfs = []
    def getcommoncols(collist):
        print("------------")
        print(collist)
        result = set(collist[0])
        print(result)
        for cols in collist[1:]:
            result.intersection_update(cols)

        return list(result)

    for i in range(len(files_list)):
        print(path+files_list[i])
        df = pd.read_excel(path+files_list[i])
        fldfs.append(df)
        print(list(df.columns))
        columnsList.append(list(df.columns))

    commoncols = getcommoncols(columnsList)

    # if mergeallcommon: {
    if commoncols:
        df1 = pd.DataFrame(columns=commoncols)
        for i in range(len(fldfs)):
            df1 = df1.merge(fldfs[i], on=commoncols, how='inner') if i else fldfs[i]
        dfs.append({
            'Joined on': 'All common',
            'DataFrame': df1
        })
        # }

        # else: {
        for col in commoncols:
            df1 = pd.DataFrame(columns=[col])
            for i in range(len(fldfs)):
                df1 = df1.merge(fldfs[i], on=col, how='inner') if i else fldfs[i]
            dfs.append({
                'Joined on': col,
                'DataFrame': df1
            })
        # }
        return dfs
    else: 
            return False
    # except:
    #     print("Exception occured!")


class ManagerViewAllPlans(CommonBaseHandler):
    @logger_ob.log_er
    def post(self):
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        value = {}
        value1 = self.get_argument("unit", default="", strip=False)
        if value1 != "":
            value["unit"] = value1

        value1 = self.get_argument("department", default="", strip=False)
        if value1 != "":
            value["department"] = value1

        value1 = self.get_argument("sub_department", default="", strip=False)
        if value1 != "":
            value["sub_department"] = value1

        value1 = self.get_argument("role", default="", strip=False)
        if value1 != "":
            value["role"] = value1

        value1 = self.get_argument("product", default="", strip=False)
        if value1 != "":
            value["product"] = value1

        value1 = self.get_argument("status", default="", strip=False)
        if value1 != "":
            value["status"] = value1

        plan_limit = self.get_argument("plan_limit", default="0", strip=False)

        get_all_plans = query_obj.get_all_filter_plans

        for ele in value:
            temp = value[ele]
            get_all_plans += ele + " LIKE '%" + temp + "%' AND "
        get_all_plans = get_all_plans[:-4]
        get_all_plans += " ORDER BY created_at desc LIMIT "+str(plan_limit)+",9"

        print(get_all_plans)

        count,total_rows,plans = self.get_this_data_count(get_all_plans)
        if plans:
            stats = {
                'approved_plans' : 0,
                'rejected_plans' : 0,
                'closed_plans' : 0,
                'pending_plans' : 0,
                'total_plans' : 0
            }
            created_dates = []
            from_duration = []
            to_duration = []
            pprint(plans)
            for i in range(len(plans)):
                stats['total_plans'] = stats['total_plans'] + 1
                if (plans[i]['status'] == "approved"):
                    stats['approved_plans'] = stats['approved_plans'] + 1
                elif (plans[i]['status'] == "rejected"):
                    stats['rejected_plans'] = stats['rejected_plans'] + 1
                elif (plans[i]['status'] == "closed"):
                    stats['closed_plans'] = stats['closed_plans'] + 1
                elif (plans[i]['status'] == "pending"):
                    stats['pending_plans'] = stats['pending_plans'] + 1

                created_dates.append(humanize.naturaldate(plans[i]['created_at']))
                from_duration.append(humanize.naturaldate(plans[i]['plan_start_date']))
                to_duration.append(humanize.naturaldate(plans[i]['plan_end_date']))
            response_data = json.dumps({
                    'success': {
                        'code': "200",
                        'message': "plans fetched successfully",
                        'data': plans,
                        'created_dates': created_dates,
                        'from_duration': from_duration,
                        'to_duration': to_duration,
                        'fetch_count': count,
                        'total_count': total_rows
                    }
                },
                default=str,
                indent=4)
        else:
            response_data = json.dumps({
                'error': {
                    'code': "404",
                    'message': "No plans found",
                    'fetch_count': count,
                    'total_count': total_rows
                }
            })
            
        self.write(response_data)
        self.finish()


    def get(self):
        plans = self.get_this_data(query_obj.get_all_plans)
        if plans:
            stats = {
                'approved_plans' : 0,
                'rejected_plans' : 0,
                'closed_plans' : 0,
                'pending_plans' : 0,
                'total_plans' : 0
            }
            created_dates = []
            from_duration = []
            to_duration = []
            pprint(plans)
            for i in range(len(plans)):
                stats['total_plans'] = stats['total_plans'] + 1
                if (plans[i]['status'] == "approved"):
                    stats['approved_plans'] = stats['approved_plans'] + 1
                elif (plans[i]['status'] == "rejected"):
                    stats['rejected_plans'] = stats['rejected_plans'] + 1
                elif (plans[i]['status'] == "closed"):
                    stats['closed_plans'] = stats['closed_plans'] + 1
                elif (plans[i]['status'] == "pending"):
                    stats['pending_plans'] = stats['pending_plans'] + 1

                created_dates.append(humanize.naturaldate(plans[i]['created_at']))
                from_duration.append(humanize.naturaldate(plans[i]['plan_start_date']))
                to_duration.append(humanize.naturaldate(plans[i]['plan_end_date']))
        self.render("dev_manager_view_plans.html",settings=Settings.configs,error=None, plans=plans, stats=stats, created_dates=created_dates,from_duration=from_duration, to_duration=to_duration)


class DevProductivityUpload(CommonBaseHandler):
    # @logger_ob.log_er
    # @logger_ob.with_sys_logging
    def sql_dump(self, file):
        return self.write_data(file)

    def post(self):
        file1 = self.request.files['file1'][0]
        original_fname = str(file1['filename'])
        file_location = "BackEnd/ninja/emp_csv_uploads/" + original_fname
        response_data = {}

        with open(file_location, 'wb') as output_file:
            output_file.write(file1['body'])
        # status = self.sql_dump(file_location)
        data_f = pd.read_excel(file_location)
        # df = data_f.reset_index()
        
        dt_t = data_f.transpose()
        print(dt_t)
        indexes = dt_t.index
        for i in indexes:
            print(i,'>>>>>>')
            print(dt_t.loc[i]['Quartiles'])
            single_quarter_data = self.get_data_list(query_obj.get_single_quarter_rl_data % (str(i)))
            if single_quarter_data:
                self.DB_write_operation(query_obj.update_single_quarter_rl_data % (str(dt_t.loc[i]['Quartiles']),
                 str(dt_t.loc[i]['Disbursement']), str(dt_t.loc[i]['% Contri']), 
                 str(dt_t.loc[i]['Quartile Median']),str(dt_t.loc[i]['LOB Median']),str(i)))
            else:
                self.DB_write_operation(query_obj.create_single_quarter_rl_data % (str(i),str(dt_t.loc[i]['Quartiles']),
                 str(dt_t.loc[i]['Disbursement']), str(dt_t.loc[i]['% Contri']), 
                 str(dt_t.loc[i]['Quartile Median']),str(dt_t.loc[i]['LOB Median'])))


        response_data = json.dumps({
            'success': {
                'code': '200',
                'message': "Success",
            }   
        })

        self.write(response_data)
        self.finish()


class GenerateProductivityGraph(CommonBaseHandler):
    @logger_ob.log_er
    @logger_ob.with_sys_logging
    def get(self):
        master_plan_details = self.get_this_data(query_obj.get_all_master_plan_details)
        self.render("dev_manager_productivity_graph.html",settings=Settings.configs,error=None, master_plan_details=json.dumps(master_plan_details, default=self.date_handler))

        # productivity_details = self.get_this_data(query_obj.get_productivity_graph_data)
        # self.render("dev_manager_productivity_graph.html",settings=Settings.configs,error=None, productivity_details=json.dumps(productivity_details, default=self.date_handler))

    def post(self):
        master_plan_id = self.get_argument("master_plan_id", default="", strip=False)
        master_plan_name = self.get_argument("master_plan_name", default="", strip=False)

        from_duration = self.get_argument("from_duration", default="", strip=False)
        to_duration = self.get_argument("to_duration", default="", strip=False)

        response_data = self.calculate_incentive_api(master_plan_id, from_duration, to_duration)
        response_data_obj = json.loads(response_data)
        final_resp_obj={}
        if "success" in response_data_obj:
            resp_data_json = json.dumps(response_data_obj["success"]["data"])

            incentive_df = pd.read_json(resp_data_json)

            disbursal_value_functions = incentive_df["actual disbursal value"].describe(include='all')
            
            stat_dict = {}
            
            q1_stat_dict = {}
            q1_stat_dict["start"] =  disbursal_value_functions["min"]
            q1_stat_dict["end"] =  disbursal_value_functions["25%"]

            q2_stat_dict = {}
            q2_stat_dict["start"] =  disbursal_value_functions["25%"]
            q2_stat_dict["end"] =  disbursal_value_functions["50%"]

            q3_stat_dict = {}
            q3_stat_dict["start"] =  disbursal_value_functions["50%"]
            q3_stat_dict["end"] =  disbursal_value_functions["75%"]

            q4_stat_dict = {}
            q4_stat_dict["start"] =  disbursal_value_functions["75%"]
            q4_stat_dict["end"] =  disbursal_value_functions["max"]

            stat_dict["Q1"] = q1_stat_dict
            stat_dict["Q2"] = q2_stat_dict
            stat_dict["Q3"] = q3_stat_dict
            stat_dict["Q4"] = q4_stat_dict

            stat_df = pd.DataFrame(stat_dict).transpose()

            q1_disbursals = [0.0]
            q2_disbursals = [0.0]
            q3_disbursals = [0.0]
            q4_disbursals = [0.0]
            all_disbursals = []

            indexes = incentive_df.index
            for i in indexes:
                ach_disbursal = float(incentive_df.loc[i]['actual disbursal value'])
                print(ach_disbursal)
                all_disbursals.append(ach_disbursal)
                if(ach_disbursal >= float(q1_stat_dict["start"]) and ach_disbursal <= float(q1_stat_dict["end"])):
                    incentive_df.loc[i, "Quartiles"] = "Q1"
                    q1_disbursals.append(ach_disbursal)
                elif(ach_disbursal > float(q2_stat_dict["start"]) and ach_disbursal <= float(q2_stat_dict["end"])):
                    incentive_df.loc[i, "Quartiles"] = "Q2"
                    q2_disbursals.append(ach_disbursal)
                elif(ach_disbursal > float(q3_stat_dict["start"]) and ach_disbursal <= float(q3_stat_dict["end"])):
                    incentive_df.loc[i, "Quartiles"] = "Q3"
                    q3_disbursals.append(ach_disbursal)
                elif(ach_disbursal > float(q4_stat_dict["start"]) and ach_disbursal <= float(q4_stat_dict["end"])):
                    incentive_df.loc[i, "Quartiles"] = "Q4"
                    q4_disbursals.append(ach_disbursal)

            q1_median = statistics.median(q1_disbursals)
            q2_median = statistics.median(q2_disbursals)
            q3_median = statistics.median(q3_disbursals)
            q4_median = statistics.median(q4_disbursals)
            lob_median = statistics.median(all_disbursals)

            q1_disbursals_sum = sum(q1_disbursals)
            q2_disbursals_sum = sum(q2_disbursals)
            q3_disbursals_sum = sum(q3_disbursals)
            q4_disbursals_sum = sum(q4_disbursals)
            total_disbursals = sum(all_disbursals)

            if(total_disbursals):
                q1_contri_percent = (q1_disbursals_sum/total_disbursals) * 100
                q2_contri_percent = (q2_disbursals_sum/total_disbursals) * 100
                q3_contri_percent = (q3_disbursals_sum/total_disbursals) * 100
                q4_contri_percent = (q4_disbursals_sum/total_disbursals) * 100

            main_obj = {}
            unique_quartiles = incentive_df["Quartiles"].unique()
            for i in sorted(unique_quartiles):
                quartile_obj = {}
                if i == "Q1":
                    quartile_obj["quartile_disbursement"] = disbursal_value_functions["min"]
                    quartile_obj["percent_contri"] = q1_contri_percent
                    quartile_obj["quartile_median"] = q1_median
                    quartile_obj["lob_median"] = lob_median
                    main_obj[i] = quartile_obj
                if i == "Q2":
                    quartile_obj["quartile_disbursement"] = disbursal_value_functions["25%"]
                    quartile_obj["percent_contri"] = q2_contri_percent
                    quartile_obj["quartile_median"] = q2_median
                    quartile_obj["lob_median"] = lob_median
                    main_obj[i] = quartile_obj
                if i == "Q3":
                    quartile_obj["quartile_disbursement"] = disbursal_value_functions["50%"]
                    quartile_obj["percent_contri"] = q3_contri_percent
                    quartile_obj["quartile_median"] = q3_median
                    quartile_obj["lob_median"] = lob_median
                    main_obj[i] = quartile_obj
                if i == "Q4":
                    quartile_obj["quartile_disbursement"] = disbursal_value_functions["75%"]
                    quartile_obj["percent_contri"] = q4_contri_percent
                    quartile_obj["quartile_median"] = q4_median
                    quartile_obj["lob_median"] = lob_median
                    main_obj[i] = quartile_obj
                
            final_resp_obj = json.dumps({
                'success': {
                    'code': '200',
                    'message': "Success",
                    'data': main_obj
                }
            })
        else:
            final_resp_obj = response_data;
        self.write(final_resp_obj)
        self.finish()


class GetFiles(CommonBaseHandler):             
    @logger_ob.log_er
    def post(self): #####################
        self.set_headerlist()
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        modal_flag = self.get_argument('modal_flag', default=0)  # To check if files from particular directory or 'uploads'
        file_names = core_fun.get_uploaded_files() if modal_flag else core_fun.get_uploaded_files() 
        # mapped_data = self.get_user_filename_mapping(file_names)
        # pprint(mapped_data)
        response_data = json.dumps({
                    'success': {
                        'code': "200",
                        'message': success_obj.success_get_data,
                        'data': file_names
                    }
                },default=self.date_handler)
        print("===================================")
        print(response_data)
        self.write(response_data)
        self.finish()

class MetricUpload(CommonBaseHandler):             
    def metrics_sql_dump(self, file):
        return self.metrics_write_data(file)

    def post(self):
        file1 = self.request.files['file1'][0]
        original_fname = str(file1['filename'])
        file_location = "BackEnd/ninja/metrics_uploads/" + original_fname
        response_data = {}

        with open(file_location, 'wb') as output_file:
            output_file.write(file1['body'])
        status = self.metrics_sql_dump(file_location)
        if status['status'] == 0:
            response_data = json.dumps({
                'error': {
                    'code': 0,
                    'message': status["message"],
                }
            })
        else:
            response_data = json.dumps({
            'success': {
                'code': 1,
                'message': "Success",
            }
        })

        self.write(response_data)
        self.finish()
        