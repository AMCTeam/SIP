import tornado.httpserver
import tornado.ioloop
import tornado.web
import os
import pymysql
from settings import Settings as set
from settings import MYSQLConfiguration
from admin_api import *
from tornado.web import url
# from common import CustomStaticFileHandler

Settings = set()
db_obj = MYSQLConfiguration()
import rsa



if __name__ == "__main__":
    os.chdir('..')
    __file__ = str(os.getcwd()) + "/FrontEnd/"
    app = tornado.web.Application([
        (r"/manager/master_plan_generator", MasterPlanGenerator),
        (r"/manager/uploads", ManagerUploads),
        (r"/manager/view_incentive", EmployeeViewIncentive),
        (r"/manager/simulate_plans", PlanSimulator),

        (r"/dev/manager/master_plan_generator", DevMasterPlanGenerator),
        (r"/dev/manager/uploads", DevManagerUploads),
        (r"/dev/manager/view_incentive", DevEmployeeViewIncentive),
        (r"/dev/manager/simulate_plans", DevPlanSimulator),
        (r"/dev/employee/view_incentive/([^/]+)", DevEmployeeViewIncentiveEmp),
        (r"/dev/manager/uploads/employee_performance", DevEmployeePerformanceUploads),
        (r"/dev/manager/view/master_plan/([^/]+)", ViewUpdateSinglePlan),
        (r"/dev/manager/simulate/master_plan", SimulateMasterPlan),

        (r"/compare_upload_api", CompareUpload),
        (r"/compare_param_api", GetCompareParams),
        (r"/get_comparison_date", GetComparisonDates),
        (r"/group", GroupBy),
        (r"/select_dataframe", SelectDataFrame),
        (r"/dev/manager/SimulateHRA/", SimulateHRA),
        (r"/dev/manager/view/all_plans", ManagerViewAllPlans),
        (r"/dev/manager/uploads/productivity_quartile", DevProductivityUpload),
        (r"/dev/manager/productivity_graph", GenerateProductivityGraph),
        (r"/get_files",GetFiles),
        (r"/dev/manager/uploads/master_metrics",MetricUpload)
    ],
    cookie_secret="46osdETzKXasdQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
    template_path=__file__,
    static_path=__file__,
    # static_handler_class=CustomStaticFileHandler,
    debug=True)
    app.db = pymysql.connect(db_obj.host, db_obj.user, db_obj.password, db_obj.db)
    # app.rsa_keys = rsa.newkeys(1024)
    server = tornado.httpserver.HTTPServer(app)
    server.listen(Settings.port)
    print("Server running on port "+str(Settings.port))
    tornado.ioloop.IOLoop.current().start()
    print("Server stopped ...")