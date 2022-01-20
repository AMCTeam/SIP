import base64
import smtplib

class Settings:
    def __init__(self):
        self.port = "8005"
        self.port_candidate = "8004"
        self.configs = {
            # "host": "http://52.66.201.73:"+self.port+"/",
            "host": "http://localhost:"+str(self.port)+"/",
            "default_mail_to": "rhishikesh@myraatechnologies.com",
            "default_mail_from": "techteammyraa@gmail.com", 
            "default_mail_cc": "rhishikesh@myraatechnologies.com",
            # "host_candidate": "http://52.66.201.73:" + str(self.port_candidate)
            "host_candidate": "http://localhost:" + str(self.port_candidate),
        }
        self.head_hr = {
            "name":"Sujatha",
            "email":"rhishikesh@myraatechnologies.com"
        }

        self.l3 = {
            "name":"Jay",
            "email":"rhishikesh@myraatechnologies.com"
        }
        self.adfs = False
        self.error_url = "https://login.adityabirlacapital.com/adfs/oauth2/authorize?client_id=d8bd4859-08fe-4817-a0f9-05645026a936&response_type=code&redirect_uri=http://52.66.201.73:8003/cops/auth/success&duration=temporary&scope=profile"

    def get_smtp_settings(self):
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(base64.b64decode(b'dGVjaHRlYW1teXJhYUBnbWFpbC5jb20=').decode('utf-8'), base64.b64decode(b'TXlyYWFAMTEyMg==').decode('utf-8'))
        return s

class MYSQLConfiguration():
    def __init__(self):
        self.host = "localhost"
        self.user = "development"
        self.password = "12345"
        # self.password = ""
        self.salt = "53675842777378344275795A6B4B3473"
        self.db = "productivity"
