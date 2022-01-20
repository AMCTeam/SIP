import requests
import jwt
import tornado.web
import tornado.escape
from datetime import datetime
import traceback
# from common import CommonBaseHandler
# common_obj = CommonBaseHandler()
from Crypto.Cipher import AES
import base64
import binascii
import urllib.parse
from Crypto import Random
import hashlib
import pymysql
from settings import Settings as set

Settings = set()
BS = 16
pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
unpad = lambda s : s[0:-ord(s[-1])]

class ADFSAuthenticationSuccess(tornado.web.RequestHandler):
    def __init__(self, client_id = "", client_secret = "", adfs_token_url= "", redirect_uri="", logout_url="", salt="", storage="", adfs_post_success_url="", adfs_post_error_url="",adfs_mysql_db_host="",adfs_mysql_db_user="", adfs_mysql_db_password="",adfs_mysql_db_name="",adfs_authorize_url=""): 
        self._client_id = client_id
        self._client_secret = client_secret
        self._adfs_token_url = adfs_token_url
        self._redirect_url = redirect_uri
        self._logout_url = logout_url
        self._salt = salt
        self._storage = storage
        self._adfs_post_success_url = adfs_post_success_url
        self._adfs_post_error_url = adfs_post_error_url
        self._adfs_mysql_db_host = adfs_mysql_db_host
        self._adfs_mysql_db_user = adfs_mysql_db_user
        self._adfs_mysql_db_password = adfs_mysql_db_password
        self._adfs_mysql_db_name = adfs_mysql_db_name
        self._adfs_authorize_url = adfs_authorize_url
        

    # getter method 
    def get_client_id(self): 
        return self._client_id 
      
    # setter method 
    def set_client_id(self, cli_id): 
        self._client_id = cli_id

    # getter method 
    def get_client_secret(self): 
        return self._client_secret 
      
    # setter method 
    def set_client_secret(self, cli_secret): 
        self._client_secret = cli_secret

    # getter method 
    def get_adfs_token_url(self): 
        return self._adfs_token_url 
      
    # setter method 
    def set_adfs_token_url(self, adfs_tk_url): 
        self._adfs_token_url = adfs_tk_url

    # getter method 
    def get_redirect_url(self): 
        return self._redirect_url 
      
    # setter method 
    def set_redirect_url(self, adfs_redirect_url): 
        self._redirect_url = adfs_redirect_url

    # getter method 
    def get_logout_url(self): 
        return self._logout_url 
      
    # setter method 
    def set_logout_url(self, adfs_logout_url): 
        self._logout_url = adfs_logout_url

    # getter method 
    def get_salt(self): 
        return self._salt 
      
    # setter method 
    def set_salt(self, salt): 
        self._salt = salt

    # getter method 
    def get_db_storage(self): 
        return self._storage 
      
    # setter method 
    def set_db_storage(self, storage): 
        self._storage = storage

    # getter method 
    def get_adfs_post_success_url(self): 
        return self._adfs_post_success_url
      
    # setter method 
    def set_adfs_post_success_url(self, adfs_post_success_url): 
        self._adfs_post_success_url = adfs_post_success_url

     # getter method 
    def get_adfs_post_error_url(self): 
        return self._adfs_post_error_url
      
    # setter method 
    def set_adfs_post_error_url(self, adfs_post_error_url): 
        self._adfs_post_error_url = adfs_post_error_url

    def get_mysql_db_host(self):
        return self._adfs_mysql_db_host

    def set_mysql_db_host(self,adfs_mysql_db_host):
        self._adfs_mysql_db_host = adfs_mysql_db_host

    def get_mysql_db_user(self):
        return self._adfs_mysql_db_user

    def set_mysql_db_user(self,adfs_mysql_db_user):
        self._adfs_mysql_db_user = adfs_mysql_db_user

    def get_mysql_db_password(self):
        return self._adfs_mysql_db_password

    def set_mysql_db_password(self,adfs_mysql_db_password):
        self._adfs_mysql_db_password = adfs_mysql_db_password

    def get_mysql_db_name(self):
        return self._adfs_mysql_db_name

    def set_mysql_db_name(self, adfs_mysql_db_name):
        self._adfs_mysql_db_name = adfs_mysql_db_name

    def get_adfs_authorize_url(self):
        return self._adfs_authorize_url

    def set_adfs_authorize_url(self,adfs_authorize_url):
        self._adfs_authorize_url = adfs_authorize_url

    def set_db_settings(self):
        if self.get_db_storage() == "mysql":
            # import pymysql
            self.db = pymysql.connect(self.get_mysql_db_host(), self.get_mysql_db_user(), self.get_mysql_db_password(), self.get_mysql_db_name())
            # self.db = pymysql.connect("localhost","development","12345","cubic_hra")

    def get_access_token_by_code(self, auth_code):
        data = dict(
            scope="profile",
            client_id= self.get_client_id(),
            client_secret= self.get_client_secret(),
            redirect_uri= self.get_redirect_url(),
            grant_type='authorization_code',
            code=auth_code
        )

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        response = requests.post(self.get_adfs_token_url(), data=data, headers=headers)
        if response.status_code not in [400,401,415,500,503]:
            response = response.json()
            access_tokens_dict = {}
            access_tokens_dict['access_token'] = response.get('access_token')
            access_tokens_dict['token_type'] = response.get('token_type')
            access_tokens_dict['access_token_expires_in'] = response.get('expires_in')
            access_tokens_dict['resource'] = response.get('resource')
            access_tokens_dict['refresh_token'] = response.get('refresh_token')
            access_tokens_dict['refresh_token_expires_in'] = response.get('refresh_token_expires_in')
            access_tokens_dict['id_token'] = response.get('id_token')
            access_tokens_dict['scope'] = response.get('scope')
            return access_tokens_dict
        else:
            return False

    def get_access_token_by_refresh_token(self, refresh_token):
        data = dict(
            client_id = self.get_client_id(),
            refresh_token = refresh_token,
            client_secret=self.get_client_secret(),
            grant_type='refresh_token',
            scope='profile'
        )

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        response = requests.post(self.get_adfs_token_url(), data=data, headers=headers)
        if response.status_code not in [400,401,415,500,503]:
            response = response.json()
            refresh_tokens_dict = {}
            refresh_tokens_dict['access_token'] = response.get('access_token')
            refresh_tokens_dict['token_type'] = response.get('token_type')
            refresh_tokens_dict['access_token_expires_in'] = response.get('expires_in')
            refresh_tokens_dict['id_token'] = response.get('id_token')
            refresh_tokens_dict['scope'] = response.get('scope')
            return refresh_tokens_dict
        else:
            return False

    def check_auth_session(self, admin_login, emp_level, cookies):
        if Settings.adfs:
            if cookies["access_token"] and cookies["cops_user"]:
                access_token_cookie = tornado.escape.native_str(cookies["access_token"])
                adid_cookie = tornado.escape.native_str(cookies["cops_user"])
                if access_token_cookie and adid_cookie:
                    print("a1")
                    print(self.get_db_storage())
                    if self.get_db_storage() == "mysql":
                        print("a2")
                        decoded_access_token_client = jwt.decode(access_token_cookie, verify=False)
                        adfs_active_manager_data = self.get_this_data("SELECT * from adfs_manager_sessions WHERE manager_email ='%s' AND session_state='active'" % str(decoded_access_token_client["email"]))
                        print(decoded_access_token_client["email"])
                        if adfs_active_manager_data:
                            print("a3")
                            decoded_access_token_db = jwt.decode(adfs_active_manager_data[0]["access_token"], verify=False)
                            if access_token_cookie == adfs_active_manager_data[0]["access_token"]:
                                print("a4")
                                if self.access_token_expiry(decoded_access_token_db["exp"]):
                                    print("a5")
                                    return [True,None]
                                else:
                                    new_access_token_data = self.get_access_token_by_refresh_token(adfs_active_manager_data[0]["refresh_token"])
                                    print("a6")
                                    if new_access_token_data:
                                        print("a7")
                                        update_access_token_cursor = self.db.cursor()
                                        update_access_token = "UPDATE adfs_manager_sessions SET access_token ='%s',id_token='%s' WHERE manager_email= '%s' AND session_state='active'" % (new_access_token_data["access_token"],new_access_token_data["id_token"],decoded_access_token_client["email"])
                                        update_access_token_cursor.execute(update_access_token)
                                        update_access_token_cursor.close()
                                        self.db.commit()
                                        return [True, new_access_token_data["access_token"]]
                                    else:
                                        return [False,None]
                            else:
                                return [False,None]
                        else:
                            return [False,None]
                else:
                    return [False, None]
            else: return [False, None]
        else:
            if cookies["cops_user"] and cookies["cops_user"].decode() == str(admin_login) and cookies["cops_level"].decode() == str(emp_level):
                return [True,None]
            else:
                return[False,None]


    def access_token_expiry(self, access_token_exp):
        token_exp_date = datetime.fromtimestamp(access_token_exp).strftime('%Y-%m-%d %H:%M:%S')
        utc_current_date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        if utc_current_date < token_exp_date:
            return True
        else:
            return False

    def set_session(self, all_oauth_arguments):
        access_token_details = self.get_access_token_by_code(all_oauth_arguments["code"][0].decode())
        print("0")
        if access_token_details:
            print("1")
            decoded_access_token = jwt.decode(access_token_details["access_token"], verify=False)
            decoded_id_token = jwt.decode(access_token_details["id_token"], verify=False)
            manager_name = decoded_access_token["unique_name"]
            manager_email = decoded_access_token["email"]
            manager_adid = str(decoded_id_token["unique_name"]).split("\\")[1]
            manager_db_details = self.check_valid_manager(manager_adid)
            print("manager details")
            print(manager_adid)
            print(decoded_access_token)
            if manager_db_details:
                print("2")
                if self.get_db_storage() == "mysql":
                    get_manager_login_details = self.get_this_data("SELECT * from adfs_manager_sessions WHERE manager_adid ='%s'" % manager_adid)
                    manager_adfs_cursor = self.db.cursor()
                    if get_manager_login_details:
                        manager_update = "UPDATE adfs_manager_sessions SET access_token ='%s', session_state ='active', created_at ='%s',updated_at = '%s', refresh_token = '%s', id_token='%s', emp_id='%s' WHERE manager_adid= '%s'" % (access_token_details["access_token"],datetime.now(), datetime.now(),access_token_details["refresh_token"],access_token_details["id_token"],manager_db_details[0]["employee_id"], manager_adid)
                        manager_adfs_cursor.execute(manager_update)
                        self.db.commit()
                        print("update")
                    else:
                        manager_add = "INSERT INTO adfs_manager_sessions(manager_email,manager_name,access_token,session_state,created_at,updated_at,refresh_token,manager_adid,id_token, emp_id) VALUES('%s','%s','%s','active','%s','%s','%s','%s','%s','%s')" % (manager_email, manager_name, access_token_details["access_token"],datetime.now(), datetime.now(),access_token_details["refresh_token"],manager_adid,access_token_details["id_token"], manager_db_details[0]["employee_id"])
                        manager_adfs_cursor.execute(manager_add)
                        self.db.commit()
                        print("insert")
                    manager_adfs_cursor.close()
                    get_updated_manager_login_details = self.get_this_data("SELECT * from adfs_manager_sessions WHERE manager_adid ='%s'" % manager_adid)
                print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
                # encrypted_adid = self.encrypt(manager_adid,self.get_salt())
                # print(encrypted_adid)
                manager_level = manager_db_details[0]["level"]
                return [True, access_token_details["access_token"], str(manager_db_details[0]["employee_id"]), str(manager_level)]
            else:
                print("fail 2")
                return [False]
        else:
            print("fail 1")
            return [False]

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
            print("h1 Error: "+ str(e))
            traceback.print_exc()
            return 0
        
    def decrypt(self,enc, salt):
        try:
            private_key = hashlib.sha256(salt.encode("utf-8")).digest()
            enc = enc.strip('=')
            enc += '=' * (-len(enc) % 4)
            enc1 = base64.b64decode(enc)
            # enc = str(self.relaxed_decode_base64(enc))
            iv = enc1[:16]
            cipher = AES.new(private_key, AES.MODE_CBC, iv)
            return self.unpad(cipher.decrypt(enc1[16:]))    
        except  Exception as e:
            print("h2 Error: "+ str(e))
            traceback.print_exc()
            return 0
        
    def url_encode(self,val):
        encoded = urllib.parse.urlencode({'': val})
        return encoded[1:len(encoded)]

    def url_decode(self,val):
        val = '=' + val
        decoded = urllib.parse.parse_qs(val)
        return decoded[""][0]

    def ADFSlogout(self, cookies):
        redirect_url = ""
        if Settings.adfs:
            if self.get_db_storage() == "mysql":
                if cookies["cops_user"]:
                    get_manager_id_token = self.get_this_data("SELECT id_token from adfs_manager_sessions WHERE emp_id ='%s' and session_state='active'" % str(tornado.escape.native_str(cookies["cops_user"])))
                    if get_manager_id_token:
                        manager_logout = self.db.cursor()
                        manager_logout_sql = "UPDATE adfs_manager_sessions SET session_state='inactive' WHERE emp_id ='%s'" % str(tornado.escape.native_str(cookies["cops_user"]))
                        manager_logout.execute(manager_logout_sql)
                        manager_logout.close()
                        self.db.commit()
                        redirect_url = self.get_logout_url()+str(get_manager_id_token[0]["id_token"])
                        print("logout success 1")
                        return [True, redirect_url]
                    else:
                        print("logout success 2")
                        return [False, Settings.error_url]
                else:
                    print("logout success 3")
                    return [False, Settings.error_url]
        else:
            return [True , "/admin/login"]
    
    def get_this_data(self, sql):
        self.db.commit()
        print(sql)
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
            print("get data error")
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

    def check_valid_manager(self, adid):
        check_manager_sql = "SELECT * FROM employee WHERE adid = '%s' and (employment_status = 'Active' or employment_status = 'active')" % pymysql.escape_string(adid)
        check_manager_data = self.get_this_data(check_manager_sql)
        if check_manager_data:
            return check_manager_data
        else:
            return False

    def relaxed_decode_base64(self, data):
        # If there is already padding we strim it as we calculate padding ourselves.
        if '=' in data:
            data = data[:data.index('=')]
        # We need to add padding, how many bytes are missing.
        missing_padding = len(data) % 4

        # We would be mid-way through a byte.
        if missing_padding == 1:
            data += 'A=='
        # Jut add on the correct length of padding.
        elif missing_padding == 2:
            data += '=='
        elif missing_padding == 3:
            data += '='
        # print(data)
        # Actually perform the Base64 decode.
        return base64.b64decode(data.replace(" ","+"))
