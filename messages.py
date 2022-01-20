class errors():
    def __init__(self):
        self.error_code_unauthorized = 401
        self.error_code_not_found = 404
        self.error_code_resource_exists = 405
        self.error_code_forbidden = 403
        self.error_message_password_match = "New password match failed"
        self.error_message_empty_field = "Email or password is empty"
        self.error_message_username_exists = "User name already exists"
        self.error_message_unauthorized_user = "Admin not found or either deleted"
        self.error_message_login_fail = "Sorry. Login failed"
        self.error_message_access_error = "Unauthorized access"
        self.error_message_job_expired = "The job is either closed or is not published."
        self.error_password_repassword_do_not_match = ["Entered Password does not fulfill the password policy. Possible reasons:", "1.Password and re-enter password does not match.","2.Password must be atleast 8 characters.","3.Password must contain atleast 1 uppercase character.","4.Password must contain atleast 1 lowercase character.","5.Password must contain atleast 1 digit.","6.Password must contain atleast 1 special character."]
        self.age_flag_error = "Age flag update failed"
        self.error_file_unavailable = "File not found!"
        self.error_data_not_found = 'Data not Found'
        self.error_file_invalid_columns = 'Invalid Column names found'
        self.error_file_invalid = 'Invalid file uploaded'
        self.error_sum_axis = 'Sum can only be plotted for Numeric Graph'
        self.error_value_axis = 'Actual values can only be plotted for Non-categorical(X-axis) vs Numeric(Y-axis) graph'

class success():
    def __init__(self):
        self.success_code_Ok = 200
        self.success_code_created = 201
        self.success_message_user = "Login Successfull......"
        self.success_message_admin = "ADMIN LOGIN Successfull......"
        self.success_job_create = "Job successfully created"
        self.success_job_update = "Job successfully updated"
        self.success_candidate_status = "Successfully updated candidate's status"
        self.success_role_data = "Role data fetched"
        self.age_flag_success = "Age flag updated successfully"
        self.success_upload_successful = "File Uploaded Successfully"
        self.success_get_data = "Successfully Data Fetched"

