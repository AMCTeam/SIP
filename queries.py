from settings import MYSQLConfiguration
db_obj = MYSQLConfiguration()
class SQLqueries(): 
    def __init__(self):
        self.create_master_plan ="INSERT INTO `master_plan_details` (`plan_name`, `unit`, `department`, `sub_department`, `role`, `product`, `plan_start_date`, `plan_end_date`,`created_at`, `status`) VALUES ( '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s',NOW(),'pending')"

        self.create_base_metric_plan = "INSERT INTO base_metric_plan (metric_name, weight,scale, slab_details,scale_max_cap,master_plan_details, created_at) VALUES ('%s','%s','%s','%s','%s',%d,NOW())"

        self.create_rider_plan = "INSERT INTO rider_plan (rider_name, min_range,max_range, reduction_value,master_plan_details, created_at) VALUES ('%s','%s','%s','%s',%d,NOW())"

        self.create_payout_plan = "INSERT INTO payout_plan (payout_term, tranche_1_payout_percent,tranche_2_payout_percent, min_achievement_percent,annual_payout_cap_percent,applicability_factor, payout_slab, master_plan_details, created_at) VALUES ('%s','%s','%s','%s','%s','%s','%s', %d,NOW())"

        self.get_master_plan_details = "SELECT * FROM master_plan_details WHERE id = %d AND deleted_at IS NULL"

        self.get_base_metric_plan_details = "SELECT * FROM base_metric_plan WHERE master_plan_details = %d AND deleted_at IS NULL"

        self.get_rider_details = "SELECT * FROM rider_plan WHERE master_plan_details = %d AND deleted_at IS NULL"

        self.get_payout_details = "SELECT * FROM payout_plan WHERE master_plan_details = %d AND deleted_at IS NULL"

        self.update_master_plan = "UPDATE master_plan_details SET plan_name='%s', unit='%s', department='%s', sub_department='%s', role='%s', product='%s', plan_start_date='%s', plan_end_date='%s', status='%s', updated_at=NOW() WHERE id = %d"

        self.delete_base_metric_plan = "DELETE from base_metric_plan where master_plan_details=%d";

        self.delete_rider_plan = "DELETE from rider_plan WHERE master_plan_details=%d";

        self.update_payout_plan = "UPDATE payout_plan SET payout_term='%s', tranche_1_payout_percent='%s',tranche_2_payout_percent='%s', min_achievement_percent='%s',"\
        							"annual_payout_cap_percent='%s', applicability_factor='%s', payout_slab='%s', updated_at=NOW() WHERE master_plan_details = %d"

        self.get_all_master_plan_details = "SELECT * FROM master_plan_details";

        self.get_emp_perf_data = "SELECT * FROM employee_rl WHERE product ='%s' AND record_date BETWEEN '%s' AND '%s'"

        self.get_single_emp_performance = "SELECT * FROM employee_rl WHERE emp_code='%s' ORDER BY record_date desc LIMIT 1"

        self.get_all_filter_plans = "SELECT SQL_CALC_FOUND_ROWS * from master_plan_details WHERE deleted_at IS NULL AND "

        self.get_all_plans = "SELECT * from master_plan_details WHERE deleted_at IS NULL ORDER BY created_at desc"

        self.get_single_quarter_rl_data = "SELECT * FROM productivity_quartiles WHERE quarter = '%s'"

        self.update_single_quarter_rl_data = "UPDATE productivity_quartiles SET disbursement_percentile='%s',disbursement='%s',contri_percent='%s',quartile_median='%s',lob_median='%s', updated_at=NOW() WHERE quarter = '%s'"

        self.create_single_quarter_rl_data = "INSERT INTO productivity_quartiles(quarter,disbursement_percentile,disbursement,contri_percent,quartile_median,lob_median,created_at) VALUES ('%s','%s','%s','%s','%s','%s',NOW())"

        self.get_productivity_graph_data ="SELECT * FROM productivity_quartiles"

        self.get_metric_details = "SELECT * FROM master_metrics"