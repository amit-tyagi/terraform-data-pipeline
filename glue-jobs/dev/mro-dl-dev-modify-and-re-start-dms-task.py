import sys, boto3
from time import sleep
from logging import log
from json import dump, dumps, load
from collections import OrderedDict
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions

class ModifyAndStartDMSTask():
    """
    A class for modifying and starting DMS task with Python boto3 library
    """

    def __init__(self):
        log(30, "Modifying and re-starting DMS Task .....")

    # End __init__ method

    def modify_table_mapping_rules(self, table_mapping_filename=None, days_backward=0, in_development=True, meter_option=False):
        if table_mapping_filename:
            with open(table_mapping_filename) as json_data:
                data_to_modify = load(json_data, object_pairs_hook=OrderedDict)

            # 1. dump original data to variable
            original_data = dumps(data_to_modify)

            # 2. modify data
            new_date_value = datetime.utcnow().date() - timedelta(days=days_backward)
            data_to_modify["rules"][0]["filters"][0]["filter-conditions"][0]["value"] = str(new_date_value)
            if meter_option:
                #this is applicable to meter production volumes (i.e. modify table-name to conform to table/view name)
                data_to_modify["rules"][0]["object-locator"]["table-name"] = "VW_METERDAILY"
            modified_data = dumps(OrderedDict(data_to_modify))

            if in_development:
                # 3. log, visually inspect to ensure value has been updated to current date:
                #    only when under development & testing
                print("Modified JSON: ", modified_data)
                print("Original JSON: ", original_data)

                # 4. set output filename for the table mapping file
                saved_name = "{}{}".format(table_mapping_filename, "_1")
            else:
                # i.e. in production stage: save the modified file over existing file
                saved_name = table_mapping_filename

            # 5. finally save updated JSON structure back to file: assumes file is saved in the CWD as the script
            with open(saved_name, 'w') as json_output_file:
                dump(dumps(OrderedDict(data_to_modify)), json_output_file)

            return modified_data
        else:
            log(30, "No json mapping filename is provided !!")
    # End modify_table_mapping_rules() method

    def initiate_task(self, daily_task_arn=None, daily_60day_trans_task_arn=None, 
                      monthly_6month_trans_task_arn=None, meter_daily_task_arn=None, 
                      meter_daily_60day_trans_task_arn=None, meter_monthly_6month_trans_task_arn=None,
                      daily_meter_header_arn=None, daily_well_header_arn=None, daily_connectb_arn=None, 
                      daily_tdm_header_arn=None, table_mapping_filename=None, in_development=True, sleep_time_in_seconds=None):
                          
        client = boto3.client('dms')

        if daily_task_arn:
            # 1. modify table mapping rules (i.e. change value of "filter-operator": "gte",
            #    only for daily 60-day and monthly 6-month transactions, between 3rd and 24th hours)
            time_now = datetime.utcnow()
            hour_now = time_now.hour
            day_now = time_now.day
            modified_table_mapping_rules = None
            modified_table_mapping_rules_60day = None
            modified_table_mapping_rules_60day_meter = None
            modified_table_mapping_rules_6month = None
            modified_table_mapping_rules_6month_meter = None
            
            # occur daily anytime script is invoked -> 60-day transactions: 
            modified_table_mapping_rules_60day = self.modify_table_mapping_rules(table_mapping_filename=table_mapping_filename,
                                                                                 days_backward=60, in_development=in_development, meter_option=False)  
            modified_table_mapping_rules_60day_meter = self.modify_table_mapping_rules(table_mapping_filename=table_mapping_filename,
                                                                                 days_backward=60, in_development=in_development, meter_option=True)
                                                                                 
            if day_now == 1:
                # occurs only monthly (1st day of the month) -> for 6-month (180 days) transactions
                modified_table_mapping_rules_6month = self.modify_table_mapping_rules(table_mapping_filename=table_mapping_filename,
                                                                                      days_backward=180, in_development=in_development, meter_option=False)
                modified_table_mapping_rules_6month_meter = self.modify_table_mapping_rules(table_mapping_filename=table_mapping_filename,
                                                                                      days_backward=180, in_development=in_development, meter_option=True)
            
            # 2. assign updated modified_table_mapping_rules to the current task (only for task(s) with modified rules)
            # sleep/wait for "sleep_time_in_seconds" after each task modification, to allow task modification to stop before re-starting
            if modified_table_mapping_rules_60day and modified_table_mapping_rules_60day_meter:
                print("MODIFIED DATA: ", modified_table_mapping_rules_60day)
                migration_type= "full-load"
                client.modify_replication_task(ReplicationTaskArn=daily_60day_trans_task_arn, MigrationType=migration_type, TableMappings=modified_table_mapping_rules_60day)
                sleep(sleep_time_in_seconds)
                print("{}{}{}".format("Task with ARN of ", daily_60day_trans_task_arn, " is being modified.."))
                client.modify_replication_task(ReplicationTaskArn=meter_daily_60day_trans_task_arn, MigrationType=migration_type, TableMappings=modified_table_mapping_rules_60day_meter)
                sleep(sleep_time_in_seconds)
                print("{}{}{}".format("Task with ARN of ", meter_daily_60day_trans_task_arn, " is being modified.."))
            if modified_table_mapping_rules_6month and modified_table_mapping_rules_6month_meter:
                print("MODIFIED DATA: ",modified_table_mapping_rules_60month)
                migration_type= "full-load"
                client.modify_replication_task(ReplicationTaskArn=monthly_6month_trans_task_arn, MigrationType=migration_type, TableMappings=modified_table_mapping_rules_6month)
                sleep(sleep_time_in_seconds)
                print("{}{}{}".format("Task with ARN of ", monthly_6month_trans_task_arn, " is being modified.."))
                client.modify_replication_task(ReplicationTaskArn=meter_monthly_6month_trans_task_arn, MigrationType=migration_type, TableMappings=modified_table_mapping_rules_6month_meter)
                sleep(sleep_time_in_seconds)
                print("{}{}{}".format("Task with ARN of ", meter_monthly_6month_trans_task_arn, " is being modified.."))
                
            # 3. then, re-start replication task (with or without modified rules)
            # note 1: task_type = "start-replication" or "resume-processing" or "reload-target",
            # note 2: "start-replication" is valid only for tasks running for the first time
            # note 3: "reload-target" is valid when re-starting a task that is not running for the first time
            task_type = "reload-target"
            
            # un-comment this
            # all_replication_task_arns = [daily_task_arn, daily_60day_trans_task_arn, monthly_6month_trans_task_arn, 
            #                              meter_daily_task_arn,  meter_daily_60day_trans_task_arn, meter_monthly_6month_trans_task_arn,
            #                              daily_meter_header_arn, daily_well_header_arn, daily_connectb_arn, daily_tdm_header_arn
            #                             ]
            # un-comment tis
            
            all_replication_task_arns = [daily_60day_trans_task_arn]
            
            #for rep_task_arn in [daily_60day_trans_task_arn]: #all_replication_task_arns:
            for rep_task_arn in all_replication_task_arns:
                client.start_replication_task(ReplicationTaskArn=rep_task_arn, StartReplicationTaskType=task_type)
                print("{}{}{}".format("Task with ARN of ", rep_task_arn, " successfully re-started."))
    # End initiate_task() method
# End  ModifyAndStartDMSTask() class


def main():
    """
    main function: app entry
    """
    
    production_phase = True
    
    # specify (declare & define) relevant arguments
    if not production_phase:
        # this option is applicable when under development & testing on the dash board
        
        # change production_phase value above to False, when using this option
        # 1. general arguments:
        filename = "table_mapping.json"
        bucket_name = "mro-onicatest-dl-dev-us-east-1-bucket1"
        folder_name = "aws-glue-scripts-bucket-1"
        in_development = False
        # 2. specific arguments:
        # a. production volumes that are metered at well-head
        meter_daily_task_arn = "arn:aws:dms:us-east-1:259390597249:task:N7W55MDJ6ZY5FYPXOAEIU4WZE4"                 #"arn:aws:dms:us-east-1:259390597249:task:HSGPSPUBQGDCHF45MC42P5ISLA"
        meter_daily_60day_trans_task_arn = "arn:aws:dms:us-east-1:259390597249:task:JDJM6CK7DRWN2IZDZZFR5VKHPI"     #"arn:aws:dms:us-east-1:259390597249:task:KMRLHIIZ7542A6M2T2ROT56Q7A"
        meter_monthly_6month_trans_task_arn = "arn:aws:dms:us-east-1:259390597249:task:IUMD42EL2LPLCG5LRIK7UOK7SU"  #"arn:aws:dms:us-east-1:259390597249:task:JCBIUOMN65EU7IWH2B7TG6LARI"
        # b. production volumes that are not metered at well-head
        daily_task_arn = "arn:aws:dms:us-east-1:259390597249:task:2BVSV3MCCWCVYIAYDVJD6VS6VI"                #"arn:aws:dms:us-east-1:259390597249:task:ZDBQXDYZ6IH72JNJDN23RK26PQ"
        daily_60day_trans_task_arn = "arn:aws:dms:us-east-1:259390597249:task:UMQ3MQPZHVAROUNKTSZGAF77RM"    #"arn:aws:dms:us-east-1:259390597249:task:R5IT5HHKABQ6F56IB5YGIFOQPY"
        monthly_6month_trans_task_arn = "arn:aws:dms:us-east-1:259390597249:task:2DTIWA6FPWUPYW2UW4GHCAXAUQ" #"arn:aws:dms:us-east-1:259390597249:task:WKJ7JQXEOAV32UOCX5FFJ363B4"
        # c. meterheader, wellheader, connecttb and tdmheader
        daily_meter_header_arn = "arn:aws:dms:us-east-1:259390597249:task:5KYVXCTKIVLHKY3RV5545U44FM"  #"arn:aws:dms:us-east-1:259390597249:task:QEQBVNQWBMPWI64O6O6CEH5JKA"
        daily_well_header_arn = "arn:aws:dms:us-east-1:259390597249:task:H53E2FSHPRCC3G55WHZL44VAEA"   #"arn:aws:dms:us-east-1:259390597249:task:N6U7G3HGLV5IDSDTW6ULQJ3PXI"
        daily_connectb_arn = "arn:aws:dms:us-east-1:259390597249:task:XH2NOBMV76ZXHVKV3LDOLFOLWQ"      #"arn:aws:dms:us-east-1:259390597249:task:6TJ2WJJWZJIJJAHCVNLCIV3IRQ"
        daily_tdm_header_arn = "arn:aws:dms:us-east-1:259390597249:task:WBND3OS4WOORKEZ4OJM7H2E4TU"    #"arn:aws:dms:us-east-1:259390597249:task:P3SDBQ7RFN6IK7IATVHZ2DEY44"
        # d. sleep_time_in_seconds
        sleep_time_in_seconds = 120
    elif production_phase:
        # this option is applicable when in production: -> pass args from sys.argv
       
        # get/read relevant arguments from system arguments
        args = getResolvedOptions(sys.argv, [
            'filename',
            'bucket_name',
            'folder_name',
            'in_development',
            'meter_daily_task_arn',
            'meter_daily_60day_trans_task_arn',
            'meter_monthly_6month_trans_task_arn',
            'daily_task_arn',
            'daily_60day_trans_task_arn',
            'monthly_6month_trans_task_arn',
            'daily_meter_header_arn',
            'daily_well_header_arn',
            'daily_connectb_arn',
            'daily_tdm_header_arn',
            'sleep_time_in_seconds'
        ])
        
        # then, pass the arguments to local variables
        filename = args['filename']
        bucket_name = args['bucket_name']
        folder_name = args['folder_name']
        in_development = args['in_development']
        meter_daily_task_arn = args['meter_daily_task_arn']
        meter_daily_60day_trans_task_arn = args['meter_daily_60day_trans_task_arn']
        meter_monthly_6month_trans_task_arn = args['meter_monthly_6month_trans_task_arn']
        daily_task_arn = args['daily_task_arn']
        daily_60day_trans_task_arn = args['daily_60day_trans_task_arn']
        monthly_6month_trans_task_arn = args['monthly_6month_trans_task_arn']
        daily_meter_header_arn = args['daily_meter_header_arn']
        daily_well_header_arn = args['daily_well_header_arn']
        daily_connectb_arn = args['daily_connectb_arn']
        daily_tdm_header_arn = args['daily_tdm_header_arn']
        sleep_time_in_seconds = int(args['sleep_time_in_seconds'])
    
    # read table_mapping json file into "temp" directory and reference it
    s3_resource = boto3.resource('s3')
    read_from = "{}{}{}".format(folder_name, '/', filename)
    save_to = "{}{}".format('/tmp/', filename)
    s3_resource.meta.client.download_file(bucket_name, read_from, save_to)
    table_mapping_filename = save_to
    
    # create instance of ModifyAndStartDMSTask() and invoke its
    # initiate_task() method to modify and re-start tasks
    modify_start_dms_task = ModifyAndStartDMSTask()
    modify_start_dms_task.initiate_task(daily_task_arn=daily_task_arn,
        daily_60day_trans_task_arn=daily_60day_trans_task_arn,
        monthly_6month_trans_task_arn=monthly_6month_trans_task_arn,
        meter_daily_task_arn=meter_daily_task_arn,
        meter_daily_60day_trans_task_arn=meter_daily_60day_trans_task_arn,
        meter_monthly_6month_trans_task_arn=meter_monthly_6month_trans_task_arn,
        daily_meter_header_arn=daily_meter_header_arn,
        daily_well_header_arn=daily_well_header_arn,
        daily_connectb_arn=daily_connectb_arn,
        daily_tdm_header_arn=daily_tdm_header_arn,
        table_mapping_filename=table_mapping_filename, in_development=in_development,
        sleep_time_in_seconds=sleep_time_in_seconds)
# End main() function


# invoke app
if __name__ in ('__main__', 'app'):
    main()
