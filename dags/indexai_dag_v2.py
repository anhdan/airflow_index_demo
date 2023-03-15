from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.edgemodifier import Label
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import ast
import pendulum
import pandas as pd
import os
import sys
import json
import itertools


default_args = {
    'owner': 'danda',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id="indexai_dag_v02",
    default_args=default_args,
    description="DAG to get, predict and validate stock price and exchange rate",
    start_date=datetime(2023, 3, 15),
    schedule_interval='*/10 * * * *', # every ten minutes
    tags=["INDEXAI"],
    catchup=False,
)
def indexai_dag_v2():

    #========== Task: detect_config_change
    @task.branch(
        task_id="detect_config_change",
    )
    def detect_config_change():
        """
        ====== Task 1: Detect if the config file has been modified, if so, trigger the 
        task to re-parse the config file for validation and prediction timetable
        
        - Input: None
        - Output: Task ID of the possible branches to be triggered
            - parse_config_file: if the config file has been modified
            - skip_parse_config_file: if the config file has not been modified
        - Run on: every run of the DAG
        """

        # check if the copy of the config file exists
        if not os.path.exists( "dags/configs/indexai_config_clone.csv" ):
            return "parse_config_file"
        
        # read the config file and the clone, then compare the two
        config = pd.read_csv( "dags/configs/indexai_config.csv", header=0 )
        clone = pd.read_csv( "dags/configs/indexai_config_clone.csv", header=0 )
        if config.equals( clone ):
            return "skip_parse_config_file"
        else:
            return "parse_config_file"
    

    #========== Task: parse_config_file
    @task(
        task_id="parse_config_file",
        trigger_rule="all_done",
    )
    def parse_config_file():
        """
        ====== Task 2: Parse the config file to get the timetable for validation and
        prediction of the stock price and exchange rate
        
        - Input: None
        - Output: 3 new files in the configs folders:
            - indexai_config_clone.csv: na copy of the config file
            - indexai_validation_timetable.csv: the timetable for validation
            - indexai_prediction_timetable.csv: the timetable for prediction
        - Run on: triggered by the branch task `detect_config_change`
        """

        # copy the config file to the clone
        config = pd.read_csv( "dags/configs/indexai_config.csv", header=0 )
        config.to_csv( "dags/configs/indexai_config_clone.csv", index=False )

        # compute all the possible time to get the stock price and exchange rate
        # and save it to the collection/validation/prediction timetable
        hour_range = range(0, 24)
        minute_range = range(0, 60, 10)
        time_points = [ f'{a}:{str(b).zfill(2)}' for (a,b) in list(itertools.product( hour_range, minute_range ) )]
        time_points_df = pd.DataFrame( time_points, columns=["Time"] )
        time_points_df["CollectedLabels"] = [[] for _ in range(len(time_points_df))]
        time_points_df["ValidatedLabels"] = [[] for _ in range(len(time_points_df))]
        time_points_df["PredictedLabels"] = [[] for _ in range(len(time_points_df))]

        for id, row in config.iterrows():
            # compute the start and end time of the day
            start_time_str = row["DayStart"].split(":")
            start_time = timedelta( hours=int(start_time_str[0]), minutes=int(start_time_str[1]) )
            end_time_str = row["DayEnd"].split(":")
            end_time = timedelta( hours=int(end_time_str[0]), minutes=int(end_time_str[1]) )
            interval_str = row["Interval"].split(":")
            interval = timedelta( hours=int(interval_str[0]), minutes=int(interval_str[1]) )

            # compute time offset to collect data / validate data / predict data
            collect_offset_str = row["CollectionOffset"].split(":")
            collect_offset = timedelta( hours=int(collect_offset_str[0]), minutes=int(collect_offset_str[1]) )
            validate_offset_str = row["ValidationOffset"].split(":")
            validate_offset = timedelta( hours=int(validate_offset_str[0]), minutes=int(validate_offset_str[1]) )
            predict_offset_str = row["PredictionOffset"].split(":")
            predict_offset = timedelta( hours=int(predict_offset_str[0]), minutes=int(predict_offset_str[1]) )

            # compute the time to get the stock price and exchange rate
            time = start_time
            print( '==============> start_time: ', start_time_str )
            print( '==============> end_time: ', end_time_str )
            while time <= end_time:
                print( '----> time: ', time )
                collection_time = time + collect_offset
                collection_time = ':'.join( str(collection_time).split(":")[:2] )
                # find the index of the time in the time points
                try:
                    index = time_points_df[ time_points_df["Time"] == collection_time ].index[0]
                    collected_labels = time_points_df.at[index, "CollectedLabels"]
                    collected_labels.append( (row["Name"], ':'.join( str(time).split(":")[:2] )) )
                    time_points_df.at[index, "CollectedLabels"] = collected_labels
                except:
                    pass

                validation_time = time + validate_offset
                validation_time = ':'.join( str(validation_time).split(":")[:2] )
                # find the index of the time in the time points
                try:
                    index = time_points_df[ time_points_df["Time"] == validation_time ].index[0]
                    validated_labels = time_points_df.at[index, "ValidatedLabels"]
                    validated_labels.append( (row["Name"], ':'.join( str(time).split(":")[:2] )) )
                    time_points_df.at[index, "ValidatedLabels"] = validated_labels
                except:
                    pass

                prediction_time = time + predict_offset
                prediction_time = ':'.join( str(prediction_time).split(":")[:2] )
                next_time = time + interval
                if next_time > end_time:
                    next_time = start_time
                # find the index of the time in the time points
                try:
                    index = time_points_df[ time_points_df["Time"] == prediction_time ].index[0]
                    predicted_labels = time_points_df.at[index, "PredictedLabels"]
                    predicted_labels.append( (row["Name"], ':'.join( str(next_time).split(":")[:2] )) )
                    time_points_df.at[index, "PredictedLabels"] = predicted_labels
                except:
                    pass

                time += interval

                # print( '----> time_points_df: ', time_points_df.head(10) )
            
        # save the timetable to the validation timetable
        time_points_df.to_csv( "dags/configs/indexai_timetable.csv", index=False )


    #========== Task: get_labels_for_collection
    @task(
        task_id="get_labels_for_collection",
    )
    def get_labels_for_collection():
        """
        ====== Task 3: Get the labels for collection from the timetable
        
        - Input: indexai_timetable.csv
        - Output: a list of labels for collection
        - Run on: triggered by the branch task `detect_config_change`
        """

        # get the execution time of this run
        context = get_current_context()
        execution_time = context.get('execution_date').strftime('%H:%M')

        # check if there is any label to collect at this time
        # if there is, return the list of labels
        timetable = pd.read_csv( "dags/configs/indexai_timetable.csv", header=0 )
        try:
            index = timetable[ timetable["Time"] == execution_time ].index[0]
            collected_labels = timetable.at[index, "CollectedLabels"]
            if collected_labels == []:
                # log the message
                with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
                    log_file.write( f"----> Get labels for collection at {context.get('execution_date').strftime('%Y%M%D-%H:%M')} - No label to collect")
                    log_file.close()
                return []
            else:
                # log the message
                with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
                    log_file.write( f"----> Get labels for collection at {context.get('execution_date').strftime('%Y%M%D-%H:%M')} - {collected_labels}" )
                    log_file.close()
                # convert the string to a list
                collected_labels = ast.literal_eval( collected_labels )
                return collected_labels
        except:
            return []

    #========== Task: collect_data
    @task.short_circuit()
    def collect_data( arg: tuple ):
        """
        ====== Task 4: Collect the data for the label
        
        - Input: label name, source url
        - Output: None
        - Run on: triggered by the task `get_labels_for_collection`
        """

        # get the execution time of this run
        context = get_current_context()
        execution_time = context.get('execution_date').strftime('%Y%M%D-%H:%M')

        # get data source url from config file
        config = pd.read_csv( "dags/configs/indexai_config.csv", header=0 )
        try:
            index = config[ config["Name"] == arg[0] ].index[0]
            source_url = config.at[index, "DataSource"]
            # log the execution of this task to the log file
            with open( "dags/logs/indexai_log.txt", "a" ) as f:
                f.write( f"[CollectData] at {execution_time}  - label: {arg[0]}  -  source: {source_url}  -  section: {arg[1]}")
                f.close()
            return True
        except:
            return False
    
    #========== Task: prepare_input_for_validation_model
    @task(
        task_id="prepare_input_for_validation_model",
    )
    def prepare_input_for_validation_model():
        """
        ====== Task 5: Get the labels for validation from the timetable
        and prepare data input for the validation model to validate those labels last prediction
        
        - Input: None
        - Output: a list of labels for validation
        - Run on: triggered by the branch task `detect_config_change`
        """

        # get the execution time of this run
        context = get_current_context()
        execution_time = context.get('execution_date').strftime('%H:%M')

        # check if there is any label to collect at this time
        # if there is, return the list of labels
        timetable = pd.read_csv( "dags/configs/indexai_timetable.csv", header=0 )
        try:
            index = timetable[ timetable["Time"] == execution_time ].index[0]
            validated_labels = timetable.at[index, "ValidatedLabels"]
            if validated_labels == []:
                # log the message
                with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
                    log_file.write( f"----> Prepare data input for validation model at {context.get('execution_date').strftime('%Y%M%D-%H:%M')} - No label data to prepare")
                    log_file.close()
                return []
            else:
                # log the message
                with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
                    log_file.write( f"----> Prepare data input for validation model at {context.get('execution_date').strftime('%Y%M%D-%H:%M')} - {validated_labels}" )
                    log_file.close()
                # convert the string to a list
                validated_labels = ast.literal_eval( validated_labels )
                return validated_labels
        except:
            return []
        

    #========== Task: validate_data
    @task.branch(
        task_id="validate_data",
    )
    def validate_data( arg: list ):
        """
        ====== Task 6: Validate the data for the label

        - Input: list[(label name, datainput)]
        - Output: None
        - Run on: triggered by the task `prepare_input_for_validation_model`
        """

        # get the execution time of this run
        context = get_current_context()
        execution_time = context.get('execution_date').strftime('%Y%M%D-%H:%M')

        # log message to log file that the data is input for validation
        with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
            log_file.write( f"[ValidateData] at {execution_time}")
            log_file.close()
        
        if arg == []:
            return "noti_validation_failed"
        else:
            # log message to log file that the validation is success
            with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
                log_file.write( f"\t- Validation success for labels: {arg}")
                log_file.close()
            return "write_validation_to_db"
    
    #========== Task: write_validation_to_db
    @task(
        task_id="write_validation_to_db",
    )
    def write_validation_to_db():
        # log message to log file that the validation is success and output is written to db
        with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
            log_file.write( f"\t- Validation success and output is written to db")
            log_file.close()
    
    #========== Task: noti_validation_failed
    noti_validation_failed = EmptyOperator(
        task_id="noti_validation_failed",
    )

    #========== Task: prepare_input_for_prediction_model
    @task(
        task_id="prepare_input_for_prediction_model",
    )
    def prepare_input_for_prediction_model():
        """
        ====== Task 7: Get the labels for prediction from the timetable
        and prepare data input for the prediction model to predict currency rate
        of those labels in the next section

        - Input: None
        - Output: a list of labels for prediction
        - Run on: triggered by the branch task `detect_config_change`
        """

        # get the execution time of this run
        context = get_current_context()
        execution_time = context.get('execution_date').strftime('%H:%M')

        # check if there is any label to collect at this time
        # if there is, return the list of labels
        timetable = pd.read_csv( "dags/configs/indexai_timetable.csv", header=0 )
        try:
            index = timetable[ timetable["Time"] == execution_time ].index[0]
            predicted_labels = timetable.at[index, "PredictedLabels"]
            if predicted_labels == []:
                # log the message
                with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
                    log_file.write( f"----> Prepare data input for prediction model at {context.get('execution_date').strftime('%Y%M%D-%H:%M')} - No label data to prepare")
                    log_file.close()
                return []
            else:
                # log the message
                with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
                    log_file.write( f"----> Prepare data input for prediction model at {context.get('execution_date').strftime('%Y%M%D-%H:%M')} - {predicted_labels}" )
                    log_file.close()
                # convert the string to a list
                predicted_labels = ast.literal_eval( predicted_labels )
                return predicted_labels
        except:
            return []
    
    #========== Task: predict_data
    @task.branch(
        task_id="predict_data",
    )
    def predict_data( arg: list ):
        """
        ====== Task 8: Predict the data for the label in the next section

        - Input: list[(label name, datainput)]
        - Output: None
        - Run on: triggered by the task `prepare_input_for_prediction_model`
        """

        # get the execution time of this run
        context = get_current_context()
        execution_time = context.get('execution_date').strftime('%Y%M%D-%H:%M')

        # log message to log file that the data is input for validation
        with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
            log_file.write( f"[PredictData] at {execution_time}")
        
        if arg == []:
            return "noti_prediction_failed"
        else:
            # log message to log file that the prediction is success
            with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
                log_file.write( f"\t- Prediction success for labels: {arg}")
                log_file.close()
            return "write_prediction_to_db"
    
    #========== Task: write_prediction_to_db
    @task(
        task_id="write_prediction_to_db",
    )
    def write_prediction_to_db():
        # log message to log file that the validation is success and output is written to db
        with open( "dags/logs/indexai_log.txt", "a" ) as log_file:
            log_file.write( f"\t- Prediction success and output is written to db")
            log_file.close()
    
    #========== Task: noti_prediction_failed
    noti_prediction_failed = EmptyOperator(
        task_id="noti_prediction_failed",
    )
        
    #
    # ======== Main DAG
    #

    # Detect config file change and parse config file if there is a change
    branch = detect_config_change()
    skip_parse_task = EmptyOperator(
        task_id="skip_parse_config_file",
    )
    parse_task = parse_config_file()
    join_1_task = EmptyOperator(
        task_id="join_1",
        trigger_rule="none_failed_min_one_success",
    )
    branch >> skip_parse_task
    branch >> parse_task
    parse_task >> join_1_task
    skip_parse_task >> join_1_task

    # Get labels for collection at the current time
    get_labels_for_collection_task = get_labels_for_collection()
    join_1_task >> get_labels_for_collection_task
    collect_data_label_tasks = collect_data.expand( arg=get_labels_for_collection_task )
    join_2_task = EmptyOperator(
        task_id="join_2",
        trigger_rule="none_failed_min_one_success",
    )
    collect_data_label_tasks >> join_2_task

    # Prepare data input for validation model and perform validation
    prepare_input_for_validation_model_task = prepare_input_for_validation_model()
    join_1_task >> prepare_input_for_validation_model_task
    validate_data_tasks = validate_data( arg=prepare_input_for_validation_model_task )
    write_validation_to_db_task = write_validation_to_db()
    validate_data_tasks >> write_validation_to_db_task
    validate_data_tasks >> noti_validation_failed
    write_validation_to_db_task >> join_2_task
    noti_validation_failed >> join_2_task

    # Prepare data input for prediction model and perform prediction
    prepare_input_for_prediction_model_task = prepare_input_for_prediction_model()
    join_1_task >> prepare_input_for_prediction_model_task
    predict_data_tasks = predict_data( arg=prepare_input_for_prediction_model_task )
    write_prediction_to_db_task = write_prediction_to_db()
    predict_data_tasks >> write_prediction_to_db_task
    predict_data_tasks >> noti_prediction_failed
    write_prediction_to_db_task >> join_2_task
    noti_prediction_failed >> join_2_task



index_ai_dag = indexai_dag_v2()