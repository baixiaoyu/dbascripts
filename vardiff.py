#!/usr/bin/python
#encoding: utf-8
import random
import getpass
import pymysql


from optparse import OptionParser


class DataBase:
    def __init__(self, dbname=None, dbhost=None,dbuser = None, dbpwd = None, dbcharset = None,dbport = None):

        self._dbname = dbname
        self._dbhost = dbhost
        self._dbuser = dbuser
        self._dbpassword = dbpwd
        self._dbcharset = dbcharset
        self._dbport = int(dbport)
        self._conn = self.connectMySQL()

        if (self._conn):
            self._cursor = self._conn.cursor()

    # 数据库连接
    def connectMySQL(self):
        conn = False
        try:
            conn = pymysql.connect(host=self._dbhost,
                                   user=self._dbuser,
                                   passwd=self._dbpassword,
                                   db=self._dbname,
                                   port=self._dbport,
                                   cursorclass=pymysql.cursors.DictCursor,
                                   charset=self._dbcharset,
                                   )
        except Exception, data:
            print("connect database failed, %s" % data)
            conn = False
        return conn

    # 获取查询结果集
    def fetch_all(self, sql):
        res = ''
        if (self._conn):
            try:
                self._cursor.execute(sql)
                res = self._cursor.fetchall()
            except Exception, data:
                res = False
                print("query database exception, %s" % data)
        return res

    def fethch_one(self, sql):
        res = ''
        if (self._conn):
            try:
                self._cursor.execute(sql)
                res = self._cursor.fetchone()
            except Exception, data:
                res = False
                print("query database exception, %s" % data)
        return res

    def update(self, sql):
        flag = False
        if (self._conn):
            try:
                self._cursor.execute(sql)
                self._conn.commit()
                flag = True
            except Exception, data:
                flag = False
                print("update database exception, %s" % data)

        return flag

    # 关闭数据库连接
    def close(self):
        if (self._conn):
            try:
                if (type(self._cursor) == 'object'):
                    self._cursor.close()
                if (type(self._conn) == 'object'):
                    self._conn.close()
            except Exception, data:
                print("close database exception, %s,%s,%s" % (data, type(self._cursor), type(self._conn)))



dynamic_variables = ["audit_log_connection_policy","audit_log_exclude_accounts","audit_log_flush","audit_log_format_unix_timestamp","audit_log_include_accounts","audit_log_read_buffer_size","audit_log_rotate_on_size","audit_log_statement_policy","authentication_ldap_sasl_auth_method_name","authentication_ldap_sasl_bind_base_dn","authentication_ldap_sasl_bind_root_dn","authentication_ldap_sasl_bind_root_pwd","authentication_ldap_sasl_ca_path","authentication_ldap_sasl_group_search_attr","authentication_ldap_sasl_group_search_filter","authentication_ldap_sasl_init_pool_size","authentication_ldap_sasl_log_status","authentication_ldap_sasl_max_pool_size","authentication_ldap_sasl_server_host","authentication_ldap_sasl_server_port","authentication_ldap_sasl_tls","authentication_ldap_sasl_user_search_attr","authentication_ldap_simple_auth_method_name","authentication_ldap_simple_bind_base_dn","authentication_ldap_simple_bind_root_dn","authentication_ldap_simple_bind_root_pwd","authentication_ldap_simple_ca_path","authentication_ldap_simple_group_search_attr","authentication_ldap_simple_group_search_filter","authentication_ldap_simple_init_pool_size","authentication_ldap_simple_log_status","authentication_ldap_simple_max_pool_size","authentication_ldap_simple_server_host","authentication_ldap_simple_server_port","authentication_ldap_simple_tls","authentication_ldap_simple_user_search_attr","auto_increment_increment","auto_increment_offset","autocommit","automatic_sp_privileges","avoid_temporal_upgrade","big_tables","binlog_cache_size","binlog_checksum","binlog_direct_non_transactional_updates","binlog_error_action","binlog_format","binlog_group_commit_sync_delay","binlog_group_commit_sync_no_delay_count","binlog_max_flush_queue_time","binlog_order_commits","binlog_row_image","binlog_rows_query_log_events","binlog_stmt_cache_size","binlog_transaction_dependency_history_size","binlog_transaction_dependency_tracking","block_encryption_mode","bulk_insert_buffer_size","character_set_client","character_set_connection","character_set_database","character_set_filesystem","character_set_results","character_set_server","check_proxy_users","collation_connection","collation_database","collation_server","completion_type","concurrent_insert","connect_timeout","connection_control_failed_connections_threshold","connection_control_max_connection_delay","connection_control_min_connection_delay","debug","debug_sync","default_password_lifetime","default_storage_engine","default_tmp_storage_engine","default_week_format","delay_key_write","delayed_insert_limit","delayed_insert_timeout","delayed_queue_size","div_precision_increment","end_markers_in_json","enforce_gtid_consistency","eq_range_index_dive_limit","event_scheduler","expire_logs_days","explicit_defaults_for_timestamp","flush","flush_time","foreign_key_checks","ft_boolean_syntax","general_log","general_log_file","group_concat_max_len","group_replication_allow_local_disjoint_gtids_join","group_replication_allow_local_lower_version_join","group_replication_auto_increment_increment","group_replication_bootstrap_group","group_replication_components_stop_timeout","group_replication_compression_threshold","group_replication_enforce_update_everywhere_checks","group_replication_exit_state_action","group_replication_flow_control_applier_threshold","group_replication_flow_control_certifier_threshold","group_replication_flow_control_mode","group_replication_force_members","group_replication_group_name","group_replication_group_seeds","group_replication_gtid_assignment_block_size","group_replication_ip_whitelist","group_replication_local_address","group_replication_member_weight","group_replication_poll_spin_loops","group_replication_recovery_complete_at","group_replication_recovery_reconnect_interval","group_replication_recovery_retry_count","group_replication_recovery_ssl_ca","group_replication_recovery_ssl_capath","group_replication_recovery_ssl_cert","group_replication_recovery_ssl_cipher","group_replication_recovery_ssl_crl","group_replication_recovery_ssl_crlpath","group_replication_recovery_ssl_key","group_replication_recovery_ssl_verify_server_cert","group_replication_recovery_use_ssl","group_replication_single_primary_mode","group_replication_ssl_mode","group_replication_start_on_boot","group_replication_transaction_size_limit","group_replication_unreachable_majority_timeout","gtid_executed_compression_period","gtid_mode","gtid_next","gtid_purged","host_cache_size","identity","init_connect","init_slave","innodb_adaptive_flushing","innodb_adaptive_flushing_lwm","innodb_adaptive_hash_index","innodb_adaptive_max_sleep_delay","innodb_api_bk_commit_interval","innodb_api_trx_level","innodb_autoextend_increment","innodb_background_drop_list_empty","innodb_buffer_pool_dump_at_shutdown","innodb_buffer_pool_dump_now","innodb_buffer_pool_dump_pct","innodb_buffer_pool_filename","innodb_buffer_pool_load_abort","innodb_buffer_pool_load_now","innodb_buffer_pool_size","innodb_change_buffer_max_size","innodb_change_buffering","innodb_change_buffering_debug","innodb_checksum_algorithm","innodb_cmp_per_index_enabled","innodb_commit_concurrency","innodb_compress_debug","innodb_compression_failure_threshold_pct","innodb_compression_level","innodb_compression_pad_pct_max","innodb_concurrency_tickets","innodb_deadlock_detect","innodb_default_row_format","innodb_disable_resize_buffer_pool_debug","innodb_disable_sort_file_cache","innodb_fast_shutdown","innodb_fil_make_page_dirty_debug","innodb_file_format","innodb_file_format_max","innodb_file_per_table","innodb_fill_factor","innodb_flush_log_at_timeout","innodb_flush_log_at_trx_commit","innodb_flush_neighbors","innodb_flush_sync","innodb_flushing_avg_loops","innodb_ft_aux_table","innodb_ft_enable_diag_print","innodb_ft_enable_stopword","innodb_ft_num_word_optimize","innodb_ft_result_cache_limit","innodb_ft_server_stopword_table","innodb_ft_user_stopword_table","innodb_io_capacity","innodb_io_capacity_max","innodb_large_prefix","innodb_limit_optimistic_insert_debug","innodb_lock_wait_timeout","innodb_log_checkpoint_now","innodb_log_checksums","innodb_log_compressed_pages","innodb_log_write_ahead_size","innodb_lru_scan_depth","innodb_max_dirty_pages_pct","innodb_max_dirty_pages_pct_lwm","innodb_max_purge_lag","innodb_max_purge_lag_delay","innodb_max_undo_log_size","innodb_merge_threshold_set_all_debug","innodb_monitor_disable","innodb_monitor_enable","innodb_monitor_reset","innodb_monitor_reset_all","innodb_old_blocks_pct","innodb_old_blocks_time","innodb_online_alter_log_max_size","innodb_optimize_fulltext_only","innodb_print_all_deadlocks","innodb_purge_batch_size","innodb_purge_rseg_truncate_frequency","innodb_random_read_ahead","innodb_read_ahead_threshold","innodb_replication_delay","innodb_rollback_segments","innodb_saved_page_number_debug","innodb_spin_wait_delay","innodb_stats_auto_recalc","innodb_stats_include_delete_marked","innodb_stats_method","innodb_stats_on_metadata","innodb_stats_persistent","innodb_stats_persistent_sample_pages","innodb_stats_sample_pages","innodb_stats_transient_sample_pages","innodb_status_output","innodb_status_output_locks","innodb_strict_mode","innodb_support_xa","innodb_sync_spin_loops","innodb_table_locks","innodb_thread_concurrency","innodb_thread_sleep_delay","innodb_tmpdir","innodb_trx_purge_view_update_only_debug","innodb_trx_rseg_n_slots_debug","innodb_undo_log_truncate","innodb_undo_logs","insert_id","interactive_timeout","internal_tmp_disk_storage_engine","join_buffer_size","keep_files_on_create","key_buffer_size","key_cache_age_threshold","key_cache_block_size","key_cache_division_limit","keyring_aws_cmk_id","keyring_aws_region","keyring_encrypted_file_data","keyring_encrypted_file_password","keyring_file_data","keyring_okv_conf_dir","keyring_operations","last_insert_id","lc_messages","lc_time_names","local_infile","lock_wait_timeout","log_bin_trust_function_creators","log_bin_use_v1_row_events","log_builtin_as_identified_by_password","log_error_verbosity","log_output","log_queries_not_using_indexes","log_slow_admin_statements","log_slow_slave_statements","log_statements_unsafe_for_binlog","log_syslog","log_syslog_facility","log_syslog_include_pid","log_syslog_tag","log_throttle_queries_not_using_indexes","log_timestamps","log_warnings","long_query_time","low_priority_updates","master_info_repository","master_verify_checksum","max_allowed_packet","max_binlog_cache_size","max_binlog_size","max_binlog_stmt_cache_size","max_connect_errors","max_connections","max_delayed_threads","max_error_count","max_execution_time","max_heap_table_size","max_insert_delayed_threads","max_join_size","max_length_for_sort_data","max_points_in_geometry","max_prepared_stmt_count","max_relay_log_size","max_seeks_for_key","max_sort_length","max_sp_recursion_depth","max_tmp_tables","max_user_connections","max_write_lock_count","min_examined_row_limit","multi_range_count","myisam_data_pointer_size","myisam_max_sort_file_size","myisam_repair_threads","myisam_sort_buffer_size","myisam_stats_method","myisam_use_mmap","mysql_firewall_mode","mysql_firewall_trace","mysql_native_password_proxy_users","mysqlx_connect_timeout","mysqlx_idle_worker_thread_timeout","mysqlx_max_allowed_packet","mysqlx_max_connections","mysqlx_min_worker_threads","ndb_allow_copying_alter_table","ndb_autoincrement_prefetch_sz","ndb_blob_read_batch_bytes","ndb_blob_write_batch_bytes","ndb_cache_check_time","ndb_clear_apply_status","ndb_data_node_neighbour","ndb_default_column_format","ndb_default_column_format","ndb_deferred_constraints","ndb_deferred_constraints","ndb_distribution","ndb_distribution","ndb_eventbuffer_free_percent","ndb_eventbuffer_max_alloc","ndb_extra_logging","ndb_force_send","ndb_fully_replicated","ndb_index_stat_enable","ndb_index_stat_option","ndb_join_pushdown","ndb_log_binlog_index","ndb_log_empty_epochs","ndb_log_empty_epochs","ndb_log_empty_update","ndb_log_empty_update","ndb_log_exclusive_reads","ndb_log_exclusive_reads","ndb_log_update_as_write","ndb_log_update_minimal","ndb_log_updated_only","ndb_optimization_delay","ndb_read_backup","ndb_recv_thread_activation_threshold","ndb_recv_thread_cpu_mask","ndb_report_thresh_binlog_epoch_slip","ndb_report_thresh_binlog_mem_usage","ndb_row_checksum","ndb_show_foreign_key_mock_tables","ndb_slave_conflict_role","ndb_table_no_logging","ndb_table_temporary","ndb_use_exact_count","ndb_use_transactions","ndbinfo_max_bytes","ndbinfo_max_rows","ndbinfo_offline","ndbinfo_show_hidden","net_buffer_length","net_read_timeout","net_retry_count","net_write_timeout","new","offline_mode","old_alter_table","old_passwords","optimizer_prune_level","optimizer_search_depth","optimizer_switch","optimizer_trace","optimizer_trace_features","optimizer_trace_limit","optimizer_trace_max_mem_size","optimizer_trace_offset","parser_max_mem_size","preload_buffer_size","profiling","profiling_history_size","pseudo_slave_mode","pseudo_thread_id","query_alloc_block_size","query_cache_limit","query_cache_min_res_unit","query_cache_size","query_cache_type","query_cache_wlock_invalidate","query_prealloc_size","rand_seed1","rand_seed2","range_alloc_block_size","range_optimizer_max_mem_size","rbr_exec_mode","read_buffer_size","read_only","read_rnd_buffer_size","relay_log_info_repository","relay_log_purge","replication_optimize_for_static_plugin_config","replication_sender_observe_commit_only","require_secure_transport","rewriter_enabled","rewriter_verbose","rpl_semi_sync_master_enabled","rpl_semi_sync_master_timeout","rpl_semi_sync_master_trace_level","rpl_semi_sync_master_wait_for_slave_count","rpl_semi_sync_master_wait_no_slave","rpl_semi_sync_master_wait_point","rpl_semi_sync_slave_enabled","rpl_semi_sync_slave_trace_level","rpl_stop_slave_timeout","secure_auth","server_id","session_track_gtids","session_track_schema","session_track_state_change","session_track_system_variables","session_track_transaction_info","sha256_password_proxy_users","show_compatibility_56","show_create_table_verbosity","show_old_temporals","slave_allow_batching","slave_checkpoint_group","slave_checkpoint_period","slave_compressed_protocol","slave_exec_mode","slave_max_allowed_packet","slave_net_timeout","slave_parallel_type","slave_parallel_workers","slave_pending_jobs_size_max","slave_preserve_commit_order","slave_rows_search_algorithms","slave_sql_verify_checksum","slave_transaction_retries","slave_type_conversions","slow_launch_time","slow_query_log","slow_query_log_file","sort_buffer_size","sql_auto_is_null","sql_big_selects","sql_buffer_result","sql_log_bin","sql_log_off","sql_mode","sql_notes","sql_quote_show_create","sql_safe_updates","sql_select_limit","sql_slave_skip_counter","sql_warnings","stored_program_cache","super_read_only","sync_binlog","sync_frm","sync_master_info","sync_relay_log","sync_relay_log_info","table_definition_cache","table_open_cache","thread_cache_size","thread_pool_high_priority_connection","thread_pool_max_unused_threads","thread_pool_prio_kickup_timer","thread_pool_stall_limit","time_zone","timestamp","tmp_table_size","transaction_alloc_block_size","transaction_allow_batching","transaction_isolation","transaction_prealloc_size","transaction_read_only","transaction_write_set_extraction","tx_isolation","tx_read_only","unique_checks","updatable_views_with_limit","validate_password_check_user_name","validate_password_dictionary_file","validate_password_length","validate_password_mixed_case_count","validate_password_number_count","validate_password_policy","validate_password_special_char_count","version_tokens_session","wait_timeout"]
def comparelocalvariables(src,port,user,password):
    db = DataBase( dbhost = src, dbuser=user, dbpwd=password, dbport=port)
    instance_variables = db.fetch_all("show variables")
    db.close()

    instance_variables_map = {}
    for instance_variable in instance_variables:
        key = instance_variable["Variable_name"]
        value = instance_variable["Value"]
        instance_variables_map[key] = value

    file_variables_list = []
    with open('/Users/xiaoyu.bai/sandboxes/rsandbox_percona-server-5_7_21/master/my.sandbox.cnf','r') as f:
        file_variables = f.readline()
        while file_variables:
            # print file_variables
            if len(file_variables) != 0 and file_variables.startswith("#") == False and file_variables != '\n' and file_variables.startswith("[") ==False:

                file_variables_list.append(file_variables)
            file_variables = f.readline()



    for file_variable in file_variables_list:
        var_kv =  file_variable.split("=")

        variable_name, variables_value = var_kv[0].strip(),var_kv[1].strip()

        if variable_name.find("-") > 0:
            variable_name = variable_name.replace("-", "_")

        if variable_name in dynamic_variables:
            if instance_variables_map.has_key(variable_name):
                instance_value = instance_variables_map[variable_name]

                if instance_value.upper() != variables_value.upper():
                    if (instance_value.upper() == "ON" or instance_value.upper()=="TRUE") and (variables_value.upper()=="ON" or variables_value.upper()=="TRUE"):
                        continue
                    if (instance_value.upper() == "OFF" or instance_value.upper()=="FALSE") and (variables_value.upper()=="OFF" or variables_value.upper()=="FALSE"):
                        continue
                    print(variable_name, " is not same between disk and instance.")
                    print(instance_value, variables_value)


def comparedifferentinstance(src,port,user,password,dest):
    srcdb = DataBase(dbhost=src, dbuser=user, dbpwd=password, dbport=port)
    destdb = DataBase(dbhost=src, dbuser=user, dbpwd=password, dbport=port)
    src_variables = srcdb.fetch_all("show variables")
    dest_variables = destdb.fetch_all("show variables")
    srcdb.close()
    destdb.close()
    src_variables_map = {}
    dest_variables_map = {}
    for instance_variable in src_variables:
        key = instance_variable["Variable_name"]
        value = instance_variable["Value"]
        src_variables_map[key] = value

    for instance_variable in dest_variables:
        key = instance_variable["Variable_name"]
        value = instance_variable["Value"]
        dest_variables_map[key] = value

    for key,value in src_variables_map:
        if key in dynamic_variables:
            if value !=  dest_variables_map[key]:
                print(key, " is not same between src db and dest db.")
                print(key, dest_variables_map[key])

if __name__ == "__main__":
    usage = "vardiff.py -s 10.92.1.1 -P 3301 -d 10.92.1.2 --check different variables" \
            "vardiff.py -s 10.92.1.1 -P 3301  --check local variables"

    parser = OptionParser(usage)
    parser.add_option("-s", "--src", dest="src", help="src ip", type="string")
    parser.add_option("-P", "--port", dest="port", help="mysql port", type="string")
    parser.add_option("-d", "--dest", dest="dest", help="dest ip", default= "",type="string")
    parser.add_option("-u", "--user", dest="user", help="username", type="string")
    parser.add_option("-p", "--password", dest="password", help="password", type="string")

    (options, args) = parser.parse_args()
    src = options.src
    port = options.port
    dest = options.dest
    user = options.user
    password = options.password

    try:
        if dest == "":
            comparelocalvariables(src,port,user,password)
        else:
            comparedifferentinstance(src,port,user,password,dest)
        # conn = pymysql.connect(user = "root", passwd = p, host = ip, port=port,db = "mysql")
        # conn2 = pymysql.connect(user="xx", passwd="xxx", host="127.0.0.1", port=3306, db="mysql")
        # cur = conn.cursor()
        # cur2 = conn2.cursor()
        # createdb(conn,dbname,ip,port,conn2,net,cur,cur2,charset,collateset)
    except Exception, data:
        print("failed, %s" % data)
    # finally:
    #     conn.close()
    #     conn2.close()

