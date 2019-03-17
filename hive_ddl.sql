------											
------Source Table with is being loaded using sqoop
------
DROP TABLE IF EXISTS nikhilvemula.t_care_ca_rel_src;
CREATE EXTERNAL TABLE IF NOT EXISTS nikhilvemula.t_care_ca_rel_src (
prtn_id string,
cus_id string,
fbsi_firm_c string,
fbsi_brch_c string,
fbsi_base_c string,
fid_acc_id string,
rel_ty_c string,
seq_n  string,
prnc_nm_c string,
multi_co_n string,
cre_tmst string,
cre_user string,
upd_tmst string,
upd_usr  string,
email_ad_of_rec_c string,
prm_cus_c string,
ibmsnap_intentseq string,
ibmsnap_commitseq string,
ibmsnap_operation string,
ibmsnap_logmarker string,
rel_to_tre_c string,
tst_cont_opt_out_c string,
active_ind string,
run_d string,
hdfs_insert_tmst string,
hdfs_update_tmst string,
hdfs_update_user string )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nikhilvemula.db/t_care_ca_rel_src";


---Will contain pre day records for change data capture.
DROP TABLE IF EXISTS nikhilvemula.t_care_ca_rel_prev;
CREATE EXTERNAL TABLE IF NOT EXISTS nikhilvemula.t_care_ca_rel_prev (
prtn_id string,
cus_id string,
fbsi_firm_c string,
fbsi_brch_c string,
fbsi_base_c string,
fid_acc_id string,
rel_ty_c string,
seq_n  string,
prnc_nm_c string,
multi_co_n string,
cre_tmst string,
cre_user string,
upd_tmst string,
upd_usr  string,
email_ad_of_rec_c string,
prm_cus_c string,
ibmsnap_intentseq string,
ibmsnap_commitseq string,
ibmsnap_operation string,
ibmsnap_logmarker string,
rel_to_tre_c string,
tst_cont_opt_out_c string,
active_ind string,
run_d string,
hdfs_insert_tmst string,
hdfs_update_tmst string,
hdfs_update_user string,
eff_strt_dt string,
eff_end_dt string )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nikhilvemula.db/t_care_ca_rel_prev";

----Main Table with is the with contains the CDC records
DROP TABLE IF EXISTS nikhilvemula.t_care_ca_rel_main;
CREATE EXTERNAL TABLE IF NOT EXISTS nikhilvemula.t_care_ca_rel_main (
prtn_id string,
cus_id string,
fbsi_firm_c string,
fbsi_brch_c string,
fbsi_base_c string,
fid_acc_id string,
rel_ty_c string,
seq_n  string,
prnc_nm_c string,
multi_co_n string,
cre_tmst string,
cre_user string,
upd_tmst string,
upd_usr  string,
email_ad_of_rec_c string,
prm_cus_c string,
ibmsnap_intentseq string,
ibmsnap_commitseq string,
ibmsnap_operation string,
ibmsnap_logmarker string,
rel_to_tre_c string,
tst_cont_opt_out_c string,
active_ind string,
run_d string,
hdfs_insert_tmst string,
hdfs_update_tmst string,
hdfs_update_user string,
eff_strt_dt string,
eff_end_dt string )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location "hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nikhilvemula.db/t_care_ca_rel_main";