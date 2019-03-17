from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession \
    .builder \
    .appName("SCD Type 2") \
    .getOrCreate()


#read the tables into df
df_main_tbl=spark.sql("""select * from nikhilvemula.t_care_ca_rel_main""")
df_prev_tbl=spark.sql("""select * from nikhilvemula.t_care_ca_rel_prev""")
df_src_tbl=spark.sql("""select * from nikhilvemula.t_care_ca_rel_src""")

#registered temporary for use
df_src_tbl.createOrReplaceTempView("temp_tbl_src")
df_prev_tbl.createOrReplaceTempView("temp_tbl_prev")

eff_strt_dt="2019-03-02"
eff_end_dt="9999-12-30"
curr_dt="2019-03-02"

df_new_records=spark.sql("""
select
src.prtn_id
,src.cus_id
,src.fbsi_firm_c
,src.fbsi_brch_c
,src.fbsi_base_c
,src.fid_acc_id
,src.rel_ty_c
,src.seq_n 
,src.prnc_nm_c
,src.multi_co_n
,src.cre_tmst
,src.cre_user
,src.upd_tmst
,src.upd_usr 
,src.email_ad_of_rec_c
,src.prm_cus_c
,src.ibmsnap_intentseq
,src.ibmsnap_commitseq
,src.ibmsnap_operation
,src.ibmsnap_logmarker
,src.rel_to_tre_c
,src.tst_cont_opt_out_c
,src.active_ind
,src.run_d
,src.hdfs_insert_tmst
,src.hdfs_update_tmst
,src.hdfs_update_user
 from temp_tbl_prev prev
full outer join
temp_tbl_src src
on
src.prtn_id = prev.prtn_id
and src.cus_id = prev.cus_id
and src.fbsi_firm_c = prev.fbsi_firm_c
and src.fbsi_brch_c = prev.fbsi_brch_c
and src.fbsi_base_c = prev.fbsi_base_c
and src.fid_acc_id  = prev.fid_acc_id
and src.rel_ty_c = prev.rel_ty_c
and src.multi_co_n = prev.multi_co_n
where
prev.prtn_id is null
and prev.cus_id is null
and prev.fbsi_firm_c is null
and prev.fbsi_brch_c is null
and prev.fbsi_base_c is null
and prev.fid_acc_id  is null
and prev.rel_ty_c is null
and prev.multi_co_n is null
and prev.prtn_id is null
--
and src.cus_id is not null
and src.fbsi_firm_c is not null
and src.fbsi_brch_c is not null
and src.fbsi_base_c is not null
and src.fid_acc_id  is not null
and src.rel_ty_c is not null
and src.multi_co_n is not null
""")

#add eff_strt_dt and eff_end_dt wihich is high end date.
df_new_records=df_new_records.withColumn("eff_strt_dt", lit(eff_strt_dt)).withColumn("eff_end_dt", lit(eff_end_dt))

# create temp table only with new records. 
df_new_records.createOrReplaceTempView("temp_new_records")

#spark.sql("""insert into table nikhilvemula.t_care_ca_rel_main select * from temp_new_records """)



df_update_records=spark.sql("""
select
src.prtn_id
,src.cus_id
,src.fbsi_firm_c
,src.fbsi_brch_c
,src.fbsi_base_c
,src.fid_acc_id
,src.rel_ty_c
,src.seq_n 
,src.prnc_nm_c
,src.multi_co_n
,src.cre_tmst
,src.cre_user
,src.upd_tmst
,src.upd_usr 
,src.email_ad_of_rec_c
,src.prm_cus_c
,src.ibmsnap_intentseq
,src.ibmsnap_commitseq
,src.ibmsnap_operation
,src.ibmsnap_logmarker
,src.rel_to_tre_c
,src.tst_cont_opt_out_c
,src.active_ind
,src.run_d
,src.hdfs_insert_tmst
,src.hdfs_update_tmst
,src.hdfs_update_user
 from temp_tbl_prev prev
full outer join
temp_tbl_src src
on
src.prtn_id = prev.prtn_id
and src.cus_id = prev.cus_id
and src.fbsi_firm_c = prev.fbsi_firm_c
and src.fbsi_brch_c = prev.fbsi_brch_c
and src.fbsi_base_c = prev.fbsi_base_c
and src.fid_acc_id  = prev.fid_acc_id
and src.rel_ty_c = prev.rel_ty_c
and src.multi_co_n = prev.multi_co_n
where
prev.prtn_id is not null
and prev.cus_id is not null
and prev.fbsi_firm_c is not null
and prev.fbsi_brch_c is not null
and prev.fbsi_base_c is not null
and prev.fid_acc_id  is not null
and prev.rel_ty_c is not null
and prev.multi_co_n is not null
and prev.prtn_id is null
--
and src.cus_id is not null
and src.fbsi_firm_c is not null
and src.fbsi_brch_c is not null
and src.fbsi_base_c is not null
and src.fid_acc_id  is not null
and src.rel_ty_c is not null
and src.multi_co_n is not null
---
and src.seq_n  <> prev.seq_n 
or src.prnc_nm_c <> prev.prnc_nm_c
or src.cre_tmst <> prev.cre_tmst
or src.cre_user <> prev.cre_user
or src.upd_tmst <> prev.upd_tmst
or src.upd_usr  <> prev.upd_usr 
or src.email_ad_of_rec_c <> prev.email_ad_of_rec_c
or src.prm_cus_c <> prev.prm_cus_c
or src.ibmsnap_intentseq <> prev.ibmsnap_intentseq
or src.ibmsnap_commitseq <> prev.ibmsnap_commitseq
or src.ibmsnap_operation <> prev.ibmsnap_operation
or src.ibmsnap_logmarker <> prev.ibmsnap_logmarker
or src.rel_to_tre_c <> prev.rel_to_tre_c
or src.tst_cont_opt_out_c <> prev.tst_cont_opt_out_c
or src.active_ind <> prev.active_ind
or src.run_d <> prev.run_d
or src.hdfs_insert_tmst <> prev.hdfs_insert_tmst
or src.hdfs_update_tmst <> prev.hdfs_update_tmst
or src.hdfs_update_user <> prev.hdfs_update_user
""")

#add eff_strt_dt and eff_end_dt wihich is high end date.
df_update_records=df_update_records.withColumn("eff_strt_dt", lit(eff_strt_dt)).withColumn("eff_end_dt", lit(eff_end_dt))

# create temp table only with update records. 
df_update_records.createOrReplaceTempView("temp_update_records")


df_update_exstng_records=spark.sql("""
select
prev.prtn_id
,prev.cus_id
,prev.fbsi_firm_c
,prev.fbsi_brch_c
,prev.fbsi_base_c
,prev.fid_acc_id
,prev.rel_ty_c
,prev.seq_n 
,prev.prnc_nm_c
,prev.multi_co_n
,prev.cre_tmst
,prev.cre_user
,prev.upd_tmst
,prev.upd_usr 
,prev.email_ad_of_rec_c
,prev.prm_cus_c
,prev.ibmsnap_intentseq
,prev.ibmsnap_commitseq
,prev.ibmsnap_operation
,prev.ibmsnap_logmarker
,prev.rel_to_tre_c
,prev.tst_cont_opt_out_c
,prev.active_ind
,prev.run_d
,prev.hdfs_insert_tmst
,prev.hdfs_update_tmst
,prev.hdfs_update_user
,prev.eff_strt_dt
 from temp_tbl_prev prev
inner  join
temp_tbl_src src
on
src.prtn_id = prev.prtn_id
and src.cus_id = prev.cus_id
and src.fbsi_firm_c = prev.fbsi_firm_c
and src.fbsi_brch_c = prev.fbsi_brch_c
and src.fbsi_base_c = prev.fbsi_base_c
and src.fid_acc_id  = prev.fid_acc_id
and src.rel_ty_c = prev.rel_ty_c
and src.multi_co_n = prev.multi_co_n
where
prev.prtn_id is not null
and prev.cus_id is not null
and prev.fbsi_firm_c is not null
and prev.fbsi_brch_c is not null
and prev.fbsi_base_c is not null
and prev.fid_acc_id  is not null
and prev.rel_ty_c is not null
and prev.multi_co_n is not null
and prev.prtn_id is null
--
and src.cus_id is not null
and src.fbsi_firm_c is not null
and src.fbsi_brch_c is not null
and src.fbsi_base_c is not null
and src.fid_acc_id  is not null
and src.rel_ty_c is not null
and src.multi_co_n is not null
---
and src.seq_n  <> prev.seq_n 
or src.prnc_nm_c <> prev.prnc_nm_c
or src.cre_tmst <> prev.cre_tmst
or src.cre_user <> prev.cre_user
or src.upd_tmst <> prev.upd_tmst
or src.upd_usr  <> prev.upd_usr 
or src.email_ad_of_rec_c <> prev.email_ad_of_rec_c
or src.prm_cus_c <> prev.prm_cus_c
or src.ibmsnap_intentseq <> prev.ibmsnap_intentseq
or src.ibmsnap_commitseq <> prev.ibmsnap_commitseq
or src.ibmsnap_operation <> prev.ibmsnap_operation
or src.ibmsnap_logmarker <> prev.ibmsnap_logmarker
or src.rel_to_tre_c <> prev.rel_to_tre_c
or src.tst_cont_opt_out_c <> prev.tst_cont_opt_out_c
or src.active_ind <> prev.active_ind
or src.run_d <> prev.run_d
or src.hdfs_insert_tmst <> prev.hdfs_insert_tmst
or src.hdfs_update_tmst <> prev.hdfs_update_tmst
or src.hdfs_update_user <> prev.hdfs_update_user
and 
prev.eff_end_dt='9999-12-30'
""")

#close the existing high end dates
df_update_exstng_records=df_update_exstng_records.withColumn("eff_end_dt", lit(curr_dt))



# find the deletes.
df_del_records=spark.sql("""
select
prev.prtn_id
,prev.cus_id
,prev.fbsi_firm_c
,prev.fbsi_brch_c
,prev.fbsi_base_c
,prev.fid_acc_id
,prev.rel_ty_c
,prev.seq_n 
,prev.prnc_nm_c
,prev.multi_co_n
,prev.cre_tmst
,prev.cre_user
,prev.upd_tmst
,prev.upd_usr 
,prev.email_ad_of_rec_c
,prev.prm_cus_c
,prev.ibmsnap_intentseq
,prev.ibmsnap_commitseq
,prev.ibmsnap_operation
,prev.ibmsnap_logmarker
,prev.rel_to_tre_c
,prev.tst_cont_opt_out_c
,prev.active_ind
,prev.run_d
,prev.hdfs_insert_tmst
,prev.hdfs_update_tmst
,prev.hdfs_update_user
,prev.eff_strt_dt
 from temp_tbl_prev prev
left outer  join
temp_tbl_src src
on
src.prtn_id = prev.prtn_id
and src.cus_id = prev.cus_id
and src.fbsi_firm_c = prev.fbsi_firm_c
and src.fbsi_brch_c = prev.fbsi_brch_c
and src.fbsi_base_c = prev.fbsi_base_c
and src.fid_acc_id  = prev.fid_acc_id
and src.rel_ty_c = prev.rel_ty_c
and src.multi_co_n = prev.multi_co_n
where
src.cus_id is  null
and src.fbsi_firm_c is  null
and src.fbsi_brch_c is  null
and src.fbsi_base_c is  null
and src.fid_acc_id  is  null
and src.rel_ty_c is  null
and src.multi_co_n is  null
-- 
and prev.eff_end_dt="9999-12-30"
""")

#soft delete the rows by closing the existing high end dates
df_del_records=df_del_records.withColumn("eff_end_dt", lit(curr_dt))



# create temp table only with update records. 
df_del_records.createOrReplaceTempView("df_del_records")

df_temp_final=df_new_records.unionAll(df_update_records).unionAll(df_update_exstng_records).unionAll(df_del_records)
df_temp_final.createOrReplaceTempView("temp_upd_del_new_records")


df_unchanged_tmp_records=spark.sql("""
select
prev.prtn_id
,prev.cus_id
,prev.fbsi_firm_c
,prev.fbsi_brch_c
,prev.fbsi_base_c
,prev.fid_acc_id
,prev.rel_ty_c
,prev.seq_n 
,prev.prnc_nm_c
,prev.multi_co_n
,prev.cre_tmst
,prev.cre_user
,prev.upd_tmst
,prev.upd_usr 
,prev.email_ad_of_rec_c
,prev.prm_cus_c
,prev.ibmsnap_intentseq
,prev.ibmsnap_commitseq
,prev.ibmsnap_operation
,prev.ibmsnap_logmarker
,prev.rel_to_tre_c
,prev.tst_cont_opt_out_c
,prev.active_ind
,prev.run_d
,prev.hdfs_insert_tmst
,prev.hdfs_update_tmst
,prev.hdfs_update_user
,prev.eff_strt_dt
,prev.eff_end_dt
from temp_tbl_prev prev
inner  join
temp_upd_del_new_records tmp
on
tmp.prtn_id = prev.prtn_id
and tmp.cus_id = prev.cus_id
and tmp.fbsi_firm_c = prev.fbsi_firm_c
and tmp.fbsi_brch_c = prev.fbsi_brch_c
and tmp.fbsi_base_c = prev.fbsi_base_c
and tmp.fid_acc_id  = prev.fid_acc_id
and tmp.rel_ty_c = prev.rel_ty_c
and tmp.multi_co_n = prev.multi_co_n
and tmp.eff_strt_dt = prev.eff_strt_dt
""")

df_unchanged_records=df_prev_tbl.subtract(df_unchanged_tmp_records)
df_temp_final=df_new_records.unionAll(df_update_records).unionAll(df_update_exstng_records).unionAll(df_del_records).unionAll(df_unchanged_records)





# create temp table only with update records. 
df_temp_final.createOrReplaceTempView("final_table")
	
spark.sql("""INSERT OVERWRITE TABLE nikhilvemula.t_care_ca_rel_main select * from final_table""")