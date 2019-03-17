##########################################
# Date Preparation script. 
# Reads the data from the main table and inserts
# into previous  day table for cdc. 
###########################################

SRC_TBL="nikhilvemula.t_care_ca_rel_main"
TRGT_TBL="nikhilvemula.t_care_ca_rel_prev"
TRGT_TBL_PATH="hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nikhilvemula.db/t_care_ca_rel_prev"

#clean up the prev day table before load
echo -e "\nINFO : Cleaning up ${TRGT_TBL} for load"
echo "INFO : hadoop fs -rm -r -f -skipTrash ${TRGT_TBL_PATH}/*"
hadoop fs -rm -r -f -skipTrash ${TRGT_TBL_PATH}/*
echo "INFO : Clean up of ${TRGT_TBL} is complete"

#load the prev day table 
load_hql="INSERT OVERWRITE TABLE ${TRGT_TBL} select * from ${SRC_TBL};"
echo "INFO : Loading data into ${TRGT_TBL}"
echo "INFO : ${load_hql}"
hive -e "${load_hql}"

if [ $? -eq 0 ]
then
	echo -e "\n######################################"
	echo "INFO : Previous day table i.e., ${TRGT_TBL} load is complete and is ready for use."
	echo "######################################"
else
	echo "######################################"
	echo "ERROR: Previous day table i.e., ${TRGT_TBL} table load has failed."
	echo "exit 1"
	echo "######################################"
	exit 1
fi
